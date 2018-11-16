import math
from datetime import timedelta
import logging

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.types import *
import pendulum
import pandas
import requests

conf = SparkConf().setAppName("PySparkStreaming") \
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.default.parallelism", 6) \
        .set("spark.speculation", "true") \
        .set("spark.speculation.interval", "1s") \
        .set("spark.streaming.kafka.maxRatePerPartition", 300) \
        .set("spark.sql.autoBroadcastJoinThreshold", -1)

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
sc.setCheckpointDir("/tmp/spark-streaming")

spark = SparkSession \
        .builder \
        .appName("sparkSQL_car_gps") \
        .getOrCreate()
spark.sql("SET spark.sql.shuffle.partitions=6")

grid_schema = StructType([
    StructField("grid", IntegerType(), False),
    StructField("block", IntegerType(), False)
])


def grid_block(lng_x, lat_y):
    if not (121.34 <= lng_x < 121.68 and 24.92 <= lat_y < 25.2):
        return (-1, -1)
    cor_x = math.floor((lng_x - 121.34) / 0.02)
    cor_y = math.floor((lat_y - 24.92) / 0.02)
    grid = 17 * cor_y + cor_x

    dx = lng_x - (121.34 + 0.02 * cor_x + 0.01)
    dy = lat_y - (24.92 + 0.02 * cor_y + 0.01)
    if (dx >= 0 and dy >= 0):
        block = 1
    elif (dx < 0 and dy >= 0):
        block = 2
    elif (dx < 0 and dy < 0):
        block = 3
    else:
        block = 4

    return (grid, block)


grid_block_udf = udf(grid_block, grid_schema)
schema = StructType([StructField("memsn", LongType(), True),
            StructField("utc", StringType(), True),
            StructField("meter", LongType(), True),
            StructField("busy", LongType(), True),
            StructField("acc", BooleanType(), True),
            StructField("grid", IntegerType(), True),
            StructField("block", IntegerType(), True)])
last_df = spark.createDataFrame(sc.emptyRDD(), schema)

def process(time, rowRdd):
    print("========= %s =========" % str(time))
    if rowRdd.isEmpty():
        print("Rdd is empty")
        return
    tw = pendulum.timezone("Asia/Taipei")
    time = tw.convert(time)
    utc = pendulum.timezone("UTC")
    end = pendulum.instance(utc.convert(time)).subtract(minutes=2)
    start = end.subtract(minutes=3)
    end.set_to_string_format("%Y-%m-%dT%H:%M:%SZ")
    start.set_to_string_format("%Y-%m-%dT%H:%M:%SZ")

    taxi_df = spark.createDataFrame(rowRdd)
    taxi_df = taxi_df.filter(taxi_df.utc < str(end)) \
                     .filter(taxi_df.utc > str(start))
    taxi_df_grid = taxi_df \
            .withColumn("grid_block", grid_block_udf("lng_x", "lat_y"))
    taxi_df_grid = taxi_df_grid \
            .withColumn("grid", taxi_df_grid.grid_block.grid) \
            .withColumn("block", taxi_df_grid.grid_block.block)
    taxi_df_grid = taxi_df_grid.drop("car_type", "height", "speed", "course",
                                     "grid_block")
    taxi_df_grid = taxi_df_grid.where("grid >= 0")
    taxi_df_grid.cache()
    print("taxis in this batch:", taxi_df_grid.select("memsn").distinct().count())
    # merge last batch
    tableNames = spark.catalog.listTables()
    if "last_df" in [t.name for t in tableNames]:
        taxi_df_grid.createOrReplaceTempView("taxi")
        taxi_df_grid = spark.sql("""
                                 select * from last_df
                                 union all
                                 select * from taxi
                                 """)
    taxi_df_grid = taxi_df_grid.withColumn("idx",
                    monotonically_increasing_id()).cache()
    taxi_df_grid.createOrReplaceTempView("taxi")
    # status changed
    busy_cars = spark.sql("""
                    with ts as (
                      select
                        row_number() over (partition by memsn
                          order by utc) as rownum,
                        utc, memsn, grid, block, acc, meter, busy,
                        lng_x, lat_y
                      from taxi
                    )
                    select
                      cur.utc, cur.memsn, cur.grid, cur.block, 
                      cur.lng_x, cur.lat_y
                    from ts cur inner join ts prev
                      on prev.rownum = cur.rownum - 1
                      and prev.memsn = cur.memsn
                    where prev.acc = 1 and prev.meter = 0 and prev.busy = 0
                      and cur.acc = 1 and cur.meter = 1
                    """)
    print("status changed:", busy_cars.count())
#    host = "52.246.188.40:3306"
#    db = "realtime_car"
#    try:
#        busy_cars \
#          .write.format("jdbc") \
#          .option("url",
#                  "jdbc:mysql://{}/{}?useSSL=false".format(host, db)) \
#          .option("driver", "com.mysql.jdbc.Driver") \
#          .option("dbtable", "busy_cars") \
#          .option("user", "taxi_dashboard") \
#          .option("password", "dashboard123") \
#          .save(mode="append")
#    except Exception as e:
#        logging.exception(e)
    # find the last record of each driver
    last_records = spark.sql("""
                    select t1.* from taxi t1
                      left join taxi t2 on t1.memsn = t2.memsn
                        and ((t1.utc < t2.utc) or
                        (t1.utc = t2.utc and t1.idx < t2.idx))
                    where t2.memsn is null
                    """)
    last_records = last_records.drop("idx").coalesce(6).checkpoint()
    last_records_free = last_records \
            .where("acc = 1 and meter = 0 and busy = 0") \
            .drop("meter", "busy", "acc")
    # write to db
    host = "52.246.185.78:3306"
    db = "taxi"
    try:
        last_records_free \
              .write.format("jdbc") \
              .option("url",
                      "jdbc:mysql://{}/{}?useSSL=false".format(host, db)) \
              .option("driver", "com.mysql.jdbc.Driver") \
              .option("dbtable", "taxi_gps") \
              .option("user", "taxi_manager") \
              .option("password", "taxi1215@") \
              .save(mode="overwrite")
    except Exception as e:
        logging.exception(e)
    else:
        domain = "40.115.238.9"
        url = "http://{}:3851/passengersradar/realtimeData/".format(domain)
        requests.post(url, json={"result": "ok"})

    # clean up dataframe
    spark.catalog.clearCache()
    last_records.createOrReplaceTempView("last_df")
    del last_records
    #last_records_grid = last_records_grid.where("grid > 0")
    #grid_free_count = last_records_grid \
    #        .where("acc = 1 and meter = 0 and busy = 0") \
    #        .orderBy(["grid", "block"]) \
    #        .groupBy(["grid", "block"]) \
    #        .count()
    #print("grid_free_count:", grid_free_count.toPandas().shape[0])
    #grid_free_count.show()


def main():
    import time
    import json

    from pyspark.streaming import StreamingContext
    from pyspark.streaming.kafka import KafkaUtils
    from pyspark.sql import Row

    ssc = StreamingContext(sc, 60)
    #  ssc.checkpoint("/tmp/spark-streaming-cargps")
    kafkaStream = KafkaUtils. \
            createDirectStream(ssc, ["car_gps"],
                               {"metadata.broker.list": "192.168.100.12:9092",
                                "auto.offset.reset": "largest"})

    fields = ["memsn", "utc", "lng_x", "lat_y", "car_type", "meter", "height",
              "speed", "course", "busy", "acc"]
    recordRow = Row(*fields)
    recordStream = kafkaStream \
            .repartition(6) \
            .map(lambda kv: kv[1]) \
            .flatMap(lambda s: json.loads(s)) \
            .map(lambda r: recordRow(*r))
    #recordStream.pprint()
    recordStream.foreachRDD(process)

    ssc.remember(300)
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()

