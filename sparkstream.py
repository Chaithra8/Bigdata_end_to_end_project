import pyspark
from pyspark.sql import DataFrameWriter
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from os import *


def saveToMysql(df, epoch_id):
    url = "jdbc:mysql://localhost:3306/bigdata_proj"
    df.write.format("jdbc").option("url", url).option("dbtable", "weblogdetails") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("user","username") \
    .option("password", "password*").mode("append").save()
    pass

def main():
    KAFKA_TOPIC = "weblog"
    KAFKA_SERVER = "localhost:9092"
    scala_version = '2.12'
    spark_version = '3.0.3'

    packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.0',
    'com.mysql:mysql-connector-j:8.0.31']  # TODO: match Kafka version

    spark = SparkSession.builder.appName("Kafka_Spark").master("local[*]") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.driver.bindAddress","127.0.0.1").getOrCreate()  # Spark 2.x

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

    df1 = df.selectExpr("CAST(value AS STRING)")

    df2 = df1.withColumn("value_split", split(col("value"), ",")) \
    .withColumn("datevalue", to_timestamp(col("value_split").getItem(0), 'yyyy/MM/dd HH:mm:ss'))\
    .withColumn("ipaddress", col("value_split").getItem(1)) \
    .withColumn("host", col("value_split").getItem(2)) \
    .withColumn("url", col("value_split").getItem(3)) \
    .withColumn("responsecode", col("value_split").getItem(4).cast("Integer")) \
    .drop("value_split","value")

    df2.printSchema()

    df2.writeStream.format("console").option("truncate", "false").outputMode("append").start()

    df2.writeStream.outputMode("append").foreachBatch(saveToMysql).start().awaitTermination()

if __name__ == "__main__":
    main()
