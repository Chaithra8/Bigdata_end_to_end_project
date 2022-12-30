import pyspark
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from os import *

packages = [
'com.mysql:mysql-connector-j:8.0.31']

spark = SparkSession.builder.appName("Hive_Spark")\
.master("local[*]")\
.config("spark.jars.packages", ",".join(packages))\
.config("spark.driver.bindAddress","127.0.0.1")\
.enableHiveSupport()\
.getOrCreate()  # Spark 2.x

spark.sparkContext.setLogLevel("ERROR")

spark.sql("use hadoop_proj;")

df = spark.sql("select * from weblog_dynamic_partition order by id;")
df = df.withColumn("responsecode",df["responsecode"].cast(StringType()))
df.show()

 # to get total requests sent for each host & ipaddress combination
df1=df.groupBy(['host','ipaddress']).agg({'url':'count'})\
    .withColumnRenamed("count(url)","total_request")
# df1.show()
 # fetching count of each response type for host & ipaddress combination
df2 = df.groupBy(['host','ipaddress']).pivot('responsecode',[200,302,304]).count()

 # Joining total requests & response count
df3 = df1.join(df2, (df1.host == df2.host) & (df1.ipaddress == df2.ipaddress),'inner')\
        .select(df1["host"], df1["ipaddress"], df1["total_request"], df2["200"].alias("total_200_responsecode"), \
        df2["302"].alias("total_302_responsecode"), df2["304"].alias("total_304_responsecode")).fillna(value=0)
# df3.show()

# / To get count of each response code for each request type
df.createOrReplaceTempView("df_table")
df4 = spark.sql("WITH CTE as (SELECT SUBSTRING(url, 1, 4) as request_type, responsecode FROM df_table)\
SELECT request_type, SUM(CASE WHEN responsecode='200' THEN 1 ELSE 0 END) as total_200_responsecode,\
SUM(CASE WHEN responsecode='302' THEN 1 ELSE 0 END) as total_302_responsecode,\
SUM(CASE WHEN responsecode='304' THEN 1 ELSE 0 END) as total_304_responsecode FROM \
CTE GROUP BY request_type;")

sqlproperties = {"user": "username", "password": "password", "driver": "com.mysql.cj.jdbc.Driver"}

print("\n", "Mysql Ingestion started", "\n")

df3.write.jdbc(url="jdbc:mysql://localhost:3306/bigdata_proj", table="hive_spark_agg", mode="overwrite", properties=sqlproperties)

df4.write.jdbc(url="jdbc:mysql://localhost:3306/bigdata_proj", table="hive_spark_agg2", mode="overwrite", properties=sqlproperties)
