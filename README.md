# Bigdata_end_to_end_project
Description: Aim of this project is to capture, tranform, store straming weblog entry and finally visualizing the transformed data.

## Tools used:
   Kafka, Spark Structured Streaming, SQOOP, MySQL, HDFS, Hive, PySpark, Grafana, Airflow
   
## Architecture:
   <img src="https://github.com/Chaithra8/Bigdata_end_to_end_project/blob/main/Images/Project_architecture.png" width=75% length=75%>

## Project workflow:
   - Install all the required tools & setup accordingly. I had used below version:
     * Ubuntu - 22.04
     * Java - 1.8.0_352
     * Python - 3.10.6
     * Hadoop - 3.3.4
     * Pyspark - 3.10.6
     * Kafka - 3.2.0
     * MySQL - 8.0.31
     * Sqoop - 1.4.7
     * Hive - 3.1.2
     * Airflow - 2.5.0
   - Start below services
     * Start all Hadoop daemons:
        ```
        $HADOOP_HOME/sbin/start-all.sh
        ```
     * Start Spark master & worker node
       ```
       $SPARK_HOME/sbin/start-all.sh
       ```
    * Start Kafka zookeeper & server
       ```
       systemctl start zookeeper
       systemctl start kafka
       ```
   - Create weblog data generator code https://github.com/Chaithra8/Bigdata_end_to_end_project/blob/main/weblog_simulator.py. This code generates weblosg messages through kafka producers.
   - These messages are fetched by subcribing to the topic through Spark structured streaming https://github.com/Chaithra8/Bigdata_end_to_end_project/blob/main/sparkstream.py, which is then transformed and stored into MySQL database.
   - Data in MySQL table is then stored into HDFS using SQOOP.
   - First we need to initialize SQOOP job  
     ```
     sqoop job --create Sqoop_<table_name>_<db_name> -- import --connect jdbc:mysql://localhost:<port>/<db_name>
		--username <username> --password-file file:///<path> --a-textfile --table <tb_name> --target-dir <path> --incremental append --check-column
		<col_name> --last-value <value> -m1 --direct
     ```
   - Sqoop job is executed using below code
     ```
     sqoop job --exec <sqoob_job_name>
     ```
   - After SQOOP job execution verify HDFS files in target directory using below command
     ```
     hdfs dfs -ls <target-dir>
   - Data stored in HDFS is then stored onto Hive partitioned table. Before loading the data start Hive metastore, server, CLI
     ```
     nohup hive --service metastore
		 hiveserver
     hive
     ```
   - we need to create partitioned table in hive. Inorder to load data from hdfs into partitioned table, partition column has to be at the last in file
			if that is not the case then first hdfs data has to be moved into non-partitoned table & then has to be moved to partitioned table.
   - In hive CLI, create non-partitoned & partitioned table
     ```
     -- To create non-partitoned table
     create external table <table_name> (id int, datevalue string,ipaddress string, host string, url string, responsecode int)
		 row format delimited
		 fields terminated by ','
		 stored as textfile location '<HDFS_file_path>'; 
     
     -- To create partitoned table
     > set hive.stats.column.autogather=false;
     > set hive.exec.dynamic.partition=true;
     > set hive.exec.dynamic.partition.mode=nonstrict;
     > set hive.exec.max.dynamic.partitions=1000;
     > create table <table_name> (id int, datevalue string, ipaddress string, url string, responsecode int) partitioned by (host string) row format delimited fields terminated by ',' stored as textfile;
     ```
   - Load data into partitioned table
     ```
     insert into <partione_tab_name> partition(host) select id, datevalue, ipaddress, url, responsecode, host from <non_partition_table> as a where not exists (select b.id from <partione_tab_name> as b where a.id = b.id);
     ```
   - On data stored in Hive, transformation is done to make data ready fro analysis & stored it into MySQL table. https://github.com/Chaithra8/Bigdata_end_to_end_project/blob/main/Spark_Hive_Aggregation.py
   - All the above task is scheduled using Apache Airflow.https://github.com/Chaithra8/Bigdata_end_to_end_project/blob/main/Airflow_DAG.py
   <img src="https://github.com/Chaithra8/Bigdata_end_to_end_project/blob/main/Images/Airflow_DAG_workflow.png" width=75% length=75%>
   - In Grafana, MySQL database is linked as data source & visualized streaming weblogdata.
     Here 2 scenarios are visualized:
     1. Count of each response type(200, 302, 304) for Request type i.e., GET, PUT, POST
     2. Count of different response for Host & IPaddress combination
   <img src="https://github.com/Chaithra8/Bigdata_end_to_end_project/blob/main/Images/Grafana_dashboard.png" width=75% length=75%>
