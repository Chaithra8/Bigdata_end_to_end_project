import airflow
import airflow.providers.apache
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator

DAG_NAME = 'Bigdata_Project'
args = {
    'owner': 'Airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval='* * * * *' #"@once"
)

SQOOP_Task1 = BashOperator(task_id="FromMysql_Sqoop_HDFS",
                      bash_command='sqoop job --exec Sqoop_weblogdetails_bigdata_proj', dag=dag)

my_query = """
    use hadoop_proj;
    set hive.stats.column.autogather=false;
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.max.dynamic.partitions=1000;
    insert into weblog_dynamic_partition partition(host) select id, datevalue, ipaddress, url, responsecode, host from weblog_external as a where not exists (select b.id from weblog_dynamic_partition as b where a.id = b.id);
    """

hive_Task2 = HiveOperator(
    task_id= "Loading_into_Hive_Partition",
    hive_cli_conn_id='hive_local',
    hql = my_query,
    dag=dag)

spark_submit_Task3 = SparkSubmitOperator(
    task_id="sparksubmit_Aggregate",
    application='/home/chaithra/Documents/Spark_Hive_Aggregation.py',
    conn_id='spark_local',
    dag= dag)

SQOOP_Task1 >> hive_Task2 >> spark_submit_Task3

if __name__ == '__main__':
    dag.cli()
