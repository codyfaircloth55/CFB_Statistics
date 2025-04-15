import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

# Create a DAG to run cfb_statistics_schmema.sql and cfb_stats_app.py
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["cfb_stats", "ssh", "mysql"],
)
def ssh_mysql_cfb_stats_dag():
    # Define the SSH command to run cfb_stats_app.py
    command = """
    export JAVA_HOME=/opt/java/openjdk;
    /opt/spark/bin/spark-submit --master spark://spark-leader-cfb:7077 /opt/spark/work-dir/cfb_stats_app.py
    """
    # Create SSHOperator task to run cfb_stats_app.py
    ssh_cfb_stats = SSHOperator(
        ssh_conn_id="ssh_leader", task_id="ssh_task", command=command, cmd_timeout=600
    )
    # Create SQLExecuteQueryOperator task to run cfb_statistics_schema.sql
    cfb_stats_schema = SQLExecuteQueryOperator(
        task_id="cfb_stats_schema",
        sql="/cfb_statistics_schema.sql",
        conn_id="mysql_sys"
    )

    # Schedule tasks
    cfb_stats_schema >> ssh_cfb_stats

ssh_mysql_cfb_stats_dag()