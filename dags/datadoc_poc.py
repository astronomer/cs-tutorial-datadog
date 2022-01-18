import sys
import time
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.datadog import ddclient

def failing_task():
    time.sleep(3)
    sys.exit()

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "on_success_callback": ddclient().success_callback,
    "on_failure_callback": ddclient().failure_callback,
    "on_retry_callback": ddclient().retry_callback
}

with DAG(
        dag_id="datadog_poc",
        start_date=datetime(2022, 1, 6),
        max_active_runs=3,
        schedule_interval=None,
        default_args=default_args,
        catchup=False
    ) as dag:

    # Demonstrates on_success_callback
    start = DummyOperator(
        task_id="start"
    )

    # custom api call using requests
    send_log_to_datadog = PythonOperator(
        task_id="send_log_to_datadog",
        python_callable=ddclient()._send_log,
        provide_context=True,
        op_kwargs={
            "ddsource": "airflow",
            "ddtags": " {{ task_instance_key_str }} ",
            "hostname": " {{ task.dag_id }} ",
            "message": " {{ execution_date }} INFO {{ task_instance_key_str }} > This is my custom message",
            "service": "{{ task.task_id }}"
        }
        # used template variables to demonstrate possibilities: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
    )

    # Demonstrates on_failure_callback & on_retry_callback
    failing_task = PythonOperator(
        task_id="failing_task",
        python_callable=failing_task,
        provide_context=True
    )

    # Demonstrates on_success_callback
    finish = DummyOperator(
        task_id="finish",
        trigger_rule='all_done'
    )

    start >> [send_log_to_datadog, failing_task] >> finish
