import requests
import json
from airflow.hooks.base import BaseHook
# from airflow.providers.datadog.hooks.datadog import DatadogHook

class ddclient():
    def __init__(self, conn_id='datadog_default'):
        self.conn = BaseHook.get_connection(conn_id)
        self.dd_api_key = self.conn.extra_dejson.get('api_key')
        self.dd_app_key = self.conn.extra_dejson.get('app_key')
        self.headers = {
            "Content-Type": "application/json",
            "DD-API-KEY": self.dd_api_key
        }

    def success_callback(self, context):
        self._send_log(context=context, service="SUCCESS")

    def failure_callback(self, context):
        self._send_log(context=context, service="FAILED")

    def retry_callback(self, context):
        self._send_log(context=context, service="UP FOR RETRY")

    def _send_log(self, ddsource=None, ddtags=None, hostname=None, message=None, service=None, **context):
        '''
        uses the send logs endpoint described in Datadoc API docs here:
        https://docs.datadoghq.com/api/latest/logs/#send-logs
        '''
        ti = context['task_instance']
        if ddsource is None:
            ddsource = "airflow"
        if hostname is None:
            hostname = f"{ti.dag_id}.{ti.task_id}"
        if message is None:
            message = ti.log_url


        payload = {
            "ddsource": ddsource,
            "ddtags": ddtags,
            "hostname": hostname,
            "message": message,
            "service": service
        }
        try:
            requests.post(
                url="https://http-intake.logs.datadoghq.com/api/v2/logs",
                data=json.dumps(payload),
                headers=self.headers
            )
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)