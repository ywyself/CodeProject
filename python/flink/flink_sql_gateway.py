# 文件名：flink_sql_gateway.py

import requests
import time

class FlinkSQLGatewayClient:
    def __init__(self, host: str):
        self.base_url = host.rstrip('/')
        self.session_id = None

    def create_session(self, session_name="default-session"):
        resp = requests.post(f"{self.base_url}/sessions", json={
            "kind": "generic", "sessionName": session_name
        })
        self.session_id = resp.json()["sessionHandle"]["sessionId"]
        return self.session_id

    def execute_sql(self, sql: str):
        resp = requests.post(f"{self.base_url}/sessions/{self.session_id}/statements", json={
            "statement": sql
        })
        return resp.json()["operationHandle"]

    def wait_until_finished(self, operation_id: str):
        while True:
            status_resp = requests.get(
                f"{self.base_url}/sessions/{self.session_id}/operations/{operation_id}"
            )
            status = status_resp.json().get("status")
            if status == "FINISHED":
                return
            elif status == "ERROR":
                raise RuntimeError("SQL execution failed")
            time.sleep(1)

    def fetch_results(self, operation_id: str):
        results = []
        token = None
        while True:
            params = {"token": token} if token else {}
            resp = requests.get(
                f"{self.base_url}/sessions/{self.session_id}/operations/{operation_id}/results",
                params=params
            )
            data = resp.json()
            results.extend(data.get("data", []))
            if not data.get("hasMore"):
                break
            token = data.get("nextResultUri")
        return results

    def close(self):
        if self.session_id:
            requests.delete(f"{self.base_url}/sessions/{self.session_id}")


if __name__ == '__main__':
    client = FlinkSQLGatewayClient("http://localhost:8083")
    client.create_session()
    handle = client.execute_sql("SELECT * FROM your_table LIMIT 10")
    client.wait_until_finished(handle['operationId'])
    results = client.fetch_results(handle['operationId'])
    client.close()
    
    for row in results:
        print(row)
