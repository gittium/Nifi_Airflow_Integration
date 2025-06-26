import json
import urllib3
from pendulum import datetime
from airflow.decorators import task, dag
# from airflow.utils.task_group import TaskGroup

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- REPLACE WITH YOUR DETAILS ---
NIFI_PG_IDS = {
    "csv": "ab4038f9-0197-1000-05f4-ff1b74af3bad",
}
NIFI_USERNAME = "nifi"
NIFI_PASSWORD = "nifipassword"
NIFI_BASE_URL = "https://nifi:8443"
# --- ----------------------- ---

@dag(
    dag_id="nifi_elt_orchestrator_v3",
    start_date=datetime(2025, 6, 23, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["nifi", "elt", "v3"],
    render_template_as_native_obj=True,
)
def nifi_elt_orchestrator():
    
    @task
    def get_nifi_access_token():
        """
        Retrieves a NiFi access token with proper error handling.
        """
        import requests
        
        try:
            print(f"Attempting to get token from {NIFI_BASE_URL}")
            print(f"Using username: {NIFI_USERNAME}")
            
            response = requests.post(
                f"{NIFI_BASE_URL}/nifi-api/access/token",
                data=f"username={NIFI_USERNAME}&password={NIFI_PASSWORD}",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                verify=False,
                timeout=30
            )
            
            print(f"Token request status code: {response.status_code}")
            print(f"Token request headers: {dict(response.headers)}")
            
            if response.status_code in [200, 201]:
                token = response.text.strip()
                print(f"Successfully retrieved token (length: {len(token)})")
                print(f"Token preview: {token[:50]}...")
                return token
            else:
                print(f"Failed to retrieve token. Status: {response.status_code}")
                print(f"Response body: {response.text}")
                raise Exception(f"Token retrieval failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"Exception getting NiFi token: {str(e)}")
            raise

    @task
    def test_nifi_connectivity():
        """
        Test basic connectivity to NiFi without authentication.
        """
        import requests
        
        try:
            response = requests.get(
                f"{NIFI_BASE_URL}/nifi-api/system-diagnostics",
                verify=False,
                timeout=10
            )
            print(f"Connectivity test status: {response.status_code}")
            return response.status_code in [200, 401]
        except Exception as e:
            print(f"Connectivity test failed: {str(e)}")
            return False

    @task
    def start_nifi_flow(pg_id: str, token: str):
        """
        Start a NiFi process group using direct API calls.
        """
        import requests
        
        if not token:
            raise Exception("No token provided")
        
        print(f"Starting flow for PG: {pg_id}")
        print(f"Using token: {token[:50]}...")
        
        try:
            response = requests.put(
                f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{pg_id}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json={"id": pg_id, "state": "RUNNING"},
                verify=False,
                timeout=30
            )
            
            print(f"Start flow status: {response.status_code}")
            print(f"Start flow response: {response.text}")
            
            if response.status_code in [200, 201]:
                print("Successfully started NiFi flow")
                return {"status": "success", "pg_id": pg_id}
            else:
                print(f"Failed to start flow: {response.status_code} - {response.text}")
                raise Exception(f"Start flow failed: {response.status_code}")
                
        except Exception as e:
            print(f"Exception starting flow: {str(e)}")
            raise

    @task
    def wait_for_nifi_flow_completion(pg_id: str, token: str):
        """
        Wait for NiFi flow to complete processing.
        """
        import requests
        import time
        
        if not token:
            raise Exception("No token provided")
        
        max_attempts = 240  # 60 minutes with 15-second intervals
        attempt = 0
        
        print(f"Waiting for flow completion: {pg_id}")
        
        while attempt < max_attempts:
            try:
                response = requests.get(
                    f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{pg_id}/status",
                    headers={"Authorization": f"Bearer {token}"},
                    verify=False,
                    timeout=30
                )
                
                if response.status_code in [200, 201]:
                    status_data = response.json()
                    queued_count = status_data.get("processGroupStatus", {}).get("aggregateSnapshot", {}).get("flowFilesQueued", -1)
                    
                    print(f"Attempt {attempt + 1}: Flow files queued: {queued_count}")
                    
                    if queued_count == 0:
                        print("Flow processing completed")
                        return {"status": "completed", "pg_id": pg_id}
                else:
                    print(f"Status check failed: {response.status_code}")
                
                attempt += 1
                if attempt < max_attempts:
                    time.sleep(15)
                
            except Exception as e:
                print(f"Exception checking flow status: {str(e)}")
                attempt += 1
                if attempt < max_attempts:
                    time.sleep(15)
        
        raise Exception(f"Flow {pg_id} did not complete within timeout period")

    @task(trigger_rule="all_done")
    def stop_nifi_flow(pg_id: str, token: str):
        """
        Stop a NiFi process group.
        """
        import requests
        
        if not token:
            print("No token available, skipping stop")
            return {"status": "skipped", "pg_id": pg_id}
        
        print(f"Stopping flow: {pg_id}")
        
        try:
            response = requests.put(
                f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{pg_id}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json={"id": pg_id, "state": "STOPPED"},
                verify=False,
                timeout=30
            )
            
            print(f"Stop flow status: {response.status_code}")
            print(f"Stop flow response: {response.text}")
            
            if response.status_code in [200, 201]:
                print("Successfully stopped NiFi flow")
                return {"status": "stopped", "pg_id": pg_id}
            else:
                print(f"Failed to stop flow: {response.status_code} - {response.text}")
                return {"status": "failed", "pg_id": pg_id}
                
        except Exception as e:
            print(f"Exception stopping flow: {str(e)}")
            return {"status": "error", "pg_id": pg_id}

    # Create initial tasks
    connectivity_test = test_nifi_connectivity()
    token_task = get_nifi_access_token()
    
    # Process each source
    for source, pg_id in NIFI_PG_IDS.items():
        # Create tasks for this source
        start_task = start_nifi_flow(pg_id, token_task)
        wait_task = wait_for_nifi_flow_completion(pg_id, token_task)
        stop_task = stop_nifi_flow(pg_id, token_task)
        
        # Set up dependencies
        connectivity_test >> token_task >> start_task >> wait_task >> stop_task

# Instantiate the DAG
dag_instance = nifi_elt_orchestrator()