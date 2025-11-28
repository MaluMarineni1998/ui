
import streamlit as st
import requests
import time

# Databricks credentials
DATABRICKS_INSTANCE = "https://dbc-7c82be33-847c.cloud.databricks.com"
TOKEN = "dapi971adb4aab9fff1b477651ac822c06fc"
JOB_ID = "45166209390535"

st.title("SCD Validation Automation")

# File upload widgets
stm_file = st.file_uploader("Upload STM Excel file", type=["xlsx"])
source_file = st.file_uploader("Upload Source CSV file", type=["csv"])
output_folder = "/dbfs/FileStore/tables/output"  # Fixed DBFS output path

def upload_to_dbfs(file, dbfs_path):
    """Upload file to DBFS using Databricks REST API"""
    headers = {"Authorization": f"Bearer {TOKEN}"}
    files = {"contents": file.getbuffer()}
    data = {"path": dbfs_path, "overwrite": "true"}
    response = requests.post(f"{DATABRICKS_INSTANCE}/api/2.0/dbfs/put",
                             headers=headers, data=data, files=files)
    return response.status_code, response.text

if st.button("Run Validation"):
    if stm_file and source_file:
        st.write("Uploading files to DBFS...")

        stm_dbfs_path = f"/FileStore/tables/{stm_file.name}"
        source_dbfs_path = f"/FileStore/tables/{source_file.name}"

        # Upload STM file
        status_stm, resp_stm = upload_to_dbfs(stm_file, stm_dbfs_path)
        if status_stm != 200:
            st.error(f"Failed to upload STM file: {resp_stm}")
            st.stop()

        # Upload Source file
        status_src, resp_src = upload_to_dbfs(source_file, source_dbfs_path)
        if status_src != 200:
            st.error(f"Failed to upload Source file: {resp_src}")
            st.stop()

        st.success(f"Files uploaded:\nSTM: {stm_dbfs_path}\nSource: {source_dbfs_path}")

        # Trigger Databricks job
        st.write("Triggering Databricks job...")
        headers = {"Authorization": f"Bearer {TOKEN}"}
        payload = {
            "job_id": JOB_ID,
            "notebook_params": {
                "STM_FILE": f"/dbfs{stm_dbfs_path}",
                "SOURCE_FILE": f"/dbfs{source_dbfs_path}",
                "OUTPUT_FILE": output_folder
            }
        }

        response = requests.post(f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now",
                                 json=payload, headers=headers)

        if response.status_code != 200:
            st.error(f"API Error: {response.status_code}")
            st.write(response.text)
            st.stop()

        data = response.json()
        run_id = data.get("run_id")
        if not run_id:
            st.error("No run_id returned. Response:")
            st.write(data)
            st.stop()

        st.success(f"Job started with run_id: {run_id}")

        # Poll job status
        while True:
            status_resp = requests.get(
                f"{DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get?run_id={run_id}",
                headers=headers
            )

            if status_resp.status_code != 200:
                st.error(f"Status API Error: {status_resp.status_code}")
                st.write(status_resp.text)
                break

            status_data = status_resp.json()
            state = status_data.get("state", {}).get("life_cycle_state", "UNKNOWN")
            st.write(f"Current status: {state}")

            if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                result_state = status_data.get("state", {}).get("result_state", "UNKNOWN")
                st.write(f"Result state: {result_state}")
                if result_state == "SUCCESS":
                    st.success("Job completed successfully!")

                    # Show download link for output file
                    output_file_name = f"{status_data.get('run_name', 'validation_report')}.xlsx"
                    public_url = f"{DATABRICKS_INSTANCE}/files/output/{output_file_name}"
                    st.markdown(f"Download Output File")
                else:
                    st.error(f"Job ended with state: {result_state}")
                break

            time.sleep(5)
    else:
        st.error("Please upload both STM and Source files before running the job.")
