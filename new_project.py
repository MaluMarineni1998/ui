
import streamlit as st
import requests
import time

# Databricks credentials
DATABRICKS_INSTANCE = "https://community.cloud.databricks.com"
TOKEN = "dapif6a16e9af1a6ffb0ca39fe9361e27446"
JOB_ID = "437799572030792"

st.title("START VALIDATION")

# User inputs
param1 = st.text_input("STM file path:")
param2 = st.text_input("STM file name:")
param3 = st.text_input("Source file path:")
param4 = st.text_input("Source file Name:")
param5 = st.text_input("load type:")
param6 = st.text_input("Output Path:")

if st.button("Run Notebook"):
    st.write("Triggering Databricks job...")
    headers = {"Authorization": f"Bearer {TOKEN}"}
    payload = {
        "job_id": JOB_ID,
        "notebook_params": {"param1": param1, "param2": param2, "param3": param3, "param4": param4, "param5": param5, "param6": param6}
    }
    response = requests.post(f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now", json=payload, headers=headers)
    run_id = response.json().get("run_id")
    st.write(f"Job started with run_id: {run_id}")

    # Poll status
    while True:
        status_resp = requests.get(f"{DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get?run_id={run_id}", headers=headers)
        state = status_resp.json()["state"]["life_cycle_state"]
        st.write(f"Current status: {state}")
        if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break
        time.sleep(5)

    st.success("Job completed!")
