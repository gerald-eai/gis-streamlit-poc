# Connection functions to the database connection
# Returns the data
"""
Columns of note: 
  FMZCODE 
  GISID
  SHORTGISID
  DATECREATED 
  MEASUREDLENGTH
  OPERATINGPRESSURE
  METRICCALCULATED
  SIZECHECKMETHOD
  SIZECHECKDATE
  DEPTH
  DMACODE
  PMACODE
  MAINNAME
  FAILUREPROBABILITY
  FAILUREPROBABILITYSOURCE
  WATERTYPE
  SYMBOLCODE
  geometry 
  layer
  OPERATION
  PRESSURETYPE
"""
from databricks_api import DatabricksAPI
import os
from dotenv import load_dotenv
from typing import Any
import pandas as pd
from datetime import datetime
import json
import time as t

load_dotenv()

# update this value as we go along
LAST_JOB_ID = 641964054544138
DATA_FOLDER = '../data'
OUTPUT_FOLDER = '../output'

if not os.path.exists(DATA_FOLDER):
    os.mkdir(DATA_FOLDER)
if not os.path.exists(OUTPUT_FOLDER):
    os.mkdir(OUTPUT_FOLDER)


def gen_job_id(base: str) -> str:
    """_summary_
    Returns a custom id for the given job, based on a give date and time. 
    Alternative is to use the uuid generator 
    Args:
        base (str): _description_

    Returns:
        str: _description_
    """
    # get the current date time
    curr_time = datetime.now()      # in date time format
    ct_string = curr_time.strftime("%d%m%Y_%HH%MM%SS")
    # print(f"Current Time: {ct_string} \ntype:{type(ct_string)}")
    return base + ct_string


def test_environs():
    print(os.environ.get('AZ_DB_TOKEN'))
    print(os.environ.get('AZ_DB_HOST'))
    print(os.environ.get('AZ_DB_NOTEBOOK_PATH'))


def init_db_connection(host: str, token: str) -> DatabricksAPI:
    # provide the host and token to generate the connection
    return DatabricksAPI(host=host, token=token)


def list_clusters(db_connection: DatabricksAPI) -> dict:
    # list all the clusters
    return db_connection.cluster.list_clusters()


def get_cluster(db_connection: DatabricksAPI, cluster_id: str) -> dict:
    # get a specific cluster
    return db_connection.cluster.get_cluster(cluster_id)


def list_workspace(db_connection: DatabricksAPI, workspace_path: str | None = None):
    # list all the workspaces
    return db_connection.workspace.list(path=workspace_path)


def create_job(db_connection: DatabricksAPI,
               cluster_id: str, job_name: str,
               max_retries: int | None = None, timeout_seconds: int | None = None,
               workspace_path: str | None = None,
               notebook_params: dict | None = None,
               git: bool | None = None) -> Any:
    """_summary_
    Creates a job on the databricks platform and returns the response containing the Job ID

    Args:
        db_connection (DatabricksAPI): _description_
        cluster_id (str): _description_
        job_name (str): _description_
        max_retries (int | None, optional): _description_. Defaults to None.
        timeout_seconds (int | None, optional): _description_. Defaults to None.
        workspace_path (str | None, optional): _description_. Defaults to None.
        notebook_params (dict | None, optional): _description_. Defaults to None.
        git (bool | None, optional): _description_. Defaults to None.

    Raises:
        ConnectionError: _description_

    Returns:
        Any: _description_
    """
    # create a job
    notebook_task = {}
    updated_name = gen_job_id(job_name)
    if git:
        notebook_task['source'] = 'GIT'
    else:
        notebook_task['source'] = 'WORKSPACE'
    # define the absolute path for the notebook
    notebook_task['notebook_path'] = workspace_path
    notebook_task['base_parameters'] = notebook_params
    try:
        # get the newly created job's id
        job_id = db_connection.jobs.create_job(name=updated_name, existing_cluster_id=cluster_id,
                                               max_retries=max_retries, timeout_seconds=timeout_seconds,
                                               notebook_task=notebook_task)
        return job_id
    except:
        raise ConnectionError("Error creating the job!")


def get_jobs_by_user(db_connection: DatabricksAPI, user_name: str) -> Any:
    # list all the jobs
    jobs_list = db_connection.jobs.list_jobs()
    print(f'USER NAME: {user_name}')
    # transform the json data into dataframes
    user_jobs = pd.DataFrame()
    for job in jobs_list['jobs']:
        if job['creator_user_name'] == user_name:
            # check if we have the matching user name
            # append the job to the dataframe
            nb_row = {"job_id": [job['job_id']], "creator": [job['creator_user_name']],
                      "job_name": [job['settings']['name']], "created_time": [job['created_time']],
                      "cluster_id": [job['settings']['existing_cluster_id']],
                      "notebook_path": [job['settings']['notebook_task']['notebook_path']],
                      "notebook_source": [job['settings']['notebook_task']['source']], }
            nb_row_df = pd.DataFrame(nb_row)
            user_jobs = pd.concat([user_jobs, nb_row_df], ignore_index=True)

    return user_jobs


def get_jobs_by_path(db_connection: DatabricksAPI, nb_path: str) -> Any:
    # list all the jobs
    jobs_list = db_connection.jobs.list_jobs()
    print(f'NOTEBOOK PATH: {nb_path}')
    # transform the json data into dataframes
    notebook_jobs = pd.DataFrame()
    for job in jobs_list['jobs']:
        if 'notebook_task' in job['settings']:
            # check if we have the matching notebook path
            if job['settings']['notebook_task']['notebook_path'] == nb_path:
                # append the job to the dataframe
                nb_row = {"job_id": [job['job_id']], "creator": [job['creator_user_name']],
                          "job_name": [job['settings']['name']], "created_time": [job['created_time']],
                          "cluster_id": [job['settings']['existing_cluster_id']],
                          "notebook_path": [job['settings']['notebook_task']['notebook_path']],
                          "notebook_source": [job['settings']['notebook_task']['source']],
                          "max_retries": [job['settings']['max_retries']],
                          "timeout_seconds": [job['settings']['timeout_seconds']]}

                nb_row_df = pd.DataFrame(nb_row)
                print(nb_row_df)
                notebook_jobs = pd.concat(
                    [notebook_jobs, nb_row_df], ignore_index=True)

    print(f"Our Notebook Jobs: \n{notebook_jobs}")
    return notebook_jobs


def get_job_by_id(db_connection: DatabricksAPI, job_id: int) -> Any:
    # get a specific job
    return db_connection.jobs.get_job(job_id)


def list_active_runs(db_connection: DatabricksAPI, job_id: int | None = None, 
                     active_only: bool | None = None, completed_only: bool | None = None) -> Any:
    # list the jobs based on the active list
    return db_connection.jobs.list_runs(job_id=job_id, active_only=active_only, completed_only=completed_only)


def run_job_id(db_connection: DatabricksAPI, job_id: str, params: dict | None = None) -> Any:
    """_summary_
    Runs a specific job by job id
    Provide notebook parameters if possible
    Args:
        db_connection (DatabricksAPI): _description_
        job_id (str): _description_
        params (dict | None, optional): _description_. Defaults to None.

    Returns:
        Any: _description_
    """
    return db_connection.jobs.run_now(job_id)


def get_job_output(db_connection: DatabricksAPI, run_id: int) -> Any:
    """_summary_
    Gets the output from a job run, the connection and the run id are required 
    The rest of the parameters are optional
    Args:
        db_connection (DatabricksAPI): _description_
        run_id (str): _description_

    Returns:
        Any: _description_
    """
    return db_connection.jobs.get_run_output(run_id)


def get_one_time_run(db_connection: DatabricksAPI,
                     cluster_id: str, run_name: str,
                     max_retries: int | None = None, timeout_seconds: int | None = None,
                     workspace_path: str | None = None,
                     notebook_params: dict | None = None,
                     git: bool | None = None) -> Any:

    # creates and triggers a one time run without creating a job for the run
    notebook_task = {}
    updated_name = gen_job_id(run_name)
    if git:
        notebook_task['source'] = 'GIT'
    else:
        notebook_task['source'] = 'WORKSPACE'
    # define the absolute path for the notebook
    notebook_task['notebook_path'] = workspace_path
    notebook_task['base_parameters'] = notebook_params

    try:
        # get the newly created job's id
        run_id = db_connection.jobs.submit_run(run_name=updated_name, existing_cluster_id=cluster_id,
                                               timeout_seconds=timeout_seconds, notebook_task=notebook_task)
        print(run_id)
        return run_id
    except:
        raise ConnectionError("Error Submitting the One Time Run")


def clear_active_jobs(db_connection: DatabricksAPI, job_id: int | None = None):

    print("Clear all active jobs and job runs")
    return db_connection.jobs.delete_job(job_id=job_id)


""" Main Functions to test out the module"""


def main_delete_jobs():
    job_id = 641964054544138
    db_connection = init_db_connection(os.environ.get(
        'AZ_DB_HOST'), os.environ.get('AZ_DB_TOKEN'))
    response = clear_active_jobs(job_id=job_id, db_connection=db_connection)
    print(response)


def main_base_layer():
    print("Attempting to call on the base layer of the application")
    # Main function to get the lower hall b layer from databricks
    nb_name = 'GISMAIN_Notebook_002'
    nb_path = os.environ.get('AZ_DB_NOTEBOOK_PATH') + nb_name

    # init the connection
    db_connection = init_db_connection(os.environ.get('AZ_DB_HOST'), os.environ.get('AZ_DB_TOKEN'))
    
    # run the job and return the run id
    # run_id = get_one_time_run(db_connection=db_connection, cluster_id=os.environ.get('AZ_DB_CLUSTER_ID'),run_name="Get Lower Hall B Layer", timeout_seconds=3600,
    #                           workspace_path=nb_path, notebook_params={"NE_Area": "LOWERHALLB"}, git=False)['run_id']
    # print(f"Obtained run id: {run_id}")
    
    active_jobs = list_active_runs(
        db_connection=db_connection, active_only=True)
    file_name = gen_job_id('active_jobs') + '.json'
    with open(os.path.join(OUTPUT_FOLDER, file_name), 'w') as f:
        json.dump(active_jobs, f, indent=4)
        
    # allow the job and the run to complete before calling the response
    # t.sleep(10)
    
    run_response = get_job_output(db_connection=db_connection, run_id=413921109)
    # save the run response to a json file
    file_name = gen_job_id('run_output') + '.json'
    output_path = os.path.join(OUTPUT_FOLDER, file_name)
    # json_obj = json.dumps(run_output, indent=4)
    with open(output_path, 'w') as f:
        json.dump(run_response, f, indent=4)
    
    print(f"Type of the notebook response: {type(run_response['notebook_output']['result'])}")


def main_single_run():
    # create the connection to databricks
    db_connection = init_db_connection(os.environ.get(
        'AZ_DB_HOST'), os.environ.get('AZ_DB_TOKEN'))

    # get the list of active jobs in the databricks
    active_jobs = list_active_runs(
        db_connection=db_connection, active_only=True)
    with open(os.path.join(OUTPUT_FOLDER, 'active_jobs.json'), 'w') as f:
        json.dump(active_jobs, f, indent=4)

    # get optional parameters
    cluster_info = get_cluster(
        db_connection, os.environ.get('AZ_DB_CLUSTER_ID'))
    notebook_path = os.environ.get(
        'AZ_DB_NOTEBOOK_PATH') + "GISMAIN_Notebook_001"

    # run the job, and return the run id
    # last_run_id = get_one_time_run(db_connection=db_connection,
    #            cluster_id=os.environ.get('AZ_DB_CLUSTER_ID'), run_name='Run the Mains Layer Fetch',
    #            timeout_seconds=3600,
    #            workspace_path=notebook_path,
    #            notebook_params = None,
    #            git= False)

    # get the run response
    run_response = get_job_output(
        db_connection=db_connection, run_id=412116050)

    # save the run response to a json file
    # save the output to the output.json file
    output_path = os.path.join(OUTPUT_FOLDER, 'run_output.json')
    # json_obj = json.dumps(run_output, indent=4)
    with open(output_path, 'w') as f:
        json.dump(run_response, f, indent=4)


def main():
    db_connection = init_db_connection(os.environ.get(
        'AZ_DB_HOST'), os.environ.get('AZ_DB_TOKEN'))
    # try:
    #     db_connection = init_db_connection(os.environ.get('AZ_DB_HOST'), os.environ.get('AZ_DB_TOKEN'))
    # except:
    #     print("Error connecting to Databricks")

    # list clusters found on the connection
    cluster_info = get_cluster(
        db_connection, os.environ.get('AZ_DB_CLUSTER_ID'))
    notebook_path = os.environ.get(
        'AZ_DB_NOTEBOOK_PATH') + "GISMAIN_Notebook_001"
    # created_job_id = create_job(db_connection=db_connection,
    #            cluster_id=os.environ.get('AZ_DB_CLUSTER_ID'),
    #            job_name="My Job is Done!",
    #            max_retries=12, workspace_path=notebook_path, git=False)
    # print(created_job_id)

    user_name = os.environ.get('AZ_DB_USER')
    # jobs = get_jobs_by_path(db_connection, nb_path=notebook_path)
    jobs = get_jobs_by_user(db_connection=db_connection, user_name=user_name)
    print(jobs.size)
    # run_job_resp = run_job_id(db_connection=db_connection,job_id=LAST_RUN_ID)
    # print(f'Response from the job that was ran:  \n{run_job_resp}')

    # run_id = run_job_resp['run_id']
    # number_in_job = run_job_resp['number_in_job']

    # Calculate the number of runs in the system
    run_output_path = os.path.join(OUTPUT_FOLDER, 'runs_list.json')
    list_of_runs = db_connection.jobs.list_runs(LAST_JOB_ID)
    print(f'list_of_runs: \n{list_of_runs}')
    with open(run_output_path, 'w') as f:
        json.dump(list_of_runs, f, indent=4)

    # get the output from the job run and convert to json
    run_output = get_job_output(db_connection=db_connection, run_id=412105330)
    print(f'run_output: \n{run_output}')

    # save the output to the output.json file
    output_path = os.path.join(OUTPUT_FOLDER, 'run_output.json')
    # json_obj = json.dumps(run_output, indent=4)
    with open(output_path, 'w') as f:
        json.dump(run_output, f, indent=4)


if __name__ == "__main__":
    # main()
    main_single_run()
    # main_delete_jobs()
    main_base_layer()
