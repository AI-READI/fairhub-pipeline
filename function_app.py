"""Azure Function App for ETL pipeline."""
import logging

import azure.functions as func

from publish_pipeline.generate_high_level_metadata.generate_dataset_description import (
    pipeline as generate_dataset_description_pipeline,
)
from publish_pipeline.generate_high_level_metadata.generate_readme import (
    pipeline as generate_readme_pipeline,
)
from publish_pipeline.generate_high_level_metadata.generate_study_description import (
    pipeline as generate_study_description_pipeline,
)
from stage_one.env_sensor_pipeline import pipeline as stage_one_env_sensor_pipeline
from stage_one.img_identifier_pipeline import (
    pipeline as stage_one_img_identifier_pipeline,
)
from azure.storage.filedatalake import FileSystemClient, DataLakeDirectoryClient, DataLakeServiceClient, \
    DataLakeFileClient
app = func.FunctionApp()

logging.debug("Function app created")


@app.route(route="hello", auth_level=func.AuthLevel.ANONYMOUS)
def hello(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Return a simple hello world."""
    return func.HttpResponse("Hello world!!")


@app.route(route="echo", auth_level=func.AuthLevel.ANONYMOUS)
def echo(req: func.HttpRequest) -> func.HttpResponse:
    """Echo the request body back as a response."""
    return func.HttpResponse(req.get_body(), status_code=200, mimetype="text/plain")


@app.route(route="preprocess-stage-one-env-files", auth_level=func.AuthLevel.FUNCTION)
def preprocess_stage_one_env(req: func.HttpRequest) -> func.HttpResponse:
    """Reads the data in the stage-1-container. Each file name is added to a log file in the logs folder for the study.
    Will also create an output file with a modified name to simulate a processing step.
    POC so this is just a test to see if we can read the files in the stage-1-container.
    """

    try:
        stage_one_env_sensor_pipeline()
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(
    route="preprocess-stage-one-files-n-test", auth_level=func.AuthLevel.FUNCTION
)
def preprocess_stage_one_n_test(req: func.HttpRequest) -> func.HttpResponse:
    """Reads the data in the stage-1-container. Each file name is added to a log file in the logs folder for the study.
    Will also create an output file with a modified name to simulate a processing step.
    POC so this is just a test to see if we can read the files in the stage-1-container.
    """

    try:
        stage_one_img_identifier_pipeline()
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="generate-study-description", auth_level=func.AuthLevel.FUNCTION)
def generate_study_description(req: func.HttpRequest) -> func.HttpResponse:
    """Reads the database for the study and generates a study_description.json file in the metadata folder."""

    try:
        generate_study_description_pipeline()
        return func.HttpResponse("Sucess", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="generate-dataset-description", auth_level=func.AuthLevel.FUNCTION)
def generate_dataset_description(req: func.HttpRequest) -> func.HttpResponse:
    """Reads the database for the dataset and generates a dataset_description.json file in the metadata folder."""

    try:
        generate_dataset_description_pipeline()
        return func.HttpResponse("Sucess", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="generate-readme", auth_level=func.AuthLevel.FUNCTION)
def generate_readme(req: func.HttpRequest) -> func.HttpResponse:
    """Reads the database for the study and generates a readme.md file in the metadata folder."""

    try:
        generate_readme_pipeline()
        return func.HttpResponse("Sucess", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")

@app.route(route="moving-folders", auth_level=func.AuthLevel.FUNCTION)
def moving_folders(req: func.HttpRequest) -> func.HttpResponse:
    """Moves the directories along with the files in the Azure Database."""
    file_system = (
        FileSystemClient.from_connection_string(
            f"DefaultEndpointsProtocol=https;AccountName=b2aistaging;AccountKey=ARD+Hr4hEquCtqw9jnmSgaO/hxIg5QBXZBVurhVrWt+nnK4KI34IgwCxCLmRUCwND6Sz5rMSy5xt+ASt1rMvYw==;EndpointSuffix=core.windows.net",
            file_system_name="stage-1-container"
        ))
    dir_name = "AI-READI/temp/copy-test"

    directory = file_system.get_directory_client(dir_name)

    new_dir_name = "AI-READI/copy-test"
    try:
        directory.rename_directory(
        new_name=f"{directory.file_system_name}/{new_dir_name}")

        return func.HttpResponse("Sucess", status_code=200)

    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500)
