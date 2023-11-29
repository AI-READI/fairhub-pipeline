"""Azure Function App for ETL pipeline."""
import logging

import azure.functions as func

from stage_one.env_sensor_pipeline import pipeline as stage_one_env_sensor_pipeline
from stage_one.img_identifier_pipeline import (
    pipeline as stage_one_img_identifier_pipeline,
)

app = func.FunctionApp()

logging.debug("Function app created")


@app.route(route="hello", auth_level=func.AuthLevel.ANONYMOUS)
def hello(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Return a simple hello world."""
    return func.HttpResponse("Hello world!")


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
    except Exception as e:
        print(f"Exception: {e}")

    return func.HttpResponse("Success", status_code=200, mimetype="text/plain")


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
    except Exception as e:
        print(f"Exception: {e}")

    return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
