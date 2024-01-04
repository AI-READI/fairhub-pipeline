"""Azure Function App for ETL pipeline."""
import logging

import azure.functions as func

from stage_one.env_sensor_pipeline import pipeline as stage_one_env_sensor_pipeline
from stage_one.img_identifier_pipeline import (
    pipeline as stage_one_img_identifier_pipeline,
)

from rawdata_etl_stage_0.raw_data_etl import envsensor_raw_processing

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

@app.route(route="envsensor_raw_processing", auth_level=func.AuthLevel.FUNCTION)
def etl_envsensor_raw_data(req: func.HttpRequest) -> func.HttpResponse:
    """extracts files from site-specific envsensor directories, parses and archives them and then
    uploads to pooled-data directory in stage 1 container. Subsequently can call
    preprocess-stage-one-env step to identify freshly loaded EnvSensor data."""

    try:
        envsensor_raw_processing()
        return func.HttpResponse(body="Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        logging.info(f"exception: {e}")

@app.schedule(schedule="* 0 17 * * 0",
              arg_name="env_timer",
              run_on_startup=False)
def scheduled_envsensor_raw_etl(env_timer):
    """timed version of the HTTP-triggered workflow"""
    try:
        logging.info(f"timer triggered: {env_timer}")
        envsensor_raw_processing()
    except Exception as e:
        logging.info(f"exception: {e}")

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
