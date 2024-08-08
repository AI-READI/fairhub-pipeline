"""Azure Function App for ETL pipeline."""

import json

import logging
import azure.functions as func

from publish_pipeline.generate_high_level_metadata.generate_changelog import (
    pipeline as generate_changelog_pipeline,
)
from publish_pipeline.generate_high_level_metadata.generate_dataset_description import (
    pipeline as generate_dataset_description_pipeline,
)
from publish_pipeline.generate_high_level_metadata.generate_datatype_dictionary import (
    pipeline as generate_datatype_dictionary_pipeline,
)
from publish_pipeline.generate_high_level_metadata.generate_discovery_metadata import (
    pipeline as generate_discovery_metadata_pipeline,
)
from publish_pipeline.generate_high_level_metadata.generate_license import (
    pipeline as generate_license_pipeline,
)
from publish_pipeline.generate_high_level_metadata.generate_readme import (
    pipeline as generate_readme_pipeline,
)
from publish_pipeline.generate_high_level_metadata.generate_study_description import (
    pipeline as generate_study_description_pipeline,
)
from publish_pipeline.register_doi.register_doi import pipeline as register_doi_pipeline
from ecg_pipeline import pipeline as stage_one_ecg_pipeline
from stage_one.eidon_pipeline import pipeline as stage_one_eidon_pipeline
from stage_one.env_sensor_pipeline import pipeline as stage_one_env_sensor_pipeline
from stage_one.img_identifier_pipeline import (
    pipeline as stage_one_img_identifier_pipeline,
)
from maestro2_pipeline import pipeline as maestro_2_pipeline
from stage_one.maestro_2_pipeline import pipeline as maestro_two_pipeline
from utils import file_operations
from trigger_pipeline.study_trigger.trigger_all_studies import (
    pipeline as trigger_pipeline,
)
from trigger_pipeline.study_trigger.trigger_study import (
    pipeline as trigger_study_pipeline,
)


app = func.FunctionApp()

logging.debug("Function app created")


@app.route(route="hello", auth_level=func.AuthLevel.ANONYMOUS)
def hello(req: func.HttpRequest) -> func.HttpResponse:
    """Return a simple hello world."""
    return func.HttpResponse("Hello world!!!")


@app.route(route="echo", auth_level=func.AuthLevel.ANONYMOUS)
def echo(req: func.HttpRequest) -> func.HttpResponse:
    """Echo the request body back as a response."""
    return func.HttpResponse(req.get_body(), status_code=200, mimetype="text/plain")


@app.route(route="trigger-all-studies", auth_level=func.AuthLevel.FUNCTION)
def trigger_all_studies(req: func.HttpRequest) -> func.HttpResponse:
    """Trigger all the data processing pipelines for all the studies."""

    # Block all other methods
    if req.method != "POST":
        return func.HttpResponse(
            "Method not allowed", status_code=405, mimetype="text/plain"
        )

    try:
        trigger_pipeline()
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="trigger-study", auth_level=func.AuthLevel.FUNCTION)
def trigger_study(req: func.HttpRequest) -> func.HttpResponse:
    """Trigger all data processing pipelines for a specific study."""

    # Block all other methods
    if req.method != "POST":
        return func.HttpResponse(
            "Method not allowed", status_code=405, mimetype="text/plain"
        )

    req_body_bytes = req.get_body()
    req_body = req_body_bytes.decode("utf-8")

    try:
        content = json.loads(req_body)

        if "study_id" not in content:
            return func.HttpResponse(
                "Missing study_id", status_code=400, mimetype="text/plain"
            )

        study_id = content["study_id"]

        trigger_study_pipeline(study_id)
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="process-ecg", auth_level=func.AuthLevel.FUNCTION)
def process_ecg(req: func.HttpRequest) -> func.HttpResponse:
    """TODO: Add docstring."""

    # Block all other methods
    if req.method != "POST":
        return func.HttpResponse(
            "Method not allowed", status_code=405, mimetype="text/plain"
        )

    req_body_bytes = req.get_body()
    req_body = req_body_bytes.decode("utf-8")

    try:
        content = json.loads(req_body)

        if "study_id" not in content:
            return func.HttpResponse(
                "Missing study_id", status_code=400, mimetype="text/plain"
            )

        study_id = content["study_id"]

        stage_one_ecg_pipeline(study_id)
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="process-eidon", auth_level=func.AuthLevel.FUNCTION)
def process_eidon(req: func.HttpRequest) -> func.HttpResponse:
    """TODO: Add docstring."""

    # Block all other methods
    if req.method != "POST":
        return func.HttpResponse(
            "Method not allowed", status_code=405, mimetype="text/plain"
        )

    req_body_bytes = req.get_body()
    req_body = req_body_bytes.decode("utf-8")

    try:
        content = json.loads(req_body)

        if "study_id" not in content:
            return func.HttpResponse(
                "Missing study_id", status_code=400, mimetype="text/plain"
            )

        study_id = content["study_id"]

        stage_one_eidon_pipeline(study_id)
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="process-maestro-2", auth_level=func.AuthLevel.FUNCTION)
def process_maestro_2(req: func.HttpRequest) -> func.HttpResponse:
    """TODO: Add docstring."""

    # Block all other methods
    if req.method != "POST":
        return func.HttpResponse(
            "Method not allowed", status_code=405, mimetype="text/plain"
        )

    req_body_bytes = req.get_body()
    req_body = req_body_bytes.decode("utf-8")

    try:
        content = json.loads(req_body)

        if "study_id" not in content:
            return func.HttpResponse(
                "Missing study_id", status_code=400, mimetype="text/plain"
            )

        study_id = content["study_id"]

        maestro_2_pipeline(study_id)
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="process-maestro-two", auth_level=func.AuthLevel.FUNCTION)
def preprocess_maestro_2(req: func.HttpRequest) -> func.HttpResponse:
    """ADS"""

    # Block all other methods
    if req.method != "POST":
        return func.HttpResponse(
            "Method not allowed", status_code=405, mimetype="text/plain"
        )

    req_body_bytes = req.get_body()
    req_body = req_body_bytes.decode("utf-8")

    try:
        content = json.loads(req_body)

        if "study_id" not in content:
            return func.HttpResponse(
                "Missing study_id", status_code=400, mimetype="text/plain"
            )

        study_id = content["study_id"]

        maestro_two_pipeline(study_id)
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


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
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="generate-dataset-description", auth_level=func.AuthLevel.FUNCTION)
def generate_dataset_description(req: func.HttpRequest) -> func.HttpResponse:
    """Reads the database for the dataset and generates a dataset_description.json file in the metadata folder."""

    try:
        generate_dataset_description_pipeline()
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="generate-readme", auth_level=func.AuthLevel.FUNCTION)
def generate_readme(req: func.HttpRequest) -> func.HttpResponse:
    """Reads the database for the study and generates a readme.md file in the metadata folder."""

    try:
        generate_readme_pipeline()
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="generate-license", auth_level=func.AuthLevel.FUNCTION)
def generate_license(req: func.HttpRequest) -> func.HttpResponse:
    """Reads the database for the study and generates a license.txt file in the metadata folder."""

    try:
        generate_license_pipeline()
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="generate-changelog", auth_level=func.AuthLevel.FUNCTION)
def generate_changelog(req: func.HttpRequest) -> func.HttpResponse:
    """Reads the database for the study and generates a changelog.md file in the metadata folder."""
    try:
        generate_changelog_pipeline()
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="generate-datatype-dictionary", auth_level=func.AuthLevel.FUNCTION)
def generate_datatype_dictionary(req: func.HttpRequest) -> func.HttpResponse:
    """
    Reads the database for the dataset folders and generates datatype_dictionary.yaml file
    in the metadata folder.
    """

    try:
        generate_datatype_dictionary_pipeline()
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="generate-discovery-metadata", auth_level=func.AuthLevel.FUNCTION)
def generate_discovery_metadata(req: func.HttpRequest) -> func.HttpResponse:
    """Reads the database for the study and generates a discovery_metadata.json file in the metadata folder."""

    try:
        generate_discovery_metadata_pipeline()
        return func.HttpResponse(
            "Success", status_code=200, mimetype="application/json"
        )
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="application/json")


@app.route(route="register-doi", auth_level=func.AuthLevel.FUNCTION)
def register_doi(req: func.HttpRequest) -> func.HttpResponse:
    """Registers a DOI for the study."""

    try:
        register_doi_pipeline()
        return func.HttpResponse("Success", status_code=200, mimetype="text/plain")
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Failed", status_code=500, mimetype="text/plain")


@app.route(route="moving-folders", auth_level=func.AuthLevel.FUNCTION)
def moving_folders(req: func.HttpRequest) -> func.HttpResponse:
    """Moves the directories along with the files in the Azure Database."""
    return file_operations.file_operation(file_operations.move_directory, req)


@app.route(route="copying-folders", auth_level=func.AuthLevel.FUNCTION)
def copying_folders(req: func.HttpRequest) -> func.HttpResponse:
    """Copies the directories along with the files in the Azure Database."""
    return file_operations.file_operation(file_operations.copy_directory, req)


@app.route(route="listing-structure", auth_level=func.AuthLevel.FUNCTION)
def listing_folder_structure(req: func.HttpRequest) -> func.HttpResponse:
    """List the directories along with the files in the Azure Database."""
    try:
        file_operations.pipeline()
        file_operations.get_file_tree()
        # return func.HttpResponse("Success", status_code=200)
        return func.HttpResponse(
            json.dumps(file_operations.get_file_tree().to_dict()), status_code=200
        )
    except Exception as e:
        print(f"Exception: {e}")
        return func.HttpResponse("Internal Server Error", status_code=500)
