"""Process ecg data files"""

import contextlib
import os
import tempfile
import shutil
from traceback import format_exc

import cgm.cgm as cgm
import cgm.cgm_manifest as cgm_manifest
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import time
import csv
from utils.file_map_processor import FileMapProcessor
import utils.logwatch as logging
from utils.time_estimator import TimeEstimator

"""
SCRIPT_PATH=""
FOLDER_PATH="CGM/input/UCSD-CGM/"  # Replace with the path to your CSV files
TIME_ZONE="pst"  # Set your desired timezone here
for file in ${FOLDER_PATH}DEX-*.csv; do ID=$(basename "$file" .csv | cut -d '-' -f 2) echo ${ID} python3 "${SCRIPT_PATH}CGM_API.py" "DEX-${ID}.csv" "DEX-${ID}.json" effective_time_frame=1,event_type=2,source_device_id=3,blood_glucose=4,transmitter_time=5,transmitter_id=6,uuid=AIREADI-${ID},timezone=${TIME_ZONE} --o foo=7,bar=8
done
"""


def pipeline(study_id: str):  # sourcery skip: low-code-quality
    """Process cgm data files for a study
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = f"{study_id}/pooled-data/CGM"
    processed_data_output_folder = f"{study_id}/pooled-data/CGM-processed"
    processed_data_qc_folder = f"{study_id}/pooled-data/CGM-qc"
    dependency_folder = f"{study_id}/dependency/CGM"
    manifest_folder = f"{study_id}/manifest/CGM"

    pipeline_workflow_log_folder = f"{study_id}/logs/CGM"
    ignore_file = f"{study_id}/ignore/cgm.ignore"
    participant_filter_list_file = f"{study_id}/dependency/PatientID/AllParticipantIDs07-01-2023through07-31-2024.csv"

    # input_folder = f"{study_id}/Stanford-Test/PILOT-Aug29-2024/CGM-Pool"
    # processed_data_output_folder = (
    #     f"{study_id}/Stanford-Test/PILOT-Aug29-2024/CGM-processed"
    # )
    # processed_data_qc_folder = f"{study_id}/Stanford-Test/PILOT-Aug29-2024/CGM-qc"
    # dependency_folder = f"{study_id}/Stanford-Test/PILOT-Aug29-2024/CGM-dependency"
    # manifest_folder = f"{study_id}/Stanford-Test/PILOT-Aug29-2024/CGM-manifest"

    logger = logging.Logwatch("cgm", print=True)

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_output_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_qc_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_file(f"{dependency_folder}/file_map.json")

    file_paths = []
    participant_filter_list = []

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp(prefix="cgm_pipeline_meta_")

    # Get the participant filter list file
    with contextlib.suppress(Exception):
        file_client = file_system_client.get_file_client(
            file_path=participant_filter_list_file
        )

        temp_participant_filter_list_file = os.path.join(
            meta_temp_folder_path, "filter_file.csv"
        )

        with open(file=temp_participant_filter_list_file, mode="wb") as f:
            f.write(file_client.download_file().readall())

        with open(file=temp_participant_filter_list_file, mode="r") as f:
            reader = csv.reader(f)
            for row in reader:
                participant_filter_list.append(row[0])

        # remove the first row
        participant_filter_list.pop(0)

    paths = file_system_client.get_paths(path=input_folder, recursive=False)

    for path in paths:
        t = str(path.name)

        file_name = t.split("/")[-1]

        # Check if the item is a csv file
        if file_name.split(".")[-1] != "csv":
            continue

        file_paths.append(
            {
                "file_path": t,
                "status": "failed",
                "processed": False,
                "convert_error": True,
                "output_uploaded": False,
                "qc_uploaded": True,
                "output_files": [],
            }
        )

    # Create the output folder
    file_system_client.create_directory(processed_data_output_folder)

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    file_processor = FileMapProcessor(dependency_folder, ignore_file)

    total_files = len(file_paths)

    logger.debug(f"Found {total_files} files in {input_folder}")

    manifest = cgm_manifest.CGMManifest()

    time_estimator = TimeEstimator(total_files)

    for file_item in file_paths:
        file_name_only = file_name.split(".")[0]
        patient_id = "Unknown"

        if file_name_only.split("-")[0] == "DEX":
            patient_id = file_name_only.split("-")[1]
        elif file_name_only.split("_")[0] == "Clarity":
            patient_id = file_name_only.split("_")[3]

        logger.info(f"Processing {patient_id}")

        path = file_item["file_path"]

        workflow_input_files = [path]

        # get the file name from the path. It's in the format Clarity_Export_AIREADI_{id}_*.csv
        file_name = path.split("/")[-1]

        if file_processor.is_file_ignored(file_name, path):
            logger.info(f"Ignoring {file_name} because it is in the ignore file")
            logger.time(time_estimator.step())
            continue

        if str(patient_id) not in participant_filter_list:
            logger.debug(
                f"Participant ID {patient_id} not in the allowed list. Skipping {file_name}"
            )
            continue

        # download the file to the temp folder
        input_file_client = file_system_client.get_file_client(file_path=path)

        input_last_modified = input_file_client.get_file_properties().last_modified

        should_process = file_processor.file_should_process(path, input_last_modified)

        if not should_process:
            logger.time(time_estimator.step())
            logger.debug(
                f"The file {path} has not been modified since the last time it was processed",
            )
            logger.debug(f"Skipping {path} - File has not been modified")

            logger.time(time_estimator.step())
            continue

        file_processor.add_entry(path, input_last_modified)

        file_processor.clear_errors(path)

        # Create a temporary folder on the local machine
        with tempfile.TemporaryDirectory(prefix="cgm_pipeline_") as temp_folder_path:
            # File should be downloaded as DEX_{patient_id}.csv
            download_path = os.path.join(temp_folder_path, f"DEX-{patient_id}.csv")

            logger.debug(f"Downloading {file_name} to {download_path}")

            with open(download_path, "wb") as data:
                input_file_client.download_file().readinto(data)

            logger.info(f"Downloaded {file_name} to {download_path}")

            cgm_path = download_path

            with tempfile.TemporaryDirectory(
                prefix="cgm_pipeline_temp_"
            ) as cgm_temp_folder_path:
                cgm_output_file_path = os.path.join(
                    cgm_temp_folder_path, f"DEX-{patient_id}.json"
                )
                cgm_final_output_file_path = os.path.join(
                    cgm_temp_folder_path,
                    f"DEX-{patient_id}/DEX-{patient_id}.json",
                )
                cgm_final_output_qc_file_path = os.path.join(
                    cgm_temp_folder_path,
                    f"DEX-{patient_id}/QC_results.txt",
                )

            uuid = f"AIREADI-{patient_id}"

            timezone = "pst"

            # if patient id starts with a 7xxx(UAB), set the timezone to "cst"
            if patient_id.startswith("7"):
                timezone = "cst"

            logger.debug(f"Converting {file_name}")

            try:
                cgm.convert(
                    input_path=cgm_path,
                    output_path=cgm_output_file_path,
                    effective_time_frame=1,
                    event_type=2,
                    source_device_id=3,
                    blood_glucose=4,
                    transmitter_time=5,
                    transmitter_id=6,
                    uuid=uuid,
                    timezone=timezone,
                )
            except Exception:
                logger.error(f"Failed to convert {file_name}")

                error_exception = "".join(format_exc().splitlines())

                logger.error(error_exception)

                file_processor.append_errors(error_exception, path)

                logger.time(time_estimator.step())
                continue

            logger.info(f"Converted {file_name}")

            file_item["convert_error"] = False
            file_item["processed"] = True

            logger.debug(
                f"Uploading outputs of {file_name} to {processed_data_output_folder}"
            )

            output_files = [cgm_final_output_file_path]

            workflow_output_files = []

            outputs_uploaded = True

            file_processor.delete_preexisting_output_files(path)

            for file in output_files:
                with open(file, "rb") as data:
                    f2 = file.split("/")[-1]

                    output_file_path = f"{processed_data_output_folder}/wearable_blood_glucose/continuous_glucose_monitoring/dexcom_g6/{patient_id}/{f2}"

                    logger.debug(f"Uploading {f2} to {output_file_path}")

                    try:
                        output_blob_client = file_system_client.get_file_client(
                            file_path=output_file_path
                        )

                        output_blob_client.upload_data(data, overwrite=True)

                        logger.info(f"Uploaded {f2} to {output_file_path}")
                    except Exception:
                        outputs_uploaded = False

                        logger.error(f"Failed to upload {file}")

                        error_exception = format_exc()
                        error_exception = "".join(error_exception.splitlines())

                        logger.error(error_exception)

                        file_processor.append_errors(error_exception, path)
                        continue

                    file_item["output_files"].append(output_file_path)
                    workflow_output_files.append(output_file_path)

                    manifest_glucose_file_path = f"wearable_blood_glucose/continuous_glucose_monitoring/dexcom_g6/{patient_id}/{f2}"

                    logger.debug(f"Generating manifest for {f2}")

                    # Generate the manifest entry
                    manifest.calculate_file_sampling_extent(
                        cgm_final_output_file_path, manifest_glucose_file_path
                    )

                    logger.info(f"Generated manifest for {f2}")

            logger.info(
                f"Uploaded the outputs of {file_name} to {processed_data_output_folder}"
            )

            # Add the new output files to the file map
            file_processor.confirm_output_files(
                path, workflow_output_files, input_last_modified
            )

            if outputs_uploaded:
                file_item["output_uploaded"] = True
                file_item["status"] = "success"
                logger.info(
                    f"Uploaded outputs of {file_name} to {processed_data_output_folder}"
                )
            else:
                logger.error(
                    f"Failed to upload outputs of {file_name} to {processed_data_output_folder})"
                )

            workflow_file_dependencies.add_dependency(
                workflow_input_files, workflow_output_files
            )

            # upload the QC file
            logger.debug(f"Uploading QC file for {file_name}")

            output_qc_file_path = (
                f"{processed_data_qc_folder}/{patient_id}/QC_results.txt"
            )

            try:
                with open(cgm_final_output_qc_file_path, "rb") as data:
                    output_blob_client = file_system_client.get_file_client(
                        file_path=output_qc_file_path
                    )

                    output_blob_client.upload_data(data, overwrite=True)
            except Exception:
                file_item["qc_uploaded"] = False

                error_exception = "".join(format_exc().splitlines())

                logger.error(error_exception)

                file_processor.append_errors(error_exception, path)

                logger.time(time_estimator.step())
                continue

            logger.info(f"Uploaded QC file for {file_name}")

            logger.time(time_estimator.step())
            os.remove(download_path)

    file_processor.delete_out_of_date_output_files()
    file_processor.remove_seen_flag_from_map()

    # Write the manifest to a file
    manifest_file_path = os.path.join(meta_temp_folder_path, "manifest_cgm_v2.tsv")
    manifest.write_tsv(manifest_file_path)

    logger.debug(f"Uploading file map to {dependency_folder}/file_map.json")

    try:
        file_processor.upload_json()
        logger.info(f"Uploaded file map to {dependency_folder}/file_map.json")
    except Exception as e:
        logger.error(f"Failed to upload file map to {dependency_folder}/file_map.json")
        raise e

    # Upload the manifest file
    with open(manifest_file_path, "rb") as data:

        output_blob_client = file_system_client.get_file_client(
            file_path=f"{manifest_folder}/manifest_cgm_v2.tsv"
        )

        output_blob_client.upload_data(data, overwrite=True)

    # Write the workflow log to a file
    timestr = time.strftime("%Y%m%d-%H%M%S")
    file_name = f"status_report_{timestr}.csv"
    workflow_log_file_path = os.path.join(meta_temp_folder_path, file_name)

    with open(workflow_log_file_path, "w", newline="") as csvfile:
        fieldnames = [
            "file_path",
            "status",
            "processed",
            "convert_error",
            "output_uploaded",
            "qc_uploaded",
            "output_files",
            "patient_id",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        for file_item in file_paths:
            file_item["output_files"] = ";".join(file_item["output_files"])

        writer.writeheader()
        writer.writerows(file_paths)

    with open(workflow_log_file_path, mode="rb") as data:
        logger.debug(
            f"Uploading workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

        output_blob_client = file_system_client.get_file_client(
            file_path=f"{pipeline_workflow_log_folder}/{file_name}"
        )

        output_blob_client.upload_data(data, overwrite=True)

    deps_output = workflow_file_dependencies.write_to_file(meta_temp_folder_path)

    json_file_path = deps_output["file_path"]
    json_file_name = deps_output["file_name"]

    with open(json_file_path, "rb") as data:
        output_blob_client = file_system_client.get_file_client(
            file_path=f"{dependency_folder}/{json_file_name}"
        )

        output_blob_client.upload_data(data, overwrite=True)

    shutil.rmtree(meta_temp_folder_path)


if __name__ == "__main__":
    pipeline("AI-READI")
