"""Process env sensor data files"""

import contextlib
import os
import tempfile
import shutil
import env_sensor.es_root as es
import env_sensor.es_metadata as es_metadata
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import time
import csv
from traceback import format_exc
import utils.logwatch as logging
from utils.file_map_processor import FileMapProcessor
from utils.time_estimator import TimeEstimator
import zipfile


def pipeline(
    study_id: str,
):  # sourcery skip: collection-builtin-to-comprehension, comprehension-to-generator, low-code-quality
    """Process env sensor data files for a study
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    # input_folder = f"{study_id}/pooled-data/EnvSensor"
    # manual_input_folder = f"{study_id}/pooled-data/EnvSensor-manual-year2"
    # processed_data_output_folder = f"{study_id}/pooled-data/EnvSensor-processed"
    # dependency_folder = f"{study_id}/dependency/EnvSensor"
    # data_plot_output_folder = f"{study_id}/pooled-data/EnvSensor-dataplot"
    pipeline_workflow_log_folder = f"{study_id}/logs/EnvSensor"

    input_folder = f"{study_id}/pooled-data/JS_EnvSensor"
    manual_input_folder = f"{study_id}/pooled-data/EnvSensor-manual-pilot"
    processed_data_output_folder = f"{study_id}/pooled-data/JS_EnvSensor-processed"
    dependency_folder = f"{study_id}/dependency/JS_EnvSensor"
    data_plot_output_folder = f"{study_id}/pooled-data/JS_EnvSensor-dataplot"

    ignore_file = f"{study_id}/ignore/envSensor.ignore"
    red_cap_export_file = (
        f"{study_id}/pooled-data/REDCap/AIREADiPilot-2024Sep13_EnviroPhysSensorInfo.csv"
    )
    participant_filter_list_file = f"{study_id}/dependency/PatientID/AllParticipantIDs07-01-2023through07-31-2024.csv"

    logger = logging.Logwatch("env_sensor", print=True)

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_output_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(data_plot_output_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_file(f"{dependency_folder}/file_map.json")

    with contextlib.suppress(Exception):
        file_system_client.delete_file(f"{dependency_folder}/manifest.tsv")

    paths = file_system_client.get_paths(path=input_folder, recursive=False)

    file_paths = []
    participant_filter_list = []

    # dev_allowed_files = ["ENV-1239-056.zip"]

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp(prefix="env_sensor_meta_")

    logger.debug(f"Getting file paths in {input_folder}")

    file_processor = FileMapProcessor(dependency_folder, ignore_file)

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

    for path in paths:
        t = str(path.name)

        file_name = t.split("/")[-1]

        # if file_name not in dev_allowed_files:
        #     print(f"dev-Skipping {file_name}")
        #     continue

        # Check if the file name is in the format dataType-patientID-someOtherID.zip
        if not file_name.endswith(".zip"):
            logger.debug(f"Skipping {file_name}")
            continue

        if len(file_name.split("-")) != 3:
            logger.debug(f"Skipping {file_name}")
            continue

        cleaned_file_name = file_name.replace(".zip", "")

        if file_processor.is_file_ignored_by_path(cleaned_file_name):
            logger.debug(f"Skipping {t}")
            continue

        patient_id = cleaned_file_name.split("-")[1]

        # if str(patient_id) not in participant_filter_list:
        #     logger.debug(
        #         f"Participant ID {patient_id} not in the allowed list. Skipping {file_name}"
        #     )
        #     continue

        patient_folder_name = file_name.split(".")[0]

        file_paths.append(
            {
                "file_path": t,
                "status": "failed",
                "processed": False,
                "patient_folder": patient_folder_name,
                "patient_id": patient_id,
                "convert_error": True,
                "output_uploaded": False,
                "output_files": [],
            }
        )

    # Download the redcap export file
    red_cap_export_file_path = os.path.join(meta_temp_folder_path, "redcap_export.csv")

    red_cap_export_file_client = file_system_client.get_file_client(
        file_path=red_cap_export_file
    )

    with open(red_cap_export_file_path, "wb") as data:
        red_cap_export_file_client.download_file().readinto(data)

    total_files = len(file_paths)

    logger.info(f"Found {total_files} items in {input_folder}")

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    manifest = es_metadata.ESManifest()

    time_estimator = TimeEstimator(total_files)

    for file_item in file_paths:
        path = file_item["file_path"]
        patient_folder_name = file_item["patient_folder"]

        workflow_input_files = [path]

        # get the file name from the path
        file_name = path.split("/")[-1]

        if file_processor.is_file_ignored(file_name, path):
            logger.info(f"Ignoring {file_name}")
            continue

        # download the file to the temp folder
        input_file_client = file_system_client.get_file_client(file_path=path)

        input_last_modified = input_file_client.get_file_properties().last_modified

        should_process = file_processor.file_should_process(path, input_last_modified)

        if not should_process:
            logger.debug(
                f"The file {path} has not been modified since the last time it was processed",
            )
            logger.debug(f"Skipping {path} - File has not been modified")

            logger.time(time_estimator.step())
            continue

        file_processor.add_entry(path, time.time())
        file_processor.clear_errors(path)

        with tempfile.TemporaryDirectory(
            prefix="env_sensor_pipeline_"
        ) as temp_folder_path:

            temp_input_folder = os.path.join(temp_folder_path, patient_folder_name)
            os.makedirs(temp_input_folder, exist_ok=True)

            download_path = os.path.join(temp_folder_path, "raw_data.zip")

            logger.debug(f"Downloading {file_name} to {download_path}")

            with open(file=download_path, mode="wb") as f:
                f.write(input_file_client.download_file().readall())

            logger.info(f"Downloaded {file_name} to {download_path}")

            logger.debug(f"Unzipping {download_path} to {temp_input_folder}")

            # unzip the file into the temp folder
            with zipfile.ZipFile(download_path, "r") as zip_ref:
                zip_ref.extractall(temp_input_folder)

            logger.info(f"Unzipped {download_path} to {temp_input_folder}")

            # Count the number of files in the temp_input_folder recursively
            # One liner form https://stackoverflow.com/questions/16910330/return-total-number-of-files-in-directory-and-subdirectories
            num_files = sum([len(files) for r, d, files in os.walk(temp_input_folder)])
            logger.debug(f"Number of files in {temp_input_folder}: {num_files}")

            files_to_ignore = file_processor.files_to_ignore(temp_folder_path)

            # Delete the files that match the ignore pattern
            for file_to_ignore in files_to_ignore:
                os.remove(file_to_ignore)
                logger.debug(f"Deleted {file_to_ignore} due to ignore pattern")

            output_folder = os.path.join(temp_folder_path, "output")
            os.makedirs(output_folder, exist_ok=True)

            env_sensor = es.EnvironmentalSensor()

            logger.debug(f"Converting {patient_folder_name}")

            try:
                conversion_dict = env_sensor.convert(
                    temp_input_folder,
                    output_folder,
                    visit_file=red_cap_export_file_path,
                )

            except Exception:
                logger.error(f"Failed to convert {patient_folder_name}")
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())

                logger.error(error_exception)

                file_processor.append_errors(error_exception, path)

                logger.time(time_estimator.step())

                continue

            logger.info(f"Converted {patient_folder_name}")

            data_plot_folder = os.path.join(temp_folder_path, "data_plot")
            os.makedirs(data_plot_folder, exist_ok=True)

            print(conversion_dict)

            output_file = conversion_dict["output_file"]
            # pid = conversion_dict["r"]["pppp"]
            pid = conversion_dict["participantID"]

            if conversion_dict["conversion_success"]:
                meta_dict = env_sensor.metadata(conversion_dict["output_file"])

                output_file_path = f"{data_plot_output_folder}/environmental_sensor/leelab_anura/{pid}/{output_file.split('/')[-1]}"

                manifest.add_metadata(meta_dict, output_file_path)

                dataplot_dict = env_sensor.dataplot(conversion_dict, data_plot_folder)

                dataplot_output_file = dataplot_dict["output_file"]

                uploaded_dataplot_output_file = (
                    f"{data_plot_output_folder}/{dataplot_output_file.split('/')[-1]}"
                )

                dataplot_file_client = file_system_client.get_file_client(
                    file_path=uploaded_dataplot_output_file
                )

                logger.debug(
                    f"Uploading {dataplot_output_file} to {uploaded_dataplot_output_file}"
                )

                with open(dataplot_output_file, "rb") as data:
                    dataplot_file_client.upload_data(data, overwrite=True)

                logger.info(
                    f"Uploaded {dataplot_output_file} to {uploaded_dataplot_output_file}"
                )
            else:
                logger.error(f"Failed to convert {patient_folder_name}")

                error_exception = "".join(format_exc().splitlines())

                if "conversion_issues" in conversion_dict:
                    for issue in conversion_dict["conversion_issues"]:
                        file_processor.append_errors(issue, path)

                logger.error(error_exception)
                file_processor.append_errors(error_exception, path)

                logger.time(time_estimator.step())
                continue

            logger.debug(
                f"Uploading outputs of {patient_folder_name} to {processed_data_output_folder}"
            )

            workflow_output_files = []

            outputs_uploaded = True

            file_processor.delete_preexisting_output_files(path)

            with open(f"{output_file}", "rb") as data:
                f2 = output_file.split("/")[-1]

                output_file_path = f"{processed_data_output_folder}/environmental_sensor/leelab_anura/{pid}/{f2}"

                logger.debug(f"Uploading {output_file} to {output_file_path}")

                try:
                    output_file_client = file_system_client.get_file_client(
                        file_path=output_file_path
                    )

                    # Check if the file already exists. If it does, throw an exception
                    if output_file_client.exists():
                        raise Exception(
                            f"File {output_file_path} already exists. Throwing exception"
                        )

                    output_file_client.upload_data(data, overwrite=True)

                    logger.info(f"Uploaded {output_file_path}")
                except Exception:
                    outputs_uploaded = False
                    logger.error(f"Failed to upload {output_file_path}")

                    error_exception = "".join(format_exc().splitlines())

                    logger.error(error_exception)
                    file_processor.append_errors(error_exception, path)

                    logger.time(time_estimator.step())
                    continue

                file_item["output_files"].append(output_file_path)
                workflow_output_files.append(output_file_path)

            file_processor.confirm_output_files(path, workflow_output_files, "")

            if outputs_uploaded:
                file_item["output_uploaded"] = True
                file_item["status"] = "success"
                logger.info(
                    f"Uploaded outputs of {patient_folder_name} to {processed_data_output_folder}"
                )
            else:
                logger.error(
                    f"Failed to upload outputs of {patient_folder_name} to {processed_data_output_folder}"
                )

            workflow_file_dependencies.add_dependency(
                workflow_input_files, workflow_output_files
            )

            logger.time(time_estimator.step())

    file_processor.delete_out_of_date_output_files()
    file_processor.remove_seen_flag_from_map()

    # Write the manifest to a file
    manifest_file_path = os.path.join(meta_temp_folder_path, "manifest.tsv")

    manifest.write_tsv(manifest_file_path)

    logger.debug(f"Uploading manifest file to {dependency_folder}/manifest.tsv")

    # Upload the manifest file
    with open(manifest_file_path, "rb") as data:
        manifest_output_file_client = file_system_client.get_file_client(
            file_path=f"{processed_data_output_folder}/manifest.tsv"
        )

        manifest_output_file_client.upload_data(data, overwrite=True)

    os.remove(manifest_file_path)

    logger.info(f"Uploaded manifest file to {dependency_folder}/manifest.tsv")

    # Move any manual files to the destination folder
    logger.debug(f"Getting manual file paths in {manual_input_folder}")

    manual_input_folder_contents = file_system_client.get_paths(
        path=manual_input_folder, recursive=True
    )

    with tempfile.TemporaryDirectory(
        prefix="env_sensor_manual_"
    ) as manual_temp_folder_path:
        for item in manual_input_folder_contents:

            item_path = str(item.name)

            file_name = item_path.split("/")[-1]

            clipped_path = item_path.split(f"{manual_input_folder}/")[-1]

            manual_input_file_client = file_system_client.get_file_client(
                file_path=item_path
            )

            file_properties = manual_input_file_client.get_file_properties().metadata

            # Check if the file is a directory
            if file_properties.get("hdi_isfolder"):
                continue

            logger.debug(f"Moving {item_path} to {processed_data_output_folder}")

            # Download the file to the temp folder
            download_path = os.path.join(manual_temp_folder_path, file_name)

            logger.debug(f"Downloading {item_path} to {download_path}")

            with open(file=download_path, mode="wb") as f:
                f.write(manual_input_file_client.download_file().readall())

            # Upload the file to the processed data output folder
            upload_path = f"{processed_data_output_folder}/{clipped_path}"

            logger.debug(f"Uploading {item_path} to {upload_path}")

            try:
                output_file_client = file_system_client.get_file_client(
                    file_path=upload_path,
                )

                # Check if the file already exists. If it does, throw an exception
                if output_file_client.exists():
                    raise Exception(
                        f"File {upload_path} already exists. Throwing exception"
                    )

                with open(file=download_path, mode="rb") as f:
                    output_file_client.upload_data(f, overwrite=True)

                    logger.info(f"Copied {item_path} to {upload_path}")
            except Exception:
                logger.error(f"Failed to upload {item_path}")

                error_exception = "".join(format_exc().splitlines())
                logger.error(error_exception)

            os.remove(download_path)

    logger.debug(f"Uploading file map to {dependency_folder}/file_map.json")
    try:
        file_processor.upload_json()
        logger.info(f"Uploaded file map to {dependency_folder}/file_map.json")
    except Exception as e:
        logger.error(f"Failed to upload file map to {dependency_folder}/file_map.json")
        raise e

    # Write the workflow log to a file
    timestr = time.strftime("%Y%m%d-%H%M%S")
    file_name = f"status_report_{timestr}.csv"
    workflow_log_file_path = os.path.join(meta_temp_folder_path, file_name)

    with open(workflow_log_file_path, mode="w") as f:
        fieldnames = [
            "file_path",
            "status",
            "processed",
            "patient_folder",
            "patient_id",
            "convert_error",
            "output_uploaded",
            "output_files",
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=",")

        for file_item in file_paths:
            file_item["output_files"] = ";".join(file_item["output_files"])

        writer.writeheader()
        writer.writerows(file_paths)

        logger.debug(
            f"Uploading workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

    with open(workflow_log_file_path, mode="rb") as data:
        workflow_output_file_client = file_system_client.get_file_client(
            file_path=f"{pipeline_workflow_log_folder}/{file_name}"
        )

        workflow_output_file_client.upload_data(data, overwrite=True)

        logger.info(
            f"Uploaded workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

    # Write the dependencies to a file
    deps_output = workflow_file_dependencies.write_to_file(meta_temp_folder_path)

    json_file_path = deps_output["file_path"]
    json_file_name = deps_output["file_name"]

    logger.debug(f"Uploading dependencies to {dependency_folder}/{json_file_name}")

    with open(json_file_path, "rb") as data:
        dependency_output_file_Client = file_system_client.get_file_client(
            file_path=f"{dependency_folder}/{json_file_name}"
        )

        dependency_output_file_Client.upload_data(data, overwrite=True)

        logger.info(f"Uploaded dependencies to {dependency_folder}/{json_file_name}")

    shutil.rmtree(meta_temp_folder_path)


if __name__ == "__main__":
    pipeline("AI-READI")

    # delete the ecg.log file
    if os.path.exists("es.log"):
        os.remove("es.log")
