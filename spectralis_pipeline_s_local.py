"""Updated spectralis pipeline"""

import argparse
import os
import tempfile
import shutil
import contextlib
import time
from traceback import format_exc
import pydicom
import sys
import spectralis.spectralis_organize_files as spectralis_organize
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import csv
import utils.logwatch as logging
from utils.file_map_processor import FileMapProcessor
from utils.time_estimator import TimeEstimator
from functools import partial
from multiprocessing.pool import ThreadPool

# from tqdm import tqdm

# Temp folder on G: drive (used instead of system temp)
G_TEMP_DIR = "G:\\temp"

overall_time_estimator = TimeEstimator(1)  # default to 1 for now


def worker(
    workflow_file_dependencies,
    file_processor,
    processed_data_output_folder,
    file_paths: list,
    worker_id: int,
):
    """This function handles the work done by the worker threads,
    and contains core operations: reading from local input, processing, and writing to local output.
    """

    logger = logging.Logwatch(
        "spectralis",
        print=True,
        thread_id=worker_id,
        overall_time_estimator=overall_time_estimator,
    )

    total_files = len(file_paths)
    time_estimator = TimeEstimator(total_files)

    for file_item in file_paths:
        path = file_item["file_path"]

        workflow_input_files = [path]

        # get the file name from the path (works for both / and \)
        file_name = os.path.basename(path)

        if file_processor.is_file_ignored(file_name, path):
            logger.info(f"Ignoring {file_name}")
            continue

        # Local input: use directory mtime
        input_last_modified = os.path.getmtime(path)

        should_process = file_processor.file_should_process(path, input_last_modified)

        if not should_process:
            logger.debug(
                f"The file {path} has not been modified since the last time it was processed",
            )
            logger.debug(f"Skipping {path} - File has not been modified")

            logger.time(time_estimator.step())
            continue

        file_processor.add_entry(path, input_last_modified)

        file_processor.clear_errors(path)

        logger.debug(f"Processing {path}")

        # Create a temporary folder on G:; input is already unzipped so copy folder to temp
        with tempfile.TemporaryDirectory(
            prefix="spectralis_pipeline_", dir=G_TEMP_DIR
        ) as temp_folder_path:
            step2_folder = os.path.join(temp_folder_path, "step2")
            logger.debug(f"Copying local input {path} to {step2_folder}")
            shutil.copytree(path, step2_folder)
            logger.info(f"Copied {file_name} to {step2_folder}")

            # go to the /DICOM directory in the step2 folder and add the .dcm extension to all the files.
            # currently they have no extension
            step2_dicom_dir = os.path.join(step2_folder, "DICOM")

            # Check if the step2_dicom_dir exists
            if not os.path.exists(step2_dicom_dir):
                logger.error(f"Step2 DICOM directory does not exist: {step2_dicom_dir}")
                file_processor.append_errors(
                    f"Step2 DICOM directory does not exist: {step2_dicom_dir}", path
                )
                logger.time(time_estimator.step())
                continue

            for file in os.listdir(step2_dicom_dir):
                if not file.endswith(".dcm"):
                    os.rename(
                        os.path.join(step2_dicom_dir, file),
                        os.path.join(step2_dicom_dir, f"{file}.dcm"),
                    )

            # process the images
            step3_folder = os.path.join(temp_folder_path, "step3")

            if not os.path.exists(step3_folder):
                os.makedirs(step3_folder)

            logger.info(f"Organizing images in {step2_dicom_dir}")
            try:
                # organize the step2 data into step3
                spectralis_organize.process_octa(step2_dicom_dir, step3_folder)
            except Exception:
                logger.error(f"Failed to organize {step2_folder}")
                error_exception = "".join(format_exc().splitlines())
                logger.error(error_exception)
                file_processor.append_errors(error_exception, path)
                logger.time(time_estimator.step())
                continue

            logger.info(f"Organized {step2_folder} to {step3_folder}")

            file_item["organize_error"] = False
            file_item["organize_result"] = "success"

            # convert the images to nema compliant dicom files
            logger.info(f"Cleaning up step3 folder {step3_folder}")

            file_list = []

            try:
                for root, dirs, files in os.walk(step3_folder):
                    for file in files:
                        if file.endswith(".dcm"):
                            file_path = os.path.join(root, file)
                            file_name = os.path.basename(file_path)
                            file_name = file_name.split(".")[0]

                            image_type = file_name.split("_")[1]

                            ds = pydicom.dcmread(file_path)

                            full_patient_id = ds.PatientID
                            patient_id = full_patient_id.split("-")[1]

                            laterality = ds.Laterality.lower()
                            sop_instance_uid = ds.SOPInstanceUID

                            ds.PatientSex = "M"
                            ds.PatientBirthDate = ""
                            ds.PatientName = ""
                            ds.PatientID = patient_id
                            ds.ProtocolName = "spectralis mac 20x20 hs octa"

                            ds.save_as(file_path)

                            file_list.append(
                                {
                                    "file_path": file_path,
                                    "patient_id": patient_id,
                                    "image_type": image_type,
                                    "laterality": laterality,
                                    "sop_instance_uid": sop_instance_uid,
                                }
                            )
            except Exception:
                logger.error(f"Failed to clean up files in step3 folder {step3_folder}")
                error_exception = "".join(format_exc().splitlines())
                logger.error(error_exception)
                file_processor.append_errors(error_exception, path)
                logger.time(time_estimator.step())
                continue

            logger.info(f"Cleaned up step3 folder {step3_folder}")

            file_item["convert_error"] = False
            file_item["convert_result"] = "success"

            destination_folder = os.path.join(temp_folder_path, "step4")

            if not os.path.exists(destination_folder):
                os.makedirs(destination_folder)

            image_type_mapping = {
                "enface": {
                    "label": "enface",
                    "path": ["retinal_octa", "enface", "heidelberg_spectralis"],
                },
                "heightmap": {
                    "label": "segmentation",
                    "path": ["retinal_octa", "segmentation", "heidelberg_spectralis"],
                },
                "vol": {
                    "label": "flow_cube",
                    "path": ["retinal_octa", "flow_cube", "heidelberg_spectralis"],
                },
                "op": {
                    "label": "ir",
                    "path": ["retinal_photography", "ir", "heidelberg_spectralis"],
                },
                "opt": {
                    "label": "oct",
                    "path": ["retinal_oct", "structural_oct", "heidelberg_spectralis"],
                },
            }

            # Rename and copy the files to the step4 folder
            try:
                for file in file_list:
                    file_path = file["file_path"]
                    patient_id = file["patient_id"]
                    image_type = file["image_type"]
                    laterality = file["laterality"]
                    sop_instance_uid = file["sop_instance_uid"]

                    mapped_image_type = image_type_mapping[image_type]

                    output_dir = os.path.join(
                        destination_folder,
                        os.path.join(*mapped_image_type["path"]),
                        patient_id,
                    )

                    if not os.path.exists(output_dir):
                        os.makedirs(output_dir)

                    new_file_name = f"{patient_id}_spectralis_mac_20x20_hs_octa_{mapped_image_type["label"]}_{laterality}_{sop_instance_uid}.dcm"

                    new_file_path = os.path.join(output_dir, new_file_name)

                    print(f"Copying {file_path} to {new_file_path}")

                    shutil.copy(file_path, new_file_path)
            except Exception:
                logger.error(f"Failed to rename and copy files to {destination_folder}")
                error_exception = "".join(format_exc().splitlines())
                logger.error(error_exception)
                file_processor.append_errors(error_exception, path)
                logger.time(time_estimator.step())
                continue

            logger.info(
                f"Renamed and copied {len(file_list)} files to {destination_folder}"
            )

            file_item["format_error"] = False
            file_item["format_result"] = "success"

            file_item["processed"] = True

            logger.debug(
                f"Writing outputs for {file_name} to {processed_data_output_folder}"
            )

            workflow_output_files = []

            outputs_uploaded = True

            file_processor.delete_preexisting_output_files(path)

            for root, dirs, files in os.walk(destination_folder):
                for file in files:
                    full_file_path = os.path.join(root, file)

                    combined_file_name = full_file_path.replace(destination_folder, "")
                    combined_file_name = combined_file_name.replace("\\", "/")
                    if combined_file_name.startswith(os.sep):
                        combined_file_name = combined_file_name.lstrip(os.sep)
                    combined_file_name = combined_file_name.replace("/", os.sep)

                    output_file_path = os.path.join(
                        processed_data_output_folder, combined_file_name
                    )

                    print(f"Copying {full_file_path} to {output_file_path}")

                    try:
                        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
                        shutil.copy(full_file_path, output_file_path)
                        logger.info(f"Copied {combined_file_name}")
                    except Exception:
                        outputs_uploaded = False
                        logger.error(f"Failed to copy {combined_file_name}")
                        error_exception = "".join(format_exc().splitlines())
                        logger.error(error_exception)
                        file_processor.append_errors(error_exception, path)
                        continue

                    file_item["output_files"].append(output_file_path)
                    workflow_output_files.append(output_file_path)

            logger.info(f"Wrote outputs for {file_name}")

            # Add the new output files to the file map
            file_processor.confirm_output_files(
                path, workflow_output_files, input_last_modified
            )

            if outputs_uploaded:
                file_item["output_uploaded"] = True
                file_item["status"] = "success"
                logger.info(f"Uploaded outputs for {file_name}")
            else:
                file_item["output_uploaded"] = False
                file_item["status"] = "failed"
                logger.error(f"Failed to upload outputs for {file_name}")

            workflow_file_dependencies.add_dependency(
                workflow_input_files, workflow_output_files
            )

            logger.time(time_estimator.step())


def pipeline(
    study_id: str,
    workers: int = 4,
    args: list = None,
    input_folder: str = None,
    output_folder: str = None,
):
    """The function contains the work done by
    the main thread, which runs only once for each operation."""

    if args is None:
        args = []

    global overall_time_estimator

    # Process spectralis data files for a study. Args:study_id (str): the study id
    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    # Local input/output folders (like garmin_pipeline)
    if input_folder is None:
        input_folder = os.path.join(os.path.expanduser("~"), "Downloads", "Spectralis")
    if output_folder is None:
        output_folder = os.path.join(
            os.path.expanduser("~"), "Downloads", "Spectralis-processed"
        )
    processed_data_output_folder = output_folder
    dependency_folder = f"{study_id}/dependency/Spectralis"
    pipeline_workflow_log_folder = f"{study_id}/logs/Spectralis"
    ignore_file = f"{study_id}/ignore/spectralis.ignore"
    participant_filter_list_file = (
        f"{study_id}/dependency/PatientID/AllParticipantIDs_year_3_12-31-2025.csv"
    )

    logger = logging.Logwatch("spectralis", print=True)

    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    # Local output: clear with shutil
    if os.path.exists(processed_data_output_folder):
        shutil.rmtree(processed_data_output_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_file(f"{dependency_folder}/file_map.json")

    file_paths = []
    participant_filter_list = []

    # Create a temporary folder on G:
    os.makedirs(G_TEMP_DIR, exist_ok=True)
    meta_temp_folder_path = tempfile.mkdtemp(
        prefix="spectralis_pipeline_meta_", dir=G_TEMP_DIR
    )

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
            participant_filter_list.extend(row[0] for row in reader)

        # remove the first row
        participant_filter_list.pop(0)

    # List local directories (input files are already unzipped = folders)
    path_entries = []
    if os.path.isdir(input_folder):
        for name in os.listdir(input_folder):
            item_path = os.path.join(input_folder, name)
            if os.path.isdir(item_path):
                path_entries.append((item_path, name))

    for t, file_name in path_entries:
        # Parse folder name like site_data_2020-2021 (no .zip extension)
        parts = file_name.split("_")
        if len(parts) < 3:
            continue

        site_name = parts[0]
        data_type = parts[1]
        start_date_end_date = parts[2]

        # one folder has a `startdate-enddate- xx` format. ignore the last part if it exists
        if len(start_date_end_date.split("-")) > 2:
            start_date_end_date = (
                start_date_end_date.split("-")[0]
                + "-"
                + start_date_end_date.split("-")[1]
            )

        start_date = start_date_end_date.split("-")[0]
        end_date = start_date_end_date.split("-")[1]

        try:
            file_size = sum(
                os.path.getsize(os.path.join(dp, f))
                for dp, _, fn in os.walk(t)
                for f in fn
            )
        except OSError:
            file_size = 0

        file_paths.append(
            {
                "file_path": t,
                "status": "failed",
                "processed": False,
                "site_name": site_name,
                "data_type": data_type,
                "start_date": start_date,
                "end_date": end_date,
                "organize_error": True,
                "organize_result": "",
                "convert_error": True,
                "convert_result": "",
                "format_error": False,
                "format_result": "",
                "output_uploaded": False,
                "file_size": file_size,
                "output_files": [],
            }
        )

    total_files = len(file_paths)

    logger.info(f"Found {len(file_paths)} items in {input_folder}")

    # Create the local output folder
    os.makedirs(processed_data_output_folder, exist_ok=True)

    workflow_file_dependencies = deps.WorkflowFileDependencies()
    file_processor = FileMapProcessor(dependency_folder, ignore_file, args)

    overall_time_estimator = TimeEstimator(total_files)

    # Guarantees that all paths are considered, even if the number of items is not evenly divisible by workers.
    chunk_size = (len(file_paths) + workers - 1) // workers
    # Comprehension that fills out and pass to worker func final 2 args: chunks and worker_id
    chunks = [file_paths[i : i + chunk_size] for i in range(0, total_files, chunk_size)]
    args = [(chunk, index + 1) for index, chunk in enumerate(chunks)]
    pipe = partial(
        worker, workflow_file_dependencies, file_processor, processed_data_output_folder
    )

    # Thread pool created
    pool = ThreadPool(workers)
    # Distributes the pipe function across the threads in the pool
    pool.starmap(pipe, args)

    file_processor.delete_out_of_date_output_files()
    file_processor.remove_seen_flag_from_map()

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
            "file_size",
            "processed",
            "site_name",
            "data_type",
            "start_date",
            "end_date",
            "organize_error",
            "organize_result",
            "convert_error",
            "convert_result",
            "format_error",
            "format_result",
            "output_uploaded",
            "output_files",
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=",")

        for file_item in file_paths:
            file_item["output_files"] = ";".join(file_item["output_files"])

        writer.writeheader()
        writer.writerows(file_paths)

    with open(workflow_log_file_path, mode="rb") as data:
        logger.debug(
            f"Uploading workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

        workflow_output_file_Client = file_system_client.get_file_client(
            file_path=f"{pipeline_workflow_log_folder}/{file_name}"
        )

        workflow_output_file_Client.upload_data(data, overwrite=True)

        logger.info(
            f"Uploaded workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

    # Write the dependencies to a file
    deps_output = workflow_file_dependencies.write_to_file(meta_temp_folder_path)

    json_file_path = deps_output["file_path"]
    json_file_name = deps_output["file_name"]

    logger.debug(
        f"Uploading dependencies to {dependency_folder}/file_dependencies/{json_file_name}"
    )

    with open(json_file_path, "rb") as data:
        dependency_output_file_client = file_system_client.get_file_client(
            file_path=f"{dependency_folder}/file_dependencies/{json_file_name}"
        )

        dependency_output_file_client.upload_data(data, overwrite=True)

        logger.info(
            f"Uploaded dependencies to {dependency_folder}/file_dependencies/{json_file_name}"
        )

    shutil.rmtree(meta_temp_folder_path)


if __name__ == "__main__":
    sys_args = sys.argv

    workers = 4
    default_input = "G:\\year3+pre\\spectralis-s"
    default_output = "G:\\year3+processed\\spectralis-s"

    parser = argparse.ArgumentParser(
        description="Process spectralis data files (local; input already unzipped)"
    )
    parser.add_argument(
        "--workers", type=int, default=workers, help="Number of workers to use"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=default_input,
        help="Local input folder (already-unzipped Spectralis folders)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=default_output,
        help="Local output folder for processed data",
    )
    args = parser.parse_args()

    workers = args.workers

    print(f"Using {workers} workers to process spectralis data files")
    print(f"Input folder: {args.input}")
    print(f"Output folder: {args.output}")

    pipeline(
        "AI-READI",
        workers,
        sys_args,
        input_folder=args.input,
        output_folder=args.output,
    )
