"""Create a mini dataset based on a list of prespecified IDs"""

import azure.storage.filedatalake as azurelake
from uuid import uuid4
from tqdm import tqdm
import os
import tempfile

import config


def pipeline():  # sourcery skip: use-itertools-product
    """Create a mini dataset based on a list of prespecified IDs"""

    source_folders = [
        "cardiac_ecg/ecg_12lead/philips_tc30",
        "environment/environmental_sensor/leelab_anura",
        "retinal_flio/flio/heidelberg_flio",
        "retinal_oct/structural_oct/heidelberg_spectralis",
        "retinal_oct/structural_oct/topcon_maestro2",
        "retinal_oct/structural_oct/topcon_triton",
        "retinal_oct/structural_oct/zeiss_cirrus",
        "retinal_octa/enface/heidelberg_spectralis",
        "retinal_octa/enface/topcon_maestro2",
        "retinal_octa/enface/topcon_triton",
        "retinal_octa/enface/zeiss_cirrus",
        "retinal_octa/flow_cube/heidelberg_spectralis",
        "retinal_octa/flow_cube/topcon_maestro2",
        "retinal_octa/flow_cube/topcon_triton",
        "retinal_octa/flow_cube/zeiss_cirrus",
        "retinal_octa/segmentation/heidelberg_spectralis",
        "retinal_octa/segmentation/topcon_maestro2",
        "retinal_octa/segmentation/topcon_triton",
        "retinal_octa/segmentation/zeiss_cirrus",
        "retinal_photography/cfp/icare_eidon",
        "retinal_photography/cfp/optomed_aurora"
        "retinal_photography/cfp/topcon_maestro2",
        "retinal_photography/cfp/topcon_triton",
        "retinal_photography/faf/icare_eidon",
        "retinal_photography/ir/heidelberg_spectralis",
        "retinal_photography/ir/icare_eidon",
        "retinal_photography/ir/topcon_maestro2",
        "retinal_photography/ir/zeiss_cirrus",
        "wearable_activity_monitor/heart_rate/garmin_vivosmart5",
        "wearable_activity_monitor/oxygen_saturation/garmin_vivosmart5",
        "wearable_activity_monitor/physical_activity/garmin_vivosmart5",
        "wearable_activity_monitor/physical_activity_calorie/garmin_vivosmart5",
        "wearable_activity_monitor/respiratory_rate/garmin_vivosmart5",
        "wearable_activity_monitor/sleep/garmin_vivosmart5",
        "wearable_activity_monitor/stress/garmin_vivosmart5",
        "wearable_blood_glucose/continuous_glucose_monitoring/dexcom_g6",
    ]

    manifest_file_paths = [
        "cardiac_ecg/manifest.tsv",
        "environment/environmental_sensor/manifest.tsv",
        "retinal_flio/manifest.tsv",
        "retinal_oct/manifest.tsv",
        "retinal_octa/manifest.tsv",
        "retinal_photography/manifest.tsv",
        "wearable_activity_monitor/manifest.tsv",
        "wearable_blood_glucose/manifest.tsv",
    ]

    participant_ids = [
        "1400",
        # "1205",
        # "4531",
        # "4403",
        # "7166",
        # "7352",
    ]

    # Create the file system clients
    input_file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name="stage-final-container",
    )

    new_container_name = f"mini-dataset-{uuid4()}"

    print(f"Creating new container: {new_container_name}")

    # Create the new container
    new_container_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name=new_container_name,
    )
    new_container_client.create_file_system()

    file_paths = []
    errors = []

    for source_folder in source_folders:
        for participant_id in participant_ids:
            print(
                f"Searching for files in {source_folder} for participant {participant_id}"
            )
            patient_folder_path = f"{source_folder}/{participant_id}"

            patient_folder_client = input_file_system_client.get_directory_client(
                patient_folder_path,
            )

            # Check if the folder exists
            if not patient_folder_client.exists():
                print(
                    f"Folder {patient_folder_path} does not exist. Skipping participant {participant_id} in {source_folder}..."
                )
                errors.append(
                    f"Folder {patient_folder_path} does not exist. Skipping participant {participant_id} in {source_folder}..."
                )
                continue

            paths = patient_folder_client.get_paths(recursive=True)

            for path in paths:
                try:
                    # Check if the path is a file
                    if not path.is_directory:
                        file_path = path.name
                        print(f"Found file: {file_path}")
                        file_client = input_file_system_client.get_file_client(
                            file_path
                        )
                        file_properties = file_client.get_file_properties()
                        file_metadata = file_properties.metadata
                        if file_metadata.get("hdi_isfolder"):
                            continue
                        file_paths.append(file_path)
                except Exception as e:
                    print(
                        f"Error searching for files in {source_folder} for participant {participant_id}: {e}. Skipping..."
                    )
                    errors.append(
                        f"Error searching for files in {source_folder} for participant {participant_id}: {e}. Skipping..."
                    )
                    continue

    print(f"Found {len(file_paths)} files")

    with tempfile.TemporaryDirectory(prefix="mini_dataset_") as temp_folder_path:
        for file_path in tqdm(
            file_paths, desc="Copying files", unit="file", dynamic_ncols=True
        ):
            try:
                file_client = input_file_system_client.get_file_client(file_path)
                file_name = os.path.basename(file_path)

                download_path = os.path.join(temp_folder_path, file_name)
                with open(download_path, "wb") as data:
                    file_client.download_file().readinto(data)

                with open(download_path, "rb") as data:
                    new_file_client = new_container_client.get_file_client(
                        file_path=file_path
                    )
                    new_file_client.upload_data(data, overwrite=True)

                os.remove(download_path)
            except Exception as e:
                print(f"Error copying file {file_path}: {e}. Skipping...")
                errors.append(f"Error copying file {file_path}: {e}. Skipping...")
                continue

    print("\n" + "-" * 100)
    for error in errors:
        print(error)
    print("\n" + "-" * 100)

    print(f"Total errors: {len(errors)}")
    print(f"Total files: {len(file_paths)}")
    print(f"Total participants: {len(participant_ids)}")
    print(f"Total source folders: {len(source_folders)}")
    print(f"Total manifest files: {len(manifest_file_paths)}")
    print(f"Total new container: {new_container_name}")
    print(f"Total new container client: {new_container_client}")


if __name__ == "__main__":
    pipeline()
