"""Remove stray folders that don't match participant IDs"""

import argparse
import csv
import os
import tempfile
from collections import defaultdict
import azure.storage.filedatalake as azurelake
import config
import utils.logwatch as logging


def process_source_folder(
    source_folder,
    idx,
    total_folders,
    file_system_client,
    participant_ids,
    dry_run=True,
):
    """Process a single source folder: find and optionally remove stray folder names (not on ID list)."""
    print(f"\n[{idx}/{total_folders}] Processing source folder: {source_folder}")

    # Get the list of folders in the source folder (non-recursive, immediate level only)
    folder_paths = file_system_client.get_paths(path=source_folder, recursive=False)

    # Folder names present in this source folder (patient_id as name)
    folder_names_in_source = set()
    stray_folders = []

    for folder_path in folder_paths:
        folder_name = str(folder_path.name).split("/")[-1]
        folder_names_in_source.add(folder_name)

        if folder_name not in participant_ids:
            stray_folder_info = {
                "source_folder": source_folder,
                "folder_path": folder_path.name,
                "folder_name": folder_name,
            }
            stray_folders.append(stray_folder_info)
            print(f"  ⚠️  Stray folder found: {folder_name} (path: {folder_path.name})")

    if dry_run:
        print(f"  [DRY RUN] Would remove {len(stray_folders)} stray folder(s)")
    else:
        for stray in stray_folders:
            try:
                # Delete the directory
                file_system_client.delete_directory(stray["folder_path"])
                print(
                    f"  ✅ Removed stray folder: {stray['folder_name']} (path: {stray['folder_path']})"
                )
            except Exception as exc:
                print(f"  ❌ Error removing {stray['folder_name']}: {exc}")

    return stray_folders


def pipeline(dry_run=True):  # sourcery skip: low-code-quality
    """Remove stray folders that don't match participant IDs"""
    print("=" * 80)
    if dry_run:
        print("Starting stray folder removal pipeline (DRY RUN)")
    else:
        print("Starting stray folder removal pipeline")
    print("=" * 80)

    # Read participant IDs from CSV file
    csv_file_path = r"C:\Users\sanjay\Developer\fairhub-pipeline\year3_person_ids.csv"
    print(f"\nReading participant IDs from: {csv_file_path}")

    participant_ids = set()
    with open(csv_file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row
        for row in reader:
            if row:  # Skip empty rows
                participant_ids.add(row[0].strip())

    print(f"✓ Loaded {len(participant_ids)} participant IDs")
    print("-" * 80)

    # Define source folders to search for participant data
    # These folders represent different data types and instruments
    source_folders = [
        # "AI-READI/year3+/cgm/CGM-processed/wearable_blood_glucose/continuous_glucose_monitoring/dexcom_g6/",
        "AI-READI/year3+/imaging-combined-clean/retinal_oct/structural_oct/zeiss_cirrus/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/enface/zeiss_cirrus/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/flow_cube/zeiss_cirrus/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/segmentation/zeiss_cirrus/",
        "AI-READI/year3+/imaging-combined-clean/retinal_photography/ir/zeiss_cirrus/",
        "AI-READI/year3+/imaging-combined-clean/retinal_photography/cfp/icare_eidon/",
        "AI-READI/year3+/imaging-combined-clean/retinal_photography/faf/icare_eidon/",
        "AI-READI/year3+/imaging-combined-clean/retinal_photography/ir/icare_eidon/",
        "AI-READI/year3+/imaging-combined-clean/retinal_flio/flio/heidelberg_flio/",
        # "AI-READI/year3+/garmin/Garmin-processed/heart_rate/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/oxygen_saturation/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/physical_activity/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/physical_activity_calorie/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/respiratory_rate/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/sleep/garmin_vivosmart5/",
        # "AI-READI/year3+/garmin/Garmin-processed/stress/garmin_vivosmart5/",
        "AI-READI/year3+/imaging-combined-clean/retinal_oct/structural_oct/topcon_maestro2/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/enface/topcon_maestro2/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/flow_cube/topcon_maestro2/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/segmentation/topcon_maestro2/",
        "AI-READI/year3+/imaging-combined-clean/retinal_photography/cfp/topcon_maestro2/",
        "AI-READI/year3+/imaging-combined-clean/retinal_photography/ir/topcon_maestro2/",
        "AI-READI/year3+/imaging-combined-clean/retinal_photography/cfp/optomed_aurora/",
        "AI-READI/year3+/imaging-combined-clean/retinal_oct/structural_oct/heidelberg_spectralis/",
        "AI-READI/year3+/imaging-combined-clean/retinal_photography/ir/heidelberg_spectralis/",
        "AI-READI/year3+/imaging-combined-clean/retinal_oct/structural_oct/heidelberg_spectralis/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/enface/heidelberg_spectralis/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/flow_cube/heidelberg_spectralis/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/segmentation/heidelberg_spectralis/",
        "AI-READI/year3+/imaging-combined-clean/retinal_photography/ir/heidelberg_spectralis/",
        "AI-READI/year3+/imaging-combined-clean/retinal_oct/structural_oct/topcon_triton/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/enface/topcon_triton/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/flow_cube/topcon_triton/",
        "AI-READI/year3+/imaging-combined-clean/retinal_octa/segmentation/topcon_triton/",
        "AI-READI/year3+/imaging-combined-clean/retinal_photography/cfp/topcon_triton/",
    ]

    print(f"Total source folders to process: {len(source_folders)}")
    print("-" * 80)

    # Initialize logger for tracking operations
    logger = logging.Logwatch("drain", print=True)

    # Connect to Azure Data Lake Storage
    # This will be used to access files in the stage-final-container
    print("Connecting to Azure Data Lake Storage...")
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name="stage-one",
    )
    print("✓ Connected to Azure Data Lake Storage successfully")
    print("-" * 80)

    # List to store all stray folders
    stray_folders = []

    # Process source folders sequentially
    print(f"\nProcessing {len(source_folders)} source folders...")

    for idx, source_folder in enumerate(source_folders, 1):
        try:
            folder_stray_folders = process_source_folder(
                source_folder,
                idx,
                len(source_folders),
                file_system_client,
                participant_ids,
                dry_run,
            )
            stray_folders.extend(folder_stray_folders)
        except Exception as exc:
            print(f"Source folder {source_folder} generated an exception: {exc}")

    # Count total stray items
    total_stray_folders = len(stray_folders)
    print("\n" + "=" * 80)
    if dry_run:
        print(
            f"DRY RUN complete: Found {total_stray_folders} stray folder(s) that would be removed"
        )
    else:
        print(f"Removal complete: Removed {total_stray_folders} stray folder(s)")
    print("=" * 80)
    logger.info(f"Total stray folders processed: {total_stray_folders}")

    print("=" * 80)
    print("Pipeline completed successfully!")
    print("=" * 80)


# Main entry point: run the pipeline when script is executed directly
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Remove stray folders that don't match participant IDs"
    )
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        help="Actually remove the stray folders (default is dry run)",
    )
    args = parser.parse_args()

    dry_run = not args.no_dry_run
    pipeline(dry_run=dry_run)
