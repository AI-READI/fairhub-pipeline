"""Process spectralis data files locally with the DICOM converter."""

import os
import shutil
import subprocess
from traceback import format_exc

# Spectralis DICOM converter executable path
dicom_executable_location = os.path.abspath(
    "C:\\Users\\sanjay\\Downloads\\spx-dicom-converter\\SP-X_DICOM_Converter.exe"
)

# Input folder: subfolders to process (one per subject/session)
# Output folder: where converted output is written (one subfolder per input subfolder)
input_folder = os.path.abspath("D:\\year3+raw\\spectralis")
output_folder = os.path.abspath("D:\\year3+pre\\spectralis")


def main():
    """Process all subfolders in input_folder with the DICOM converter; write output to output_folder."""

    if not os.path.isdir(input_folder):
        raise SystemExit(f"Input folder does not exist: {input_folder}")

    os.makedirs(output_folder, exist_ok=True)

    subfolders = [
        name
        for name in os.listdir(input_folder)
        if os.path.isdir(os.path.join(input_folder, name))
    ]

    if not subfolders:
        print(f"No subfolders found in {input_folder}")
        return

    for folder_name in subfolders:
        print(f"Processing folder {folder_name}")

        source_dir = os.path.join(input_folder, folder_name)
        temp_output_dir = None

        try:
            temp_output_dir = os.path.join(output_folder, folder_name)
            os.makedirs(temp_output_dir, exist_ok=True)
            output_dir = os.path.join(temp_output_dir, "converted")

            print(f"Running executable for folder {folder_name}")
            subprocess.call([dicom_executable_location, source_dir, output_dir])
        except Exception as e:
            print(f"Command failed with error: {e}")
            error_log = format_exc()
            print(error_log)
            error_log_file = os.path.join(output_folder, f"{folder_name}_error_log.txt")
            with open(error_log_file, "w") as f:
                f.write(error_log)
            if temp_output_dir and os.path.isdir(temp_output_dir):
                shutil.rmtree(temp_output_dir, ignore_errors=True)
            continue

        print(f"Folder {folder_name} processed successfully")


if __name__ == "__main__":
    main()
