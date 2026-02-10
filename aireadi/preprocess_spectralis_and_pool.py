"""Process spectralis data files locally and zip output preserving folder structure."""

import os
import shutil
import subprocess
import zipfile
from traceback import format_exc

# Spectralis DICOM converter executable path
dicom_executable_location = os.path.abspath(
    "C:\\Users\\b2aiUsr\\Downloads\\spx-dicom-converter\\SP-X_DICOM_Converter.exe"
)

# Input folder: subfolders to process (one per subject/session)
# Output folder: where converted zip files are written
input_folder = os.path.abspath(r"C:\path\to\spectralis_input")
output_folder = os.path.abspath(r"C:\path\to\spectralis_output")


def main():
    """Process all subfolders in input_dir with the DICOM converter and write zips to output_dir."""

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
            temp_output_dir = os.path.join(output_folder, ".temp", folder_name)
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

        zip_path = os.path.join(output_folder, f"{folder_name}.zip")
        print(f"Creating zip file {zip_path}")

        with zipfile.ZipFile(zip_path, "w") as archive:
            if os.path.isdir(output_dir):
                for dir_path, _dir_names, file_list in os.walk(output_dir):
                    for file in file_list:
                        file_path = os.path.join(dir_path, file)
                        archive_path = os.path.relpath(file_path, output_dir)
                        archive.write(filename=file_path, arcname=archive_path)

        if temp_output_dir and os.path.isdir(temp_output_dir):
            shutil.rmtree(temp_output_dir, ignore_errors=True)

        print(f"Folder {folder_name} processed successfully")


if __name__ == "__main__":
    main()
