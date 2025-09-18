"""Process Spectralis data files"""

import subprocess
import os
import shutil


dicom_executable_location = os.path.abspath(
    "C:\\Users\\sanjay\\Downloads\\dicom-converter\\SP-X_DICOM_Converter.exe"
)


def main():  # sourcery skip: low-code-quality
    """dev - test the executable"""

    data_source_dir = "C:\\Users\\sanjay\\Downloads\\UW_Spectralis_20231204-20231208"
    output_dir = (
        "C:\\Users\\sanjay\\Downloads\\UW_Spectralis_20231204-20231208_CONVERTED"
    )

    # delete the output dir if it exists
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    print("Starting")

    subprocess.call([dicom_executable_location, data_source_dir, output_dir])

    print("Done")


if __name__ == "__main__":
    main()
