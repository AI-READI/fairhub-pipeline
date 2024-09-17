import os
import shutil
import imaging.imaging_classifying_rules as imaging_classifying_rules


def filter_spectralis_files(file, outputfolder):
    """
    Filters and processes Spectralis DICOM files according to predefined rules and saves the processed files to a specified output folder.

    This function checks if the input file is a valid DICOM file using predefined imaging classification rules. If the file is valid,
    it extracts relevant metadata, constructs an output file path based on these metadata, and copies the file to the output directory.
    If the file is not a valid DICOM, it logs an error and copies the file to an error folder within the output directory.

    Args:
        file (str): The path to the input file to be processed.
        outputfolder (str): The path to the output folder where the processed or error files should be saved.

    Returns:
        dict: A dictionary containing information about the processed file, including the rule applied, PatientID, Rows, Columns,
              Laterality, Input file path, Output file path, and any errors encountered.

    """

    if imaging_classifying_rules.is_dicom_file(file):

        filename = file.split("/")[-1]
        rule = imaging_classifying_rules.find_rule(file)
        b = imaging_classifying_rules.extract_dicom_entry(file)
        laterality = b.laterality
        uid = b.sopinstanceuid
        patientid = b.patientid
        reference = b.referencedsopinstance
        rows = b.rows
        columns = b.columns
        framenumber = b.framenumber
        error = b.error
        original_path = file

        output_path = f"{outputfolder}/{rule}/{rule}_{patientid}_{laterality}_{filename}_{uid}.dcm"

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        shutil.copyfile(original_path, output_path)

        dic = {
            "Rule": rule,
            "PatientID": patientid,
            "Rows": rows,
            "Columns": columns,
            "Laterality": laterality,
            "Input": file,
            "Output": output_path,
            "Error": error,
        }

    else:
        filename = file.split("/")[-1]
        error = "Invalid_dicom"

        original_path = file
        original_path_for_name = file.replace("/", "_")
        output_path = f"{outputfolder}/{error}/{error}_{original_path_for_name}"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        shutil.copyfile(original_path, output_path)

        dic = {
            "Input": file,
            "Output": output_path,
            "Error": error,
        }

    return dic
