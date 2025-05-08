import os
import shutil
import imaging.imaging_classifying_rules as imaging_classifying_rules


def filter_optomed_files(file, outputfolder):
    """
    Filter and process OPTOMED files based on classification rules.

    This function applies classification rules to a DICOM file, extracts relevant information,
    and copies the file to an appropriate output directory based on the classification rule.

    Args:
        file (str): The path to the DICOM file to be processed.
        outputfolder (str): The directory where the processed files will be stored.

    Returns:
        dict: A dictionary containing information about the processed file, including rule, patient ID,
        patient name, laterality, rows, columns, SOP instance UID, series instance UID, filename,
        original file path, and any errors encountered.
    """

    if imaging_classifying_rules.is_dicom_file(file):

        filename = file.split("/")[-1]
        rule = imaging_classifying_rules.find_rule(file)
        b = imaging_classifying_rules.extract_dicom_entry(file)
        laterality = b.laterality
        uid = b.sopinstanceuid
        patientid = b.patientid
        rows = b.rows
        columns = b.columns
        seriesuid = b.seriesuid
        error = b.error
        name = b.name

        original_path = file
        output_path = f"{outputfolder}/{rule}/{rule}_{filename}"
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
        output_path = f"{outputfolder}/{error}/{error}_{filename}"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        shutil.copyfile(original_path, output_path)

        dic = {
            "Input": file,
            "Output": output_path,
            "Error": error,
        }

    return dic
