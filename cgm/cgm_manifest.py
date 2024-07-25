import os
import json

# from datetime import datetime


class CGMManifest:
    """Class for calculating the sampling extent of continuous glucose monitoring data"""

    def __init__(
        self,
    ):
        self.output_data = []

    def calculate_sampling_extent(
        self,
        directory: str,
    ):
        # Define date format as used in JSON files
        # date_format = "%Y-%m-%dT%H:%M:%SZ"

        # Traverse through all files in the directory and its subdirectories
        for root, dirs, files in sorted(os.walk(directory)):
            dirs.sort()  # Sort directories
            for file in sorted(files):  # Sort files if needed
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    with open(file_path, "r") as json_file:
                        try:
                            data = json.load(json_file)
                            if data["body"]:
                                # Initialize variables for glucose calculation
                                total_glucose = 0
                                num_records = 0
                                unique_days = set()

                                for record in data["body"]:
                                    if (
                                        "blood_glucose" in record
                                        and "value" in record["blood_glucose"]
                                    ):
                                        glucose_value = record["blood_glucose"]["value"]
                                        if glucose_value == "Low":
                                            glucose_value = 70
                                        elif glucose_value == "High":
                                            glucose_value = 200
                                        else:
                                            glucose_value = int(glucose_value)

                                        total_glucose += glucose_value
                                        num_records += 1

                                        # Handling date for unique days calculation
                                        date_str = record["effective_time_frame"][
                                            "time_interval"
                                        ]["start_date_time"].split("T")[0]
                                        unique_days.add(date_str)

                                # Calculate average blood glucose if there are records
                                if num_records > 0:
                                    average_glucose = total_glucose / num_records
                                else:
                                    average_glucose = None

                                # Extract additional metadata
                                participant_id = data["header"]["patient_id"].split(
                                    "-"
                                )[-1]
                                glucose_sensor_id = data["body"][0]["source_device_id"]
                                manufacturer = "Dexcom"  # As an example
                                manufacturer_model_name = "G6"  # As an example

                                # Append metadata for CSV
                                self.output_data.append(
                                    [
                                        participant_id,
                                        file_path,
                                        num_records,
                                        average_glucose,
                                        len(unique_days),
                                        glucose_sensor_id,
                                        manufacturer,
                                        manufacturer_model_name,
                                    ]
                                )
                        except (KeyError, IndexError, json.JSONDecodeError) as e:
                            print(f"Error processing file {file_path}: {e}")

    def calculate_file_sampling_extent(self, file_path: str, glucose_filepath: str):
        if file_path.endswith(".json"):
            with open(file_path, "r") as json_file:
                try:
                    data = json.load(json_file)
                    if data["body"]:
                        # Initialize variables for glucose calculation
                        total_glucose = 0
                        num_records = 0
                        unique_days = set()

                        for record in data["body"]:
                            if (
                                "blood_glucose" in record
                                and "value" in record["blood_glucose"]
                            ):
                                glucose_value = record["blood_glucose"]["value"]
                                if glucose_value == "Low":
                                    glucose_value = 70
                                elif glucose_value == "High":
                                    glucose_value = 200
                                else:
                                    glucose_value = int(glucose_value)

                                total_glucose += glucose_value
                                num_records += 1

                                # Handling date for unique days calculation
                                date_str = record["effective_time_frame"][
                                    "time_interval"
                                ]["start_date_time"].split("T")[0]
                                unique_days.add(date_str)

                        # Calculate average blood glucose if there are records
                        if num_records > 0:
                            average_glucose = total_glucose / num_records
                        else:
                            average_glucose = None

                        # Extract additional metadata
                        participant_id = data["header"]["patient_id"].split("-")[-1]
                        glucose_sensor_id = data["body"][0]["source_device_id"]
                        manufacturer = "Dexcom"  # As an example
                        manufacturer_model_name = "G6"  # As an example

                        # Append metadata for CSV
                        self.output_data.append(
                            [
                                participant_id,
                                glucose_filepath,
                                num_records,
                                average_glucose,
                                len(unique_days),
                                glucose_sensor_id,
                                manufacturer,
                                manufacturer_model_name,
                            ]
                        )
                except (KeyError, IndexError, json.JSONDecodeError) as e:
                    print(f"Error processing file {file_path}: {e}")

    def write_tsv(
        self,
        file_path: str,
    ):
        # Write the data to a TSV file
        with open(file_path, "w") as f:
            f.write(
                "participant_id\tglucose_filepath\tglucose_level_record_count\taverage_glucose_level_mg_dl\tglucose_sensor_sampling_duration_days\tglucose_sensor_id\tmanufacturer\tmanufacturer_model_name\n"
            )
            for line in self.output_data:
                f.write("\t".join(map(str, line)) + "\n")


# Replace this with the path to your main directory
# main_directory = "wearable_blood_glucose/continuous_glucose_monitoring/dexcom_g6/"
# calculate_sampling_extent(main_directory)
