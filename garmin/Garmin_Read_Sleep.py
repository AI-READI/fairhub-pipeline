import os
from fitparse import FitFile
from datetime import datetime, timedelta


def garmin_to_datetime(garmin_timestamp):
    # Garmin epoch: December 31, 1989
    garmin_epoch = datetime(1989, 12, 31)

    # Convert timestamp to timedelta
    delta = timedelta(seconds=garmin_timestamp)

    # Add timedelta to Garmin epoch to get the datetime
    return garmin_epoch + delta


def append_sleep_to_csv(sleep_data, csv_path):
    """
    Appends resting sleep data (identified by the fields 'unknown_0' and 'unknown_253')
    to the specified CSV. The 'unknown_0' is the sleep type and the 'unknown_253' field
    is treated as a UNIX timestamp, but only its time part is considered.

    Parameters:
    - sleep_data: Dictionary containing sleep data.
    - csv_path: Path to the CSV file.
    """

    sleep_type_mapping = {1: "awake", 2: "light", 3: "deep", 4: "rem"}

    sleep_type = sleep_type_mapping.get(sleep_data["unknown_0"], "unknown")

    # Convert the 'unknown_253' to datetime using Garmin epoch
    final_timestamp = garmin_to_datetime(sleep_data["unknown_253"])

    with open(csv_path, "a") as csv_file:
        csv_file.write(f"{sleep_type},{final_timestamp}\n")


def parse_fit_file(input_fit_path, sleep_csv_path):
    """
    Parses the FIT file and writes the relevant data to individual CSV files.

    Parameters:
    - input_fit_path: Path to the input .fit file.
    - sleep_csv_path: Paths to the output CSV files.
    """
    fit_data = {}

    # Writing headers to the CSV files.
    with open(sleep_csv_path, "w") as csv_file:
        csv_file.write("sleep_type,adjusted_timestamp\n")

    with FitFile(input_fit_path) as fitfile:
        for record in fitfile.get_messages():
            message_name = record.name
            record_data = {}

            for record_field in record:
                field_name = record_field.name
                field_value = record_field.value

                if field_name != "unknown":
                    record_data[field_name] = field_value

                if message_name == "unknown_275":
                    has_seen_unknown_275 = True

            if (
                set(record_data.keys()) == {"unknown_0", "unknown_253"}
                and has_seen_unknown_275
            ):
                append_sleep_to_csv(record_data, sleep_csv_path)

            if message_name not in fit_data:
                fit_data[message_name] = []
            fit_data[message_name].append(record_data)

    return fit_data


def convert(
    input_fit_path: str,
    output_path: str,
):
    # directory_name = os.path.splitext(os.path.basename(input_fit_path))[0]
    directory_name = output_path

    # Create directory if it doesn't exist.
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

    #    heart_rate_csv_path = os.path.join(directory_name, 'heart_rate_data.csv')
    sleep_csv_path = os.path.join(directory_name, "sleep_data.csv")

    fit_data = parse_fit_file(input_fit_path, sleep_csv_path)

    with open(os.path.join(directory_name, "fit_data.csv"), "w") as csv_file:
        for message_name, message_records in fit_data.items():
            csv_file.write(f"Message: {message_name}\n")
            for record_data in message_records:
                csv_file.write("\t" + str(record_data) + "\n")

    return


# def main():
#     """
#     Main execution function. Takes the input FIT file, processes it, and writes the data to CSV files.

#     Parameters:
#     - input_fit_path: Path to the input .fit file.
#     """
#     if len(sys.argv) != 2:
#         print("Usage: python script_name.py input_fit_file.fit")
#         return

#     input_fit_path = sys.argv[1]
#     directory_name = os.path.splitext(os.path.basename(input_fit_path))[0]

#     # Create directory if it doesn't exist.
#     if not os.path.exists(directory_name):
#         os.makedirs(directory_name)

#     #    heart_rate_csv_path = os.path.join(directory_name, 'heart_rate_data.csv')
#     sleep_csv_path = os.path.join(directory_name, "sleep_data.csv")

#     fit_data = parse_fit_file(input_fit_path, sleep_csv_path)

#     with open(os.path.join(directory_name, "fit_data.csv"), "w") as csv_file:
#         for message_name, message_records in fit_data.items():
#             csv_file.write(f"Message: {message_name}\n")
#             for record_data in message_records:
#                 csv_file.write("\t" + str(record_data) + "\n")


# if __name__ == "__main__":
#     main()
