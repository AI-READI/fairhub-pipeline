import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
import pytz
import numpy as np


from fitparse import FitFile
from datetime import datetime, timedelta

def convert_garmin_timestamp(base_time, timestamp_16):
    """
    Convert a Garmin 16-bit timestamp to a Python datetime object.

    Parameters:
    - base_time (datetime): Base reference time.
    - timestamp_16 (int): Garmin 16-bit timestamp to convert.

    Returns:
    - datetime: Corresponding datetime for the given timestamp.
    """
    
    # Calculate the number of seconds between the base time 
    # and Garmin's epoch time, which is 1989-12-31
    mesgTimestamp = int((base_time - datetime(1989, 12, 31)).total_seconds())
    
    # Update the message timestamp using the provided 16-bit timestamp.
    # The operation ensures we only consider the least significant 16 bits of mesgTimestamp
    # and adjust for any wrap-around in the 16-bit timestamp_16 value.
    mesgTimestamp += (timestamp_16 - (mesgTimestamp & 0xFFFF)) 
    #& 0xFFFF

    # Convert the final timestamp (seconds since 1989-12-31) 
    # back to a Python datetime object
    result_datetime = datetime(1989, 12, 31) + timedelta(seconds=mesgTimestamp)
    
    # Return the resulting datetime object
    return result_datetime


def garmin_to_datetime(garmin_timestamp):
    # Garmin epoch: December 31, 1989
    garmin_epoch = datetime(1989, 12, 31)
    
    # Convert timestamp to timedelta
    delta = timedelta(seconds=garmin_timestamp)
    
    # Add timedelta to Garmin epoch to get the datetime
    result_datetime = garmin_epoch + delta
    return result_datetime

def append_resting_heart_rate_to_csv(resting_heart_rate_data, base_time, csv_path):
    """
    Appends resting heart rate data (identified by the fields 'unknown_0', 'unknown_1', and 'unknown_253') 
    to the specified CSV. The 'unknown_0' is the resting heart rate, 'unknown_1' is the current day resting heart rate,
    and the 'unknown_253' field is treated as a UNIX timestamp, but only its time part is considered.

    Parameters:
    - resting_heart_rate_data: Dictionary containing resting heart rate data.
    - base_time: Base timestamp from the .fit file.
    - csv_path: Path to the CSV file.
    """
    resting_heart_rate = resting_heart_rate_data['unknown_0']
    current_day_resting_heart_rate = resting_heart_rate_data['unknown_1']

    # Convert the 'unknown_253' to datetime using Garmin epoch
    final_timestamp = garmin_to_datetime(resting_heart_rate_data['unknown_253'])

    with open(csv_path, 'a') as csv_file:
        csv_file.write(f"{resting_heart_rate},{current_day_resting_heart_rate},{final_timestamp}\n")


def append_respiration_rate_to_csv(respiration_rate_data, base_time, csv_path):
    """
    Appends respiration rate data (identified by the fields 'unknown_0' and 'unknown_253') to the specified CSV.
    The 'unknown_0' is treated as respiration rate in hundredths, and the 'unknown_253' field is treated as a UNIX timestamp, but only its time part is considered.

    Parameters:
    - respiration_rate_data: Dictionary containing respiration rate data.
    - base_time: Base timestamp from the .fit file.
    - csv_path: Path to the CSV file.
    """
    # Convert the 'unknown_0' to its correct value by dividing by 100
    respiration_rate = respiration_rate_data['unknown_0'] / 100

    # Convert the 'unknown_253' to datetime using Garmin epoch
    final_timestamp = garmin_to_datetime(respiration_rate_data['unknown_253'])

    with open(csv_path, 'a') as csv_file:
        csv_file.write(f"{respiration_rate},{final_timestamp}\n")


def append_spo2_to_csv(spo2_data, base_time, csv_path):
    """
    Appends SpO2 data (identified by the fields 'unknown_0', 'unknown_1', 'unknown_2', and 'unknown_253') to the specified CSV.
    The 'unknown_253' field is treated as a UNIX timestamp, but only its time part is considered.

    Parameters:
    - spo2_data: Dictionary containing SpO2 data.
    - base_time: Base timestamp from the .fit file.
    - csv_path: Path to the CSV file.
    """

    # Convert the 'unknown_253' to datetime using Garmin epoch
    final_timestamp = garmin_to_datetime(spo2_data['unknown_253'])
    
    # Check if 'unknown_2' is 3, and replace with "periodic" if so
    if spo2_data['unknown_2'] == 3:
        unknown_2_value = "periodic"
    else:
        unknown_2_value = spo2_data['unknown_2']
    
    with open(csv_path, 'a') as csv_file:
        csv_file.write(f"{spo2_data['unknown_0']},{spo2_data['unknown_1']},{unknown_2_value},{final_timestamp}\n")



def append_heart_rate_to_csv(heart_rate_data, base_time, csv_path):
    """
    Appends heart rate data to the specified CSV.

    Parameters:
    - heart_rate_data: Dictionary containing heart rate data.
    - base_time: Base timestamp from the .fit file.
    - csv_path: Path to the CSV file.
    """
    #adjusted_timestamp = base_time + timedelta(seconds=heart_rate_data['timestamp_16'])
    # Localize the timestamp to UTC (or whichever timezone it's originally in)
    #utc_time = pytz.utc.localize(adjusted_timestamp)

    # Convert the UTC time to PST
    #pst_time = utc_time.astimezone(pytz.timezone('US/Pacific'))
    
    # Extract the timestamp_16 value
    adjusted_dt = convert_garmin_timestamp (base_time, heart_rate_data['timestamp_16'])

    with open(csv_path, 'a') as csv_file:
        csv_file.write(f"{heart_rate_data['heart_rate']},{adjusted_dt}\n")

def append_active_calories_to_csv(calories_data, base_time, csv_path):
    """
    Appends active calories data to the specified CSV.

    Parameters:
    - calories_data: Dictionary containing active calories data.
    - base_time: Base timestamp from the .fit file.
    - csv_path: Path to the CSV file.
    """

    #adjusted_timestamp = base_time + timedelta(seconds=calories_data['timestamp_16'])
    adjusted_timestamp = convert_garmin_timestamp (base_time, calories_data['timestamp_16'])

    active_calories = calories_data['active_calories']
    active_time = calories_data['active_time']
    activity_type = calories_data['activity_type']
    current_activity_type_intensity = calories_data['current_activity_type_intensity'][0] if calories_data['current_activity_type_intensity'] else ''
    cycles = calories_data['cycles']
    intensity = calories_data['intensity']
    
    with open(csv_path, 'a') as csv_file:
        csv_file.write(f"{active_calories},{active_time},{activity_type},{current_activity_type_intensity},{cycles},{intensity},{adjusted_timestamp}\n")

def activity_type_to_csv(activity_data, csv_path):
    """
    Appends activity type data to the specified CSV.

    Parameters:
    - activity_data: Dictionary containing activity type data.
    - csv_path: Path to the CSV file.
    """
    timestamp = activity_data['timestamp']
    activity_type = activity_data['activity_type']
    intensity = activity_data['intensity']
    current_activity_type_intensity = activity_data['current_activity_type_intensity'][0] if activity_data['current_activity_type_intensity'] else ''
    
    with open(csv_path, 'a') as csv_file:
        csv_file.write(f"{activity_type},{current_activity_type_intensity},{intensity},{timestamp}\n")

def stress_level_to_csv(stress_data, csv_path):
    """
    Appends stress level data to the specified CSV.

    Parameters:
    - stress_data: Dictionary containing stress level data.
    - csv_path: Path to the CSV file.
    """
    stress_timestamp = stress_data['stress_level_time']
    stress_value = stress_data['stress_level_value']
    
    with open(csv_path, 'a') as csv_file:
        csv_file.write(f"{stress_value},{stress_timestamp}\n")

def parse_fit_file(input_fit_path, heart_rate_csv_path, active_calories_csv_path, activity_type_csv_path, stress_level_csv_path, spo2_csv_path, respiration_rate_csv_path, resting_heart_rate_csv_path):

    """
    Parses the FIT file and writes the relevant data to individual CSV files.

    Parameters:
    - input_fit_path: Path to the input .fit file.
    - heart_rate_csv_path, active_calories_csv_path, activity_type_csv_path, stress_level_csv_path: Paths to the output CSV files.
    """
    fit_data = {}
    base_time = None  
    has_seen_unknown_211 = False
    has_seen_unknown_297 = False
    has_seen_unknown_269 = False

    #Rollover issue	
    max_timestamp_16 = 65536  # Assuming timestamp_16 is 16-bits. Adjust if not.

    #heart-rate [timestamp_16]
    previous_timestamp = 0
    rollover_offset = 0

    #calories [timestamp_16]
    calories_previous_timestamp = 0
    calories_rollover_offset = 0
    

    # Writing headers to the CSV files.
    with open(resting_heart_rate_csv_path, 'w') as csv_file:
        csv_file.write("resting_heart_rate,current_day_resting_heart_rate,datetime\n")
    with open(respiration_rate_csv_path, 'w') as csv_file:
        csv_file.write("respiration_rate(breaths/min),datetime\n")
    with open(spo2_csv_path, 'w') as csv_file:
        csv_file.write("spo2 (per minute),confidence,mode,datetime\n")
    with open(heart_rate_csv_path, 'w') as csv_file:
        csv_file.write("heart_rate (bpm),datetime\n")
    with open(active_calories_csv_path, 'w') as csv_file:
        csv_file.write("active_calories,active_time,activity_type,current_activity_type_intensity,cycles,intensity,datetime\n")
    with open(activity_type_csv_path, 'w') as csv_file:
        csv_file.write("activity_type,current_activity_type_intensity,intensity,datetime\n")
    with open(stress_level_csv_path, 'w') as csv_file:
        csv_file.write("stress_value (per minute),datetime\n")
    
    with FitFile(input_fit_path) as fitfile:
        for record in fitfile.get_messages():
            message_name = record.name
            record_data = {}
            
            for record_field in record:
                field_name = record_field.name
                field_value = record_field.value
                
                if field_name != "unknown":
                    record_data[field_name] = field_value
        
                if message_name == "unknown_211":
                    has_seen_unknown_211 = True
                    has_seen_unknown_297 = False
                    has_seen_unknown_269 = False
                elif message_name == "unknown_297":
                    has_seen_unknown_297 = True
                    has_seen_unknown_211 = False
                    has_seen_unknown_269 = False
                elif message_name == "unknown_269":
                    has_seen_unknown_269 = True
                    has_seen_unknown_211 = False
                    has_seen_unknown_297 = False

            if list(record_data.keys())[0] == 'timestamp' and isinstance(record_data['timestamp'], datetime):
                base_time = record_data['timestamp']

            if 'heart_rate' in record_data and 'timestamp_16' in record_data and base_time:
                current_timestamp = record_data['timestamp_16']
                # Check for rollover
                if current_timestamp < previous_timestamp:
                    rollover_offset += max_timestamp_16

                adjusted_timestamp = current_timestamp + rollover_offset
                record_data['timestamp_16'] = adjusted_timestamp

                append_heart_rate_to_csv(record_data, base_time, heart_rate_csv_path)

                # Update previous_timestamp for next iteration
                previous_timestamp = current_timestamp

            if 'active_calories' in record_data and 'timestamp_16' in record_data and base_time:
                calories_current_timestamp = record_data['timestamp_16']
                # Check for rollover
                if calories_current_timestamp < calories_previous_timestamp:
                    calories_rollover_offset += max_timestamp_16

                calories_adjusted_timestamp = calories_current_timestamp + calories_rollover_offset
                record_data['timestamp_16'] = calories_adjusted_timestamp

                append_active_calories_to_csv(record_data, base_time, active_calories_csv_path)

                # Update previous_timestamp for next iteration
                calories_previous_timestamp = calories_current_timestamp

            if 'current_activity_type_intensity' in record_data and 'timestamp' in record_data and not 'timestamp_16' in record_data:
                activity_type_to_csv(record_data, activity_type_csv_path)
            
            if 'stress_level_time' in record_data:
                stress_level_to_csv(record_data, stress_level_csv_path)

            if set(record_data.keys()) == set(['unknown_0', 'unknown_1', 'unknown_2', 'unknown_253']) and base_time and has_seen_unknown_269:
                 append_spo2_to_csv(record_data, base_time, spo2_csv_path)

            if set(record_data.keys()) == set(['unknown_0', 'unknown_253']) and base_time and has_seen_unknown_297:
                 append_respiration_rate_to_csv(record_data, base_time, respiration_rate_csv_path)
		
            if set(record_data.keys()) == set(['unknown_0', 'unknown_1', 'unknown_253']) and base_time and has_seen_unknown_211:
                append_resting_heart_rate_to_csv(record_data, base_time, resting_heart_rate_csv_path)


            if message_name not in fit_data:
                fit_data[message_name] = []
            fit_data[message_name].append(record_data)
    
    return fit_data

def plot_csv_data(file_path, y_axis_label, x_axis_label, ignore_condition=None):
    """Function to plot data from a CSV file."""
    df = pd.read_csv(file_path)

    # Handle any filtering conditions
    if ignore_condition:
        column, condition_value, operation = ignore_condition
        if operation == '<':
            df.loc[df[column] < condition_value, column] = np.nan
        elif operation == '<=':
            df.loc[df[column] <= condition_value, column] = np.nan

    #df = df.dropna(subset=[y_axis_label])
    plt.figure(figsize=(12, 6))
    plt.plot(pd.to_datetime(df[x_axis_label]), df[y_axis_label], label=y_axis_label)
    plt.title(f'{y_axis_label} over {x_axis_label}')
    plt.xlabel(x_axis_label)
    plt.ylabel(y_axis_label)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45)


    # Save the plot with a name based on the CSV file name, but with a .png extension
    output_file_name = os.path.splitext(os.path.basename(file_path))[0] + ".png"
    output_path = os.path.join(os.path.dirname(file_path), output_file_name)
    plt.savefig(output_path)
    plt.close()  # Close the figure

def process_fit_file(input_fit_path):

    directory_name = os.path.splitext(os.path.basename(input_fit_path))[0]

    # Extract the input file's base name without its extension
    input_filename = os.path.splitext(os.path.basename(input_fit_path))[0]

    # Create directory if it doesn't exist.
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

    collection_date = None

    #Find the collection date	
    with FitFile(input_fit_path) as fitfile:
        for record in fitfile.get_messages():
            message_name = record.name
            record_data = {}

            for record_field in record:
                field_name = record_field.name
                field_value = record_field.value
                
                if field_name != "unknown":
                    record_data[field_name] = field_value
 
            if list(record_data.keys())[0] == 'timestamp' and isinstance(record_data['timestamp'], datetime):
               collection_date = record_data['timestamp'].date()
               break  # Exit after finding the collection date

    # Use "NoCollectionDate" if collection_date is None
    date_str = "NoCollectionDate" if collection_date is None else collection_date.strftime("%Y-%m-%d")

    # Include input filename in the CSV filenames
    heart_rate_csv_path = os.path.join(directory_name, f'heart_rate_data_{date_str}_{input_filename}.csv')
    active_calories_csv_path = os.path.join(directory_name, f'active_calories_data_{date_str}_{input_filename}.csv')
    activity_type_csv_path = os.path.join(directory_name, f'activity_type_data_{date_str}_{input_filename}.csv')
    stress_level_csv_path = os.path.join(directory_name, f'stress_level_data_{date_str}_{input_filename}.csv')
    spo2_csv_path = os.path.join(directory_name, f'spo2_data_{date_str}_{input_filename}.csv')  # Define path for SpO2 CSV
    respiration_rate_csv_path = os.path.join(directory_name, f'respiration_rate_data_{date_str}_{input_filename}.csv')
    resting_heart_rate_csv_path = os.path.join(directory_name, f'resting_heart_rate_data_{date_str}_{input_filename}.csv')

    fit_data = parse_fit_file(input_fit_path, heart_rate_csv_path, active_calories_csv_path, activity_type_csv_path, stress_level_csv_path, spo2_csv_path, respiration_rate_csv_path, resting_heart_rate_csv_path)

    with open(os.path.join(directory_name, 'fit_data.csv'), 'w') as csv_file:
        for message_name, message_records in fit_data.items():
            csv_file.write(f"Message: {message_name}\n")
            for record_data in message_records:
                csv_file.write("\t" + str(record_data) + "\n")

    plot_csv_data(respiration_rate_csv_path, 'respiration_rate(breaths/min)', 'datetime',  ignore_condition=('respiration_rate(breaths/min)', 0, '<='))
    plot_csv_data(spo2_csv_path, 'spo2 (per minute)', 'datetime', ignore_condition=('spo2 (per minute)', 0, '<='))
    plot_csv_data(heart_rate_csv_path, 'heart_rate (bpm)', 'datetime', ignore_condition=('heart_rate (bpm)', 0, '<='))
    plot_csv_data(stress_level_csv_path, 'stress_value (per minute)', 'datetime', ignore_condition=('stress_value (per minute)', 0, '<='))

def main():
    """
    Main execution function. Processes each input FIT file and writes the data to CSV files.
    """
    if len(sys.argv) < 2:
        print("Usage: python script_name.py input_fit_file.fit [input_fit_file2.fit ...]")
        return

    # Process each FIT file passed as argument
    for input_fit_path in sys.argv[1:]:
        process_fit_file(input_fit_path)


if __name__ == "__main__":
    main()

