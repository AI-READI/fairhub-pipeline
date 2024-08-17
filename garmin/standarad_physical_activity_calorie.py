import pandas as pd
from datetime import datetime
import json
import glob
import os
from pathlib import Path
import sys

root_dir = sys.argv[1]

def merge_json_files(file_paths , outdir , ptname):
    # Initialize variables to store the combåined header and bodies
    combined_header = None
    combined_body_calorie = []

    for file_path in file_paths:
        # Load the current JSON file
        with open(file_path, 'r') as file:
            data = json.load(file)

            # If the combined_header is not set, take the header from the first file
            if combined_header is None:
                combined_header = data['header']
            
            # Extend the combined_body_calorie list with the calorie list from the current file
            combined_body_calorie.extend(data['body']['activity'])
            
    # Sort the combined_body_calorie list by date_time
    combined_body_calorie.sort(
        key=lambda x: x['effective_time_frame']['date_time']
    )

    # Create the combined dictionary
    combined_data = {
        'header': combined_header,
        'body': {
            'activity': combined_body_calorie
        }
    }

    # Write the combined dictionary to a new JSON file
    with open(outdir + '/' + ptname + '_calorie' + '.json', 'w') as combined_file:
        json.dump(combined_data, combined_file, indent=4)
        

for pt in os.listdir(root_dir):

    if("FitnessTracker-" in pt):

        pt_hr_count = 0

        pt_heartrate_files = []
        
        monitor_files = root_dir + pt  + "/Garmin/Monitor/"
        for entry in os.listdir(monitor_files):
            full_path = os.path.join(monitor_files, entry)
            if os.path.isdir(full_path):
                hr_file = root_dir + pt  + "/Garmin/Monitor/" + entry + "/active_calories_data*.csv"
                for filename in glob.glob(hr_file):
                    pt_heartrate_files.append(filename)
        
        # Print out the list of found CSV files
        for file_path in pt_heartrate_files:

            # Get patient ID
            patient_ID = file_path.split(root_dir)[1].split("/")[0]
            
            # Get HR file name
            hr_file_name = file_path.split("active_calories_data_")[1].replace(".csv" , "")
              
            # Load the CSV file
            calorie_df = pd.read_csv(file_path)

            # Prepare the JSON structure again
            json_output = {
                "header": {
                    "uuid": patient_ID.replace("FitnessTracker" , "AIREADI"),
                    "creation_date_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "user_id": patient_ID.replace("FitnessTracker" , "AIREADI"),
                    "schema_id": {
                        "namespace": "ieee",
                        "name": "physical-activity",
                        "version": 1.0
                    }
                },
                "body": {
                    "activity": []
                }
            }

            for index, row in calorie_df.iterrows():
                original_timestamp = pd.to_datetime(row['datetime'])
                formatted_timestamp = original_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
                
                calorie_entry = {
                    "activity_name": "kcal_burned",
                    "calories_value": {
                        "value": row['active_calories'],
                        "unit": "kcal"
                    },
                    "effective_time_frame": {
                        "date_time": formatted_timestamp
                    }
                }
                pt_hr_count = pt_hr_count + 1
                json_output['body']['activity'].append(calorie_entry)

            formatted_json = json.dumps(json_output, indent=4, sort_keys=False)

            # To save the formatted JSON to a file
            output_dir = Path("physical_activity_calorie_jsons/" +patient_ID + "_physical_activity_calorie")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_filename = patient_ID + "_" + hr_file_name + ".json"
            output_file_path = output_dir / output_filename
            with open(output_file_path , 'w') as f:
                f.write(formatted_json)
                
        
        print(pt.replace("FitnessTracker-" , "") , "," , pt_hr_count)
        ## Merge files:
        file_paths = []
        pt_directory = 'physical_activity_calorie_jsons/' + pt + "_physical_activity_calorie/"
        pt_directory_path = Path(pt_directory)
        out_directory = 'physical_activity_calorie/garmin_vivosmart5/' + pt.replace("FitnessTracker-" , "")
        out_directory_path = Path('physical_activity_calorie/garmin_vivosmart5/' + pt.replace("FitnessTracker-" , ""))
        out_directory_path.mkdir(parents=True, exist_ok=True)
        for file in pt_directory_path.glob('*.json'):
            file_paths.append(file)
        merge_json_files(file_paths , out_directory , pt.replace("FitnessTracker-" , ""))
