import csv
import os
from tqdm import tqdm

current_directory = os.getcwd()

# Load the participants-datatype.tsv file
participants_tsv_path = os.path.join(current_directory, "participants-datatype.tsv")

headers = []
participants = []

# Read the participants-datatype.tsv file
with open(participants_tsv_path, "r") as file:
    participants_tsv = csv.reader(file, delimiter="\t")

    # Extract the headers
    headers = next(participants_tsv)

    # Remove the first element from the headers list
    headers.pop(0)
    participants.extend(row[0] for row in participants_tsv)


exists = {}

for participant in participants:
    for header in headers:
        if participant not in exists:
            exists[participant] = {}

        exists[participant][header] = False

# Load the file-manifest.tsv file
file_manifest_tsv = os.path.join(current_directory, "file-manifest.tsv")

file_paths = []

with open(file_manifest_tsv, "r") as file:
    reader = csv.reader(file, delimiter="\t")
    file_paths.extend(
        {
            "path": row[2],
            "id_in_participants_list": None,
        }
        for row in reader
    )
file_paths.pop(0)


for participant_id in tqdm(participants, desc="Participants"):
    for header in tqdm(headers, desc=f"Headers for {participant_id}", leave=False):
        for i, file in enumerate(
            tqdm(file_paths, desc=f"Files for {participant_id}/{header}", leave=False)
        ):
            file_path = file["path"]

            if f"/{participant_id}/" in file_path and f"/{header}/" in file_path:
                exists[participant_id][header] = True
                file_paths[i]["id_in_participants_list"] = True


new_participants_tsv = os.path.join(current_directory, "new_participants.tsv")

with open(new_participants_tsv, "w") as file:
    writer = csv.writer(file, delimiter="\t")

    h = ["participant_id"]
    h.extend(headers)

    writer.writerow(h)

    for participant_id in participants:
        row = [participant_id]
        for header in headers:
            if exists[participant_id][header]:
                row.append("TRUE")
            else:
                row.append("FALSE")

        writer.writerow(row)

# Get the list of files whose id is not in the participants list
extra_files = []
for file in file_paths:
    if file["id_in_participants_list"] is None:
        extra_files.append(file["path"])

# Print the list of extra files
print("Extra files:")
for file in extra_files:
    print(file)

# Get the list of participant ids who don't have any date
participants_without_data = []
for participant_id in participants:
    if not any(exists[participant_id].values()):
        participants_without_data.append(participant_id)

# Print the list of participants without data
print("Participants without data:")
for participant in participants_without_data:
    print(participant)
