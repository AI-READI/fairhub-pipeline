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
    file_paths.extend(row[2] for row in reader)

file_paths.pop(0)

for participant_id in tqdm(participants, desc="Participants"):
    for header in tqdm(headers, desc=f"Headers for {participant_id}", leave=False):
        for file in tqdm(
            file_paths, desc=f"Files for {participant_id}/{header}", leave=False
        ):
            if f"/{participant_id}/" in file and f"/{header}/" in file:
                exists[participant_id][header] = True
                break

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
