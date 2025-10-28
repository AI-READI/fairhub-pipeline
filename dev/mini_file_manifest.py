import csv, os

current_directory = os.getcwd()
input_file = os.path.join(current_directory, "input/ecg-manifest.tsv")
participants_id_list = os.path.join(current_directory, "mini_participant_list.tsv")
output_folder = os.path.join(current_directory, "output")
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(
    output_folder,
    f"fdfdf{os.path.basename(input_file)}"
)
with open(participants_id_list, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    target_ids = {row["person_id"].strip() for row in reader if row.get("person_id")}

with open(input_file, newline="", encoding="utf-8") as f_in, \
     open(output_file, "w", newline="", encoding="utf-8") as f_out:
    reader = csv.DictReader(f_in, delimiter="\t")
    writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames, delimiter="\t")
    writer.writeheader()
    for row in reader:
        if row.get("person_id") in target_ids:
            writer.writerow(row)

print("Saved:", output_file)
