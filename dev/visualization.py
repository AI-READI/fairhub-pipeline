"""
Generate viz.json from participants.tsv file.
This script reads the participants TSV file and converts it to the visualization JSON format.
"""

import json
import csv
import random
from typing import List, Dict, Any
from collections import defaultdict


def map_study_group_to_viz_group(study_group: str) -> str:
    """Map study_group from participants.tsv to group format in viz.json"""
    mapping = {
        "pre_diabetes_lifestyle_controlled": "Pre-T2DM",
        "oral_medication_and_or_non_insulin_injectable_medication_controlled": "Non-insulin T2DM",
        "insulin_dependent": "Insulin T2DM",
        "healthy": "Healthy",
    }
    return mapping.get(study_group, study_group)


def bin_age(age: int) -> int:
    """
    Convert age to decade representation.
    Returns the decade digit (e.g., 5 for 50s, 6 for 60s, 7 for 70s, etc.)
    """
    return age // 10


def boolean_to_present_missing(value: str) -> str:
    """Convert TRUE/FALSE to Present/Missing"""
    return "Present" if value.upper() == "TRUE" else "Missing"


def generate_coordinates(
    data: List[Dict[str, Any]], seed: int = 42
) -> List[Dict[str, Any]]:
    """
    Generate x, y coordinates for visualization.
    Uses a simple approach: groups similar participants together based on features.
    For a more sophisticated layout, could use t-SNE or UMAP.
    """
    random.seed(seed)

    # Group participants by key features for layout
    feature_groups = defaultdict(list)

    for idx, record in enumerate(data):
        # Create a feature key based on site, group, split, and data availability
        features = (
            record["site"],
            record["group"],
            record["split"],
            record["ECG"],
            record["Clinical"],
            record["FLIO"],
            record["OCT"],
            record["CGM"],
        )
        feature_groups[features].append(idx)

    # Generate coordinates
    for record in data:
        # Use a simple hash-based approach for deterministic coordinates
        # This creates clusters based on similar features
        feature_hash = hash(
            (
                record["site"],
                record["group"],
                record["split"],
                record["ECG"],
                record["Clinical"],
                record["FLIO"],
                record["OCT"],
                record["CGM"],
            )
        )

        # Generate x, y from hash (range doubled: roughly -400 to 400)
        x = (feature_hash % 800) - 400
        y = ((feature_hash // 800) % 800) - 400

        # Add some randomness within the cluster (doubled spacing)
        x += random.randint(-40, 40)
        y += random.randint(-40, 40)

        record["x"] = x
        record["y"] = y

    return data


def read_participants_tsv(tsv_file: str) -> List[Dict[str, Any]]:
    """Read participants.tsv and convert to list of dictionaries"""
    participants = []

    with open(tsv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # Map the data to viz-v2.json format
            participant = {
                "site": row["clinical_site"],
                "group": map_study_group_to_viz_group(row["study_group"]),
                "age": bin_age(int(row["age"])),
                "split": row["recommended_split"],
                "ECG": boolean_to_present_missing(row["cardiac_ecg"]),
                "Clinical": boolean_to_present_missing(row["clinical_data"]),
                "Env": boolean_to_present_missing(row["environment"]),
                "FLIO": boolean_to_present_missing(row["retinal_flio"]),
                "OCT": boolean_to_present_missing(row["retinal_oct"]),
                "OCTA": boolean_to_present_missing(row["retinal_octa"]),
                "CFP": boolean_to_present_missing(row["retinal_photography"]),
                "Fitness": boolean_to_present_missing(row["wearable_activity_monitor"]),
                "CGM": boolean_to_present_missing(row["wearable_blood_glucose"]),
            }
            participants.append(participant)

    return participants


def generate_viz(participants_tsv: str, output_json: str):
    """
    Main function to generate viz.json from participants.tsv
    """
    print(f"Reading participants from {participants_tsv}...")
    participants = read_participants_tsv(participants_tsv)
    print(f"Loaded {len(participants)} participants")

    print("Generating coordinates...")
    participants = generate_coordinates(participants)

    print(f"Writing output to {output_json}...")
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(participants, f, indent=2, ensure_ascii=False)

    print(f"Successfully generated {output_json} with {len(participants)} participants")


if __name__ == "__main__":
    # Generate viz.json from participants.tsv
    generate_viz("particpants.tsv", "viz.json")
