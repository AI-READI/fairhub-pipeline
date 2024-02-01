import requests

import config


def pipeline():
    """Register a DOI for the dataset"""

    url = f"{config.DATACITE_API_URL}/dois"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.DATACITE_CREDENTIALS}",
    }

    payload = {
        "data": {
            "type": "dois",
            "attributes": {
                "doi": "10.5072/1234",
                "url": "https://example.com",
                "creators": [
                    {"name": "Doe, John", "affiliation": "University of Example"}
                ],
                "titles": [{"title": "Example Dataset"}],
                "publisher": "University of Example",
                "publicationYear": "2022",
                "types": {"resourceTypeGeneral": "Dataset"},
            },
        }
    }

    # Register the DOI with DataCite
    response = requests.post(url, headers=headers, json=payload, timeout=10)

    print(response.status_code)
    print(response.text)

    if response.status_code != 201:
        raise ValueError(f"Failed to register DOI: {response.text}")

    return response.json()
