import random
import string
import base64
import requests

import config


def generate_random_identifier(k):
    """Generate a random identifier"""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=k))


def pipeline():
    """Register a DOI for the dataset"""

    credentials = config.DATACITE_CREDENTIALS

    # encode the credentials to base64
    credentials = credentials.encode("utf-8")
    credentials = base64.b64encode(credentials)
    credentials = credentials.decode("utf-8")

    url = f"{config.DATACITE_API_URL}/dois"
    headers = {
        "Content-Type": "application/vnd.api+json",
        "Authorization": f"Basic {credentials}",
    }

    identifier = generate_random_identifier(6)

    payload = {
        "data": {
            "type": "dois",
            "attributes": {
                # event this makes it immediately findable. Use false if the publish is later
                "event": "publish",
                "doi": f"10.82914/{identifier}",
                "creators": [
                    {
                        "name": "Gojo Satoru",
                    },
                    {
                        "name": "Itadori Yuji",
                    },
                ],
                "titles": [
                    {
                        "title": f"Dataset {identifier}",
                    }
                ],
                "publisher": "Fairhub",
                "publicationYear": "2024",
                "types": {
                    "resourceTypeGeneral": "Dataset",
                },
                "url": "https://staging.fairhub.io/datasets/1",
            },
        }
    }

    # Register the DOI with DataCite
    response = requests.post(url, headers=headers, json=payload, timeout=10)

    if response.status_code != 201:
        raise ValueError(f"Failed to register DOI: {response.text}")

    return response.json()


# sample response below:

# {
#     "data": {
#         "id": "10.82914/227iwp",
#         "type": "dois",
#         "attributes": {
#             "doi": "10.82914/227iwp",
#             "prefix": "10.82914",
#             "suffix": "227iwp",
#             "identifiers": [],
#             "alternateIdentifiers": [],
#             "creators": [
#                 {"name": "Gojo Satoru", "affiliation": [], "nameIdentifiers": []},
#                 {"name": "Itadori Yuji", "affiliation": [], "nameIdentifiers": []},
#             ],
#             "titles": [{"title": "Dataset 227iwp"}],
#             "publisher": "Fairhub",
#             "container": {},
#             "publicationYear": 2024,
#             "subjects": [],
#             "contributors": [],
#             "dates": [],
#             "language": null,
#             "types": {
#                 "schemaOrg": "Dataset",
#                 "citeproc": "dataset",
#                 "bibtex": "misc",
#                 "ris": "DATA",
#                 "resourceTypeGeneral": "Dataset",
#             },
#             "relatedIdentifiers": [],
#             "relatedItems": [],
#             "sizes": [],
#             "formats": [],
#             "version": null,
#             "rightsList": [],
#             "descriptions": [],
#             "geoLocations": [],
#             "fundingReferences": [],
#             "xml": "",
#             "contentUrl": null,
#             "metadataVersion": 0,
#             "schemaVersion": null,
#             "source": "api",
#             "isActive": true,
#             "state": "findable",
#             "reason": null,
#             "landingPage": null,
#             "viewCount": 0,
#             "viewsOverTime": [],
#             "downloadCount": 0,
#             "downloadsOverTime": [],
#             "referenceCount": 0,
#             "citationCount": 0,
#             "citationsOverTime": [],
#             "partCount": 0,
#             "partOfCount": 0,
#             "versionCount": 0,
#             "versionOfCount": 0,
#             "created": "2024-02-02T00:24:21.000Z",
#             "registered": "2024-02-02T00:24:21.000Z",
#             "published": "2024",
#             "updated": "2024-02-02T00:24:21.000Z",
#         },
#         "relationships": {
#             "client": {"data": {"id": "bumy.ydbuwo", "type": "clients"}},
#             "provider": {"data": {"id": "bumy", "type": "providers"}},
#             "media": {"data": {"id": "10.82914/227iwp", "type": "media"}},
#             "references": {"data": []},
#             "citations": {"data": []},
#             "parts": {"data": []},
#             "partOf": {"data": []},
#             "versions": {"data": []},
#             "versionOf": {"data": []},
#         },
#     }
# }
