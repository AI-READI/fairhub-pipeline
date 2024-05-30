"""Register a DOI for the dataset"""

import base64
import datetime
import json

import azure.storage.blob as azureblob
import requests

import pyfairdatatools
import config


def pipeline():
    """Register a DOI for the dataset"""

    credentials = config.DATACITE_CREDENTIALS

    # encode the credentials to base64
    credentials = credentials.encode("utf-8")
    credentials = base64.b64encode(credentials)
    credentials = credentials.decode("utf-8")

    container = "stage-1-container/AI-READI/metadata/"

    sas_token = azureblob.generate_account_sas(
        account_name="b2aistaging",
        account_key=config.AZURE_STORAGE_ACCESS_KEY,
        resource_types=azureblob.ResourceTypes(container=True, object=True),
        permission=azureblob.AccountSasPermissions(read=True, write=True, list=True),
        expiry=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(hours=1),
    )

    # Get the blob service client
    blob_service_client = azureblob.BlobServiceClient(
        account_url="https://b2aistaging.blob.core.windows.net/",
        credential=sas_token,
    )

    # Get the dataet_descrion.json file within the metadata folder
    blob_content = blob_service_client.get_blob_client(
        container, "dataset_description.json"
    )

    # Download the blob to a stream
    stream = blob_content.download_blob().readall()

    # Load the dataset_description.json file
    dataset_description = json.loads(stream)

    # Create payload for doi registration
    payload = pyfairdatatools.utils.convert_for_datacite(dataset_description)

    url = f"{config.DATACITE_API_URL}/dois"
    headers = {
        "Content-Type": "application/vnd.api+json",
        "Authorization": f"Basic {credentials}",
    }

    # Register the DOI with DataCite
    response = requests.post(url, headers=headers, json=payload, timeout=10)

    if response.status_code != 201:
        raise ValueError(
            f"Failed to register DOI: {response.text} {response.status_code}"
        )

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
