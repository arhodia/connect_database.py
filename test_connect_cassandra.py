import csv
from astrapy.rest import create_client, http_methods
import uuid, os
import json
import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


def getAstraHTTPClient():
    """Get Astra connection information from environment variables"""

    ASTRA_DB_ID = os.environ.get('9cad10ea-2968-467f-a37b-e064ade959d5')
    ASTRA_DB_REGION = os.environ.get('ASTRA_DB_REGION')
    ASTRA_DB_APPLICATION_TOKEN = os.environ.get('ASTRA_DB_APPLICATION_TOKEN')

    # setup an Astra Client
    return create_client(astra_database_id='9cad10ea-2968-467f-a37b-e064ade959d5',
                         astra_database_region='us-east1',
                         astra_application_token='AstraCS:ZTCAQhpRlqdbhQjrXQtblgkf:89f5494a35847943f085dfc3911282afdd0aed5572d6dda51cf6d9c4475cd236')


def createJSONonAstra(astra_http_client):
    """Create a document on Astra using the Document API"""

    doc_uuid = uuid.uuid4()
    ASTRA_DB_KEYSPACE = os.environ.get('sandra')
    ASTRA_DB_COLLECTION = os.environ.get('testing_coll')

    astra_http_client.request(
        method=http_methods.PUT,
        path=f"/api/rest/v2/namespaces/{ASTRA_DB_KEYSPACE}/collections/{ASTRA_DB_COLLECTION}/{doc_uuid}",
        json_data={
            "book": "The Hunger Games",
            "author": "Suzanne Collins",
            "genre": ["fiction"],
        })

    #ghgfhfg