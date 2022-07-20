import csv
from astrapy.rest import create_client, http_methods
import uuid, os
import json
import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

csv_filemovie = csv.reader(open('./movie.csv', 'r', encoding="utf8"))
dc = []
datasetmovie=pd.read_csv("movie.csv",sep=';', header=0,on_bad_lines='skip')

df2 = datasetmovie[['movieId', 'title']]


csv_filerating = csv.reader(open('./rating.csv', 'r', encoding="utf8"))
dc3 = []
datasetrating=pd.read_csv("rating.csv",sep=';', header=0,on_bad_lines='skip')


csv_filegenome = csv.reader(open('./genome_tags.csv', 'r', encoding="utf8"))
dc3 = []
datasetgenome = pd.read_csv("genome_tags.csv",sep=',', header=0,on_bad_lines='skip')
#print(datasetgenome.head(3))

csv_filetag = csv.reader(open('./tag.csv', 'r', encoding="utf8"))
dc4 = []
datasettag = pd.read_csv("tag.csv",sep=',', header=0,on_bad_lines='skip')
#print(datasettag.head(3))


merged_leftq1 = pd.merge(left=datasetrating, right=df2, how='left', left_on='movieId', right_on='movieId')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 0)
#print(merged_leftq1.head(5))
merged_leftq1['timestamp']=pd.to_datetime(merged_leftq1['timestamp'])
#print(merged_leftq1.head(5))
#csv_data=merged_leftq1.to_csv('q1.csv',index=False)
test_q1=merged_leftq1.head(5)
#print(test_q1)
data_q1=test_q1.to_json(orient='index')
print(data_q1)

def getAstraHTTPClient():
    """Get Astra connection information from environment variables"""

    ASTRA_DB_ID = os.environ.get('5e95b7e6-25a2-4b81-a7ac-e486ebe7e12b')
    ASTRA_DB_REGION = os.environ.get('us-east1')
    ASTRA_DB_APPLICATION_TOKEN = os.environ.get('AstraCS:oIrscKCzoGIrlhDmceGpohNQ:1018883818a02ea06a05faac3a020648029fdaebe3112707cb2f4d77c299f86c')

    # setup an Astra Client
    return create_client(astra_database_id='5e95b7e6-25a2-4b81-a7ac-e486ebe7e12b',
                         astra_database_region='us-east1',
                         astra_application_token='AstraCS:oIrscKCzoGIrlhDmceGpohNQ:1018883818a02ea06a05faac3a020648029fdaebe3112707cb2f4d77c299f86c')

'''
def createJSONonAstra(astra_http_client):
    """Create a document on Astra using the Document API"""

    doc_uuid = uuid.uuid4()
    ASTRA_DB_KEYSPACE = os.environ.get('ssandra')
    ASTRA_DB_COLLECTION = os.environ.get('testing_coll')

    astra_http_client.request(
        method=http_methods.PUT,
        path=f"/api/rest/v2/namespaces/{ASTRA_DB_KEYSPACE}/collections/{ASTRA_DB_COLLECTION}/{doc_uuid}",
        json_data=json.load(data_q1))

    #ghgfhfg'''