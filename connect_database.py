import csv
from astrapy.rest import create_client, http_methods
import uuid, os
import json
import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

cloud_config= {
        'secure_connect_bundle': 'C:\\Users\\Αρχοντία\\Desktop\\secure-connect-vaseis2.zip'
}
auth_provider = PlainTextAuthProvider('jhDTeqZIXRfHZTFJEsCxpMyW', '4W42lNzs91DuL.zUE.8g1gc,-r8C+c-DasDf-0miYLq-6O6ZOrQH80nj9diDB3bncKFDfb9fUiIa6uo.rby61Sk+3wUq471RHC51xZrljIuN+RCbjC.7AziNX-ZLZN1t')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
    print(row[0])
else:
    print("An error occurred.")

cluster = Cluster()
cluster = Cluster(['192.168.0.1', '192.168.0.2'])


session.execute("USE sandra;")


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




             #create table movies
#session.execute("DROP TABLE movies_q1;")
session.execute("CREATE TABLE IF NOT EXISTS movies_q1(userId int ,movieId int,rating float, timestamp timestamp, title text, PRIMARY KEY (userId));")
query = "INSERT INTO movies_q1(userId,movieId,rating,timestamp,title) VALUES (?,?,?,?,?)"
prepared = session.prepare(query)

#for i,item in merged_leftq1.iterrows():
#        session.execute(prepared, (item[0],item[1],item[2],item[3],item[4]))



#session.execute("CREATE TABLE IF NOT EXISTS test_q1(userId int ,movieId int,rating float, timestamp timestamp, title text, PRIMARY KEY (userId));")
#query = "INSERT INTO test_q1(userId,movieId,rating,timestamp,title) VALUES (?,?,?,?,?)"
#prepared = session.prepare(query)


#for i,item in test_q1.iterrows():
#        session.execute(prepared, (item[0],item[1],item[2],item[3],item[4]))

query = session.execute("select userId,movieId from test_q1;")
for userId,movieId in query:
    print(userId,movieId)







#insert values
#session.execute(
  #  """
  #  INSERT INTO movies (movieId, title, genres, rating, timestamp, tagId, tag)
  #  VALUES (%s, %s, %s, %s, %s, %s, %s)
  #  """,
  #  (1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy", 3.5, "2005-04-02 23:53:47", 1, "007")
    #(2, "Toy Storyaaaa", "Animation|Fantasy", 4.2, "2018-04-02 23:53:47", 4, "022")
#)



