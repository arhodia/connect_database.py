import csv
import uuid, os
import json
from cassandra.query import tuple_factory
import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pandas.core import series
pd.options.mode.chained_assignment = None  # default='warn'
from time import time
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement


#************************************************************************************
#**************************  Connect with astra.datastax  ***************************
#************************************************************************************


cloud_config= {
        'secure_connect_bundle': 'C:\\Users\\vasil\\Downloads\\secure-connect-projectvaseis.zip'
}
auth_provider = PlainTextAuthProvider('oadhqNuJmmKueZenbPEYUyys', '_FG.gJq1l8RLHxb3cypHJ4C.+0DCuIyt,zRCA97UE1RocuBgNJ7U_kFPLl9CA0xf+I0C3oDTFYxp,fIqx1CfYJjsFxA1GNlYjZiRq1YErJOoDID0e+8m4_UfK6FrHk8S')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
    print(row[0])
else:
    print("An error occurred.")

session.execute("USE data;")


#******************************************************************************
#********************  Read from 4 csvs the 600 first rows  *******************
#******************************************************************************


#Read and open movie.csv
csv_filemovie = csv.reader(open('./movie.csv', 'r', encoding="utf8"))
dc1 = []
movie_df = pd.read_csv("movie.csv", sep=',', header=0, on_bad_lines='skip')

#Read and open rating.csv
csv_filerating = csv.reader(open('./rating.csv', 'r', encoding="utf8"))
dc2 = []
rating_df = pd.read_csv("rating.csv", sep=',', header=0, on_bad_lines='skip')

#Read and open tag.csv
csv_filetag = csv.reader(open('./tag.csv', 'r', encoding="utf8"))
dc3 = []
tag_df = pd.read_csv("tag.csv", sep=',', header=0, on_bad_lines='skip')

#Read and open genome_tags.csv
csv_filegenome = csv.reader(open('./genome_tags.csv', 'r', encoding="utf8"))
dc4 = []
genome_tags_df = pd.read_csv("genome_tags.csv", sep=',', header=0, on_bad_lines='skip')



#Q1
#Πρώτο merge για το Q1

q1 = pd.merge(left=rating_df, right=movie_df, how='left', left_on='movieId', right_on='movieId')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 0)
q1['timestamp']=pd.to_datetime(q1['timestamp'])
#print(q1.head(50))

#Groupby and get sum() and count() {εύρεση του averageRating}
avq1 = q1.groupby('movieId')['rating'].agg(['sum','count'])
avq1['avg_rating'] = avq1['sum'] / avq1['count']
#print(avq1.head(70))

dfq1 = pd.merge(left=q1, right=avq1, how='left', left_on='movieId', right_on='movieId')
#print('\nfinal\n', dfq1.head(50))

#dataframe με τις στήλες που θέλω μόνο
predf1 = dfq1[['movieId', 'avg_rating', 'timestamp', 'rating', 'title']]
predf1['timestamp'] = pd.to_datetime(predf1['timestamp'])
df1 = predf1.head(600)
#df1.fillna(0)
#print("\nQ1:\n", df1.head(50))


#csv_data = df1.to_csv('moviesbyRMIKRO.csv', index=False)
#df1['timestamp'].encode('utf-8')
#df1['rating'].encode('utf-8')



#Q2
#Εμφάνιση των ταινιών που περιέχουν τη λέξη “star”

olddf2 = movie_df.head(600)
newdf2 = olddf2.copy()
newdf2['title']= newdf2['title'].str.split(" ", expand =False)

newdf2=newdf2.rename(columns={"title": "new title"})
del newdf2['movieId']
del newdf2['genres']
#print(newdf2)

olddf2['new'] = newdf2['new title'].values
df2 = olddf2.head(600)

#df2.fillna(0)
#print("\nQ2:\n", df2.head(100))



#Q3
#Τρίτο merge για το Q3

dfq3 = pd.merge(left=q1, right=avq1, how='left', left_on='movieId', right_on='movieId')
predf3 = dfq3[['movieId', 'timestamp', 'genres', 'avg_rating', 'title']]

newpredf3 = predf3.copy()
#for index, row in predf3.iterrows():
newpredf3['new genres']= newpredf3['genres'].str.split("|", expand =False)
del predf3['genres']

#print("\nNEWpredf3:\n", newpredf3.head(200))
#predf3.concat([predf3,newpredf3], axis=0, ignore_index=True)
#predf3=predf3['genres'].str.split("|")
predf3['genres'] = newpredf3['new genres'].values
df3new = predf3.head(600)

df3 = df3new.sort_values(['timestamp'])

#df3.fillna(0)
#print("\nQ3:\n", df3)

#print("\nQ3:\n", df3.head(50))



#Q4
#Τέταρτο merge για το Q4

#premergedQ4 = dfq3[['movieId', 'title', 'genres', 'avg_rating']]
#print("\npremergedQ4:\n", premergedQ4.head(10))

dfq4 = pd.merge(left=movie_df, right=tag_df, how='left', left_on='movieId', right_on='movieId')
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 0)
#print("\ndfq4:\n", dfq4.head(100))

Q4 = dfq4.groupby('movieId').head(5)
#print("\nQ4:\n", Q4.head(100))

del Q4['userId']
del Q4['timestamp']

Q4.rename(columns={"tag":"topn_tag"}, inplace=True)
#print("\nQ4:\n", Q4.head(100))

Q4['s']=Q4.groupby(['movieId', 'title', 'genres']).cumcount()+1
Q4=Q4.set_index(['s', 'movieId', 'title', 'genres']).unstack(0)
Q4.columns=[f"{x}" for x in Q4.columns]
Q4=Q4.reset_index()

Q4['topn_tags'] = Q4["('topn_tag', 1)"] + (', ' + Q4["('topn_tag', 2)"]).fillna('') + (', ' + Q4["('topn_tag', 3)"]).fillna('') + (', ' + Q4["('topn_tag', 4)"]).fillna('') + (', ' + Q4["('topn_tag', 5)"]).fillna('')

del Q4["('topn_tag', 1)"]
del Q4["('topn_tag', 2)"]
del Q4["('topn_tag', 3)"]
del Q4["('topn_tag', 4)"]
del Q4["('topn_tag', 5)"]

prefinaldf4 = pd.merge(left=Q4, right=avq1, how='left', left_on='movieId', right_on='movieId')

#dataframe με τις στήλες που θέλω μόνο
prepredfQ4 = prefinaldf4[['movieId', 'title', 'genres', 'avg_rating', 'topn_tags']]

newpredf4 = prepredfQ4.copy()
newpredf4['newtopn_tags'] = newpredf4['topn_tags'].str.split(",", expand=False)
del prepredfQ4['topn_tags']

prepredfQ4['topn_tags'] = newpredf4['newtopn_tags'].values

#prepredfQ4.encode('utf-8')

df4 = prepredfQ4.head(600)

#prepredfQ4['new_topn_tags'] = prepredfQ4['topn_tags'].str.split(",", expand = False)
#del prepredfQ4['topn_tags']

#df4 = prepredfQ44.head(200)
#df4.fillna(0)

#print("\nQ4:\n", df4.head(50))



#Q5
#προηγουμενο dataframe: predf5 = pd.merge(left=df3, right=tag_df, how='left', left_on='movieId', right_on='movieId') #left_on= righton= me userId ή timestamp
prepredf5 = pd.merge(left=tag_df, right=avq1, how='left', left_on='movieId', right_on='movieId') #left_on= righton= me userId ή timestamp
predf5 = pd.merge(left=prepredf5, right=movie_df, how='left', left_on='movieId', right_on='movieId')
preQ5 = predf5[['movieId', 'title', 'avg_rating', 'tag']]
df5 = preQ5.head(600)

#df5 = predf5.head(200)

#df5.fillna(0)
#print("\nQ5:\n", df5.head(50))



#***************************************************************
#**************  Create table moviesbyAVGRATE  ***************** Q1
#***************************************************************


session.execute("DROP TABLE IF EXISTS data.moviesbyAVGRATE")

session.execute("CREATE TABLE IF NOT EXISTS moviesbyAVGRATE(movieId int, avg_rating float, timestamp timestamp, rating float, title text, PRIMARY KEY ((movieId), avg_rating, timestamp)) WITH CLUSTERING ORDER BY (avg_rating ASC, timestamp DESC);")

#tic = time()

query = "INSERT INTO moviesbyAVGRATE(movieId,avg_rating,timestamp,rating,title) VALUES (?,?,?,?,?)"
prepared = session.prepare(query)

from cassandra.query import dict_factory
session = cluster.connect('data')
session.row_factory = dict_factory
#rows = session.execute("SELECT userId FROM moviesq1 LIMIT 1")

#FIRST TRY SELECT QUERY
for i,item in df1.iterrows():
        session.execute(prepared, (item[0],item[1],item[2],item[3],item[4]))

#toc = time()
#print(toc - tic)




#***************************************************************
#*****************  Create table moviesbyT  ******************** Q2
#***************************************************************


session.execute("DROP TABLE IF EXISTS data.moviesbyT")

session.execute("CREATE TABLE IF NOT EXISTS moviesbyT(movieId int, title text, genres ascii, new list<text>, PRIMARY KEY (movieId));")

tic = time()

query = "INSERT INTO moviesbyT(movieId, title, genres, new) VALUES (?,?,?,?)"
prepared = session.prepare(query)

from cassandra.query import dict_factory
session = cluster.connect('data')
session.row_factory = dict_factory

#FIRST TRY SELECT QUERY
for i,item in df2.iterrows():
        session.execute(prepared, (item[0],item[1],item[2],item[3]))

toc = time()
print(toc - tic)



#***************************************************************
#*****************  Create table moviesbyG  ******************** Q3
#***************************************************************


session.execute("DROP TABLE IF EXISTS data.moviesbyG")

session.execute("CREATE TABLE IF NOT EXISTS moviesbyG(movieId int, timestamp timestamp, avg_rating float, title text, genres list<text>, PRIMARY KEY ((movieId), timestamp)) WITH CLUSTERING ORDER BY (timestamp ASC);")

tic = time()

query = "INSERT INTO moviesbyG(movieId, timestamp, avg_rating, title, genres) VALUES (?,?,?,?,?)"
prepared = session.prepare(query)

from cassandra.query import dict_factory
session = cluster.connect('data')
session.row_factory = dict_factory

#FIRST TRY SELECT QUERY
for i,item in df3.iterrows():
        session.execute(prepared, (item[0],item[1],item[2],item[3],item[4]))

toc = time()
print(toc - tic)


#***************************************************************
#*****************  Create table movieinfo  ******************** Q4
#***************************************************************


session.execute("DROP TABLE IF EXISTS data.movieinfo")

session.execute("CREATE TABLE IF NOT EXISTS movieinfo(movieId int, title text, genres ascii, avg_rating float, PRIMARY KEY (movieId));")

tic = time()

query = "INSERT INTO movieinfo(movieId, title, genres, avg_rating) VALUES (?,?,?,?)"
prepared = session.prepare(query)

#, avg_rating float, topn_tags list<text>
#, avg_rating, topn_tags

from cassandra.query import dict_factory
session = cluster.connect('data')
session.row_factory = dict_factory

#FIRST TRY SELECT QUERY
for i,item in df4.iterrows():
        session.execute(prepared, (item[0],item[1],item[2],item[3]))

toc = time()
print(toc - tic)


#***************************************************************
#*****************  Create table topnmovies ******************** Q5
#***************************************************************


session.execute("DROP TABLE IF EXISTS data.topnmovies")

session.execute("CREATE TABLE IF NOT EXISTS topnmovies(movieId int, title text, avg_rating float, tag text, PRIMARY KEY(title));")

tic = time()

query = "INSERT INTO topnmovies(movieId, title, avg_rating, tag) VALUES (?,?,?,?)"
prepared = session.prepare(query)

#session.execute("CREATE CUSTOM INDEX fn_prefix ON topnmovies (tag) USING 'org.apache.cassandra.index.sasi.SASIIndex';")

from cassandra.query import dict_factory
session = cluster.connect('data')
session.row_factory = dict_factory

#FIRST TRY SELECT QUERY
for i,item in df5.iterrows():
        session.execute(prepared, (item[0],item[1],item[2],item[3]))

toc = time()
print(toc - tic)