import csv
import pandas as pd

KEYSPACE = "test_cassandra"

#cluster = Cluster(['127.0.0.1'])
#session = cluster.connect()

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
print(merged_leftq1.head(5))
csv_data=merged_leftq1.to_csv('q1.csv',index=False)




newdataset_movie = datasettag.rename(columns={'userId':'userId','movieId ':'movieId ','tag':'tag'  ,'timestamp':'timestamp'})
#print(newdataset_movie.head(2))
merged_leftq2= pd.merge(left = newdataset_movie , right=datasetgenome, how='left', left_on='tag', right_on='tag')
merged_q2_final = pd.merge(left=merged_leftq2, right=df2, how='left', left_on='movieId', right_on='movieId')
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 0)
print(merged_q2_final.head(5))
