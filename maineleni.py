import csv
import pandas as pd

#KEYSPACE = "test_cassandra"
#cluster = Cluster(['127.0.0.1'])
#session = cluster.connect()


#read and open movie csv
csv_filemovie = csv.reader(open('./movie.csv', 'r', encoding="utf8"))
dc = []
datasetmovie = pd.read_csv("movie.csv", sep=',', header=0, on_bad_lines='skip')
#print(datasetmovie.head(3))


#doylepse to split
#datasetmovie = datasetmovie['genres'].str.split(pat = '|', expand = True)
#print(datasetmovie.head(3))


#dataframe με τις στήλες που θέλω μόνο
df2 = datasetmovie[['movieId', 'title']]


#read and open rating csv
csv_filerating = csv.reader(open('./rating.csv', 'r', encoding="utf8"))
dc2 = []
datasetrating=pd.read_csv("rating.csv", sep=',', header=0, on_bad_lines='skip')
#print(datasetrating.head(3))

#read and open genome_tags csv
csv_filegenome = csv.reader(open('./genome_tags.csv', 'r', encoding="utf8"))
dc3 = []
datasetgenome = pd.read_csv("genome_tags.csv", sep=',', header=0, on_bad_lines='skip')
#print(datasetgenome.head(3))


#read and open tag csv
csv_filetag = csv.reader(open('./tag.csv', 'r', encoding="utf8"))
dc4 = []
datasettag = pd.read_csv("tag.csv", sep=',', header=0, on_bad_lines='skip')
#print(datasettag.head(3))


#πρωτο merge για το Q1
merged_q1 = pd.merge(left=datasetrating, right=df2, how='left', left_on='movieId', right_on='movieId')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 0)
#print(merged_q1.head(5))



newdataset_tag = datasettag.rename(columns={'userId': 'userId', 'movieId ': 'movieId ', 'tag': 'tag', 'timestamp': 'timestamp'})
#print(newdataset_tag.head(2))


#δευτερο merge για το Q2
premerged_q2= pd.merge(left = newdataset_tag, right=datasetgenome, how='left', left_on='tag', right_on='tag')
merged_q2 = pd.merge(left=premerged_q2, right=df2, how='left', left_on='movieId', right_on='movieId')
#print(premerged_q2.head(5))

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 0)
#print(merged_q2.head(5))


#split
#next(csv_filemovie)
#for row in csv_filemovie:
   # element = {"movieId": row[0], "title": row[1], "genres": row[2].split("|")}

#print(csv_filemovie)


#τριτο merge για το Q3
oldmerged_q3= pd.merge(left=datasetmovie, right=datasetrating, how='right', left_on='movieId', right_on='movieId')
#print(oldmerged_q3.head(5))

# Groupby and get sum() and count()
dfQ3 = oldmerged_q3.groupby('movieId')['rating'].agg(['sum','count'])
dfQ3['avg_rating'] = dfQ3['sum'] / dfQ3['count']
#print(dfQ3.head(6))

merged_q3 = pd.merge(left=oldmerged_q3, right=dfQ3, how='left', left_on='movieId', right_on='movieId')
#print(merged_q3.head(20))



#τεταρτο merge για το Q4

#Create Q4 DataFrame.
#dfQ4 = merged_q3[['movieId', 'title', 'genres', 'avg_rating']]
#print(dfQ4.head(7))

old_merged_q4 = pd.merge(left=datasetmovie, right=newdataset_tag, how='right', left_on='movieId', right_on='movieId')
#print(old_merged_q4.head(5))

old2merged_q4 = pd.merge(left=old_merged_q4, right=dfQ3, how='left', left_on='movieId', right_on='movieId')
#print(old2merged_q4.head(5))

#most frequent value in tag
#top_tag = newdataset_tag['tag'].mode()
#print(top_tag)

#TOP 5
#n = 5
#b = newdataset_tag['tag'].value_counts()[:5].index.tolist()
#print(b)

#n = 5
#df['Magnitude'].value_counts().index.tolist()[:n]

#a = newdataset_tag.sort_values(['movieId', 'tag'], ascending=False).groupby('tag').head(5)
#print(a.head(20))

#b = newdataset_tag.groupby('tag').head(5)
#print(b.head(60))


#όλα τα tag για κάθε movie
#b = newdataset_tag.groupby(['movieId','tag']).agg(lambda x:x.value_counts().index[0])
#print(b.head(300))

#5 tag για κάθε movie
merged_q4 = old2merged_q4.groupby('movieId').head(5)
#print(merged_q4.head(200))

#merged_q4 = pd.merge(left=old2merged_q4, right=a, how='left', left_on='movieId', right_on='movieId')
#print(merged_q4.head(90))



#10 movie για κάθε tag
#q5 = newdataset_tag.groupby('tag').head(10)
#print(q5.head(100))


a = newdataset_tag.sort_values(['tag', 'movieId'], ascending=False).groupby('tag').head(10)
#print(a.head(200))

#10 movie για κάθε tag
#merged_q5 = a.groupby('tag').head(10)
#print(merged_q5.head(200))

#όλα τα tag για κάθε movie
c = a.groupby(['tag','movieId']).agg(lambda x:x.value_counts().index[0])
#print(c.head(300))

merged_q5 = pd.merge(left=c, right=merged_q1, how='left', left_on='movieId', right_on='movieId')
print(merged_q5.head(50))

#eleni