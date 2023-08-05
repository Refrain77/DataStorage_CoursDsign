import pymysql
import redis

red = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
pool = redis.ConnectionPool()
r_pool = redis.Redis(connection_pool=pool)
con = pymysql.connect(
    host='localhost',
    user='root',
    password='1241384713',
    database='movie'
)
cursor = con.cursor()
cursor.execute("DROP TABLE IF EXISTS Movie")
sql = """
CREATE TABLE Movie(
    Title VARCHAR(255) ,
    Vote_Average VARCHAR(255),
    Overview TEXT,
    Genre VARCHAR(255),
    Vote_Count VARCHAR(255)
    );
"""
cursor.execute(sql)
query = "INSERT INTO movie (Title, Vote_Average, Overview, Genre, Vote_Count) VALUES (%s, %s, %s, %s, %s)"
data = []
for key in red.keys():
    title = key
    vote_Average = red.hget(key, "Vote_Average")
    overview = red.hget(key, "Overview")
    genre = red.hget(key, "genre")
    vote_Count = red.hget(key, "Vote_Count")
    data.append((title, vote_Average, overview, genre, vote_Count))
cursor.executemany(query, data)
con.commit()

print("保存成功！")
sql2=""" 
SELECT * FROM movie WHERE Title=%s   
"""
value="Outside The Law"
cursor.execute(sql2,value)
result=cursor.fetchall()
print(result)