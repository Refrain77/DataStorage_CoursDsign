import redis
import pandas as pd
red =redis.Redis(host="localhost",port=6379,db=0,decode_responses=True)
pool=redis.ConnectionPool()
r_pool=redis.Redis(connection_pool=pool)
red.flushall()
file_path="E:\\电影评分数据.xlsx"
data=pd.read_excel(file_path,header=0,sheet_name=0)
data=data.rename(columns={"genel":"genre","Vote Average":"Vote_Average","Vote Count":"Vote_Count"})
data2=pd.read_excel(file_path,header=0,sheet_name=1)
data2=data2.rename(columns={"Vote Average":"Vote_Average","Vote Count":"Vote_Count"})
for i in range(0,len(data)):
    value= dict(Overview=str(data.Overview.iloc[i]),genre=str(data.genre.iloc[i]),
                Vote_Average=float(data.Vote_Average.iloc[i]), Vote_Count=int(data.Vote_Count.iloc[i]))
    red.hmset(f"Title:{data.Title.iloc[i]}",value)
for i in range(0,len(data2)):
    value = dict(Overview=str(data2.Overview.iloc[i]), genre=str(data2.genre.iloc[i]),
                 Vote_Average=float(data2.Vote_Average.iloc[i]), Vote_Count=int(data2.Vote_Count.iloc[i]))
    red.hmset(f"Title:{data2.Title.iloc[i]}",value)
print("导入成功！")
# for key in red.keys():
#     value=red.hgetall(key)
#     value=str(value)
#     with open(f"E:\\store\\{key}.txt","w") as f:
#         f.write(value)