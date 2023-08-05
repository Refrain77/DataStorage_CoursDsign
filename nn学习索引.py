import redis
import torch
import torch.nn as nn
import numpy as np
import time
r=redis.Redis(host="localhost",port=6379,db=0,decode_responses=True)
array=[]
num=0
t1=time.time()
for k in r.keys():
    title=r.hget(k,"Title")
    vote_average=r.hget(k,"Vote_Average")
    vote_count=r.hget(k,"Vote_Count")
    if vote_average is not None:
        vote_average=float(vote_average)
        v=((num,k,vote_count),vote_average)
        array.append(v)
        num+=1
t2=time.time()
model=nn.Sequential(
    nn.Linear(1,10),
    nn.ReLU(),
    nn.Linear(10,1)
)
criterion=nn.MSELoss()
opt=torch.optim.Adam(model.parameters())
data=[]
for a in array:
    l=[a[0],a[1]]
    data.append(l)
data = np.array([(a[0][0], a[1]) for a in array])
values = torch.tensor(data[:, 1], dtype=torch.float32).unsqueeze(dim=1)
positions = torch.tensor(data[:, 0], dtype=torch.float32).unsqueeze(dim=1)
num_epochs = 100
time1=time.time()
for epoch in range(num_epochs):
    outputs = model(values)
    loss = criterion(outputs, positions)
    opt.zero_grad()
    loss.backward()
    opt.step()
time2=time.time()
min=float(input("请输入最小值:"))
max=float(input("请输入最大值:"))
t3=time.time()
query = torch.tensor([[min], [max]], dtype=torch.float32)
predictions = model(query)
flag = torch.logical_and(values >= min, values <= max)
pos = positions[flag]
pos_list=pos.tolist()
t4=time.time()
pos_list=[int(x)for x in pos_list]
# 打印范围内的位置
movies=[]
for i in pos_list:
    key=array[i][0][1]
    ans=r.hgetall(key)
    movies.append(ans)
sorted_movies = sorted(movies, key=lambda x: (float(x['Vote_Average']), -float(x['Vote_Count'])), reverse=False)
for movie in sorted_movies:
    print(movie)
print("redis读数据时间:", (t2-t1))
print("学习索引索引构建时间:",time2-time1)
print("学习索引查询时间:",t4-t3)