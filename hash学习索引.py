import redis
import hashlib
import torch
from torch import nn
import numpy as np
r=redis.Redis(host="localhost",port=6379,db=0,decode_responses=True)
def hash(data):
    md5 = hashlib.md5()
    md5.update(str(data).encode())
    md5 = md5.hexdigest()
    md5 = int(md5, 16)
    md5 = md5 % 1000
    return md5
hash_table = {i: [] for i in range(1000)}
arr=[]
for k in r.keys():
    vote_average = r.hget(k, "Vote_Average")
    if vote_average is not None:
        vote_average=float(vote_average)
        hash_k = hash(vote_average)
        hash_table[hash_k].append((vote_average,k))
        arr.append([(vote_average,hash_k),k])
data = np.array([(a[0][0], a[0][1]) for a in arr])
keys = torch.tensor(data[:, 0], dtype=torch.float32).unsqueeze(dim=1)
hash_pos = torch.tensor(data[:, 1], dtype=torch.float32).unsqueeze(dim=1)
model=nn.Sequential(
    nn.Linear(1,10),
    nn.ReLU(),
    nn.Linear(10,1)
)
criterion=nn.MSELoss()
opt=torch.optim.Adam(model.parameters())
num_epochs=100
for epoch in range(num_epochs):
    outputs = model(keys)
    loss = criterion(outputs, hash_pos)
    opt.zero_grad()
    loss.backward()
    opt.step()
min_val = 5.5
max_val = 6.7
query = torch.tensor([[min_val], [max_val]], dtype=torch.float32)
predictions = model(query)
flag = torch.logical_and(keys>= min_val, keys <= max_val)
pos = hash_pos[flag]
pos_list=pos.tolist()
pos_list=[int(x)for x in pos_list]
for p in pos_list:
    print(arr[p])

