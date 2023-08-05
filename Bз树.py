import json
import time
import redis
class Node:
    def __init__(self, t):
        self.keys = []
        self.children = []
        self.buffer=[]
        self.is_leaf = True
        self.t = t
class Btree2:
    def __init__(self, t, buffer_size):
        self.root = Node(t)
        self.t = t
        self.buffer_size = buffer_size

    def insert_key_value(self, key, value):
        root = self.root
        root.buffer.append((key, value))  # 将当前的(key, value)添加到缓冲区
        if len(root.buffer) >= self.buffer_size:
            # 批量插入
            for k, v in root.buffer:
                if len(root.keys) == (2 * self.t) - 1:
                    temp = Node(self.t)
                    temp.is_leaf = False
                    self.root = temp
                    temp.children.insert(0, root)
                    self.split_child(temp, 0)
                    self._insert(temp, k, v)
                else:
                    self._insert(root, k, v)
            root.buffer.clear()

    def _insert(self, x, key, value):
        # 获取x节点键的数量
        i = len(x.keys) - 1
        # print(x.is_leaf)
        if x.is_leaf:
            x.keys.append((None, None))
            while i >= 0 and (key[0] < x.keys[i][0][0] or (key[0] == x.keys[i][0][0] and key[1] > x.keys[i][0][1])):
                x.keys[i + 1] = x.keys[i]
                i -= 1
            x.keys[i + 1] = (key, value)
        else:
            # print("else block")
            while i >= 0 and (key[0] < x.keys[i][0][0] or (key[0] == x.keys[i][0][0] and key[1] > x.keys[i][0][1])):
                i -= 1
            i += 1
            if len(x.children[i].keys) == (2 * self.t) - 1:
                self.split_child(x, i)
                # 决定应该插入哪个子树
                if key[0] > x.keys[i][0][0] or (key[0] == x.keys[i][0][0] and key[1] < x.keys[i][0][1]):
                    i += 1
            self._insert(x.children[i], key, value)
    def split_child(self, x, i):
        t = self.t
        y = x.children[i]
        z = Node(t)
        z.is_leaf=y.is_leaf
        x.children.insert(i + 1, z)
        x.keys.insert(i, y.keys[t - 1])
        z.keys = y.keys[t: (2 * t) - 1]
        y.keys = y.keys[0: t - 1]
        if not y.is_leaf:
            z.children = y.children[t: 2 * t]
            y.children = y.children[0: t]
    def range_query(self, x, l, h):
        result = []
        i = 0
        while i < len(x.keys) and x.keys[i][0][0] < l:
            i += 1
        while i < len(x.keys) and x.keys[i][0][0] <= h:
            if not x.is_leaf:
                result.extend(self.range_query(x.children[i], l, h))
            result.append(x.keys[i])
            i += 1
        if not x.is_leaf:
            result.extend(self.range_query(x.children[i], l, h))
        for key, value in x.buffer:
            if l <= key[0] <= h:
                result.append((key, value))
        return result

B = Btree2(11, buffer_size=1000)
r = redis.Redis(host='localhost', port=6379, db=0)
V=[]
time0=time.time()
for key in r.keys():
    v1=key.decode()
    v2=r.hget(key,"Vote_Average")
    v3=r.hget(key,"Vote_Count")
    if v2 and v3 is not None:
        v2=v2.decode()
        v2=float(v2)
        v3=int(v3)
        v_tuple=(v2,v3)
        value=[v_tuple,v1]
        V.append(value)
# 向B树插入数据
time1=time.time()
for value in V:
    B.insert_key_value(value[0],value[1])
time2=time.time()
# 执行范围查询
min=float(input("请输入最小值:"))
max=float(input("请输入最大值:"))
time3=time.time()
results = B.range_query(B.root,min, max)
# print(results)
time4=time.time()
print(len(results))
for res in results:
    key=res[1]
    ans=r.hgetall(key)
    print(ans)
time5=time.time()
print("redis读数据时间:",(time1-time0)+(time5-time3))
print("Bз树索引构建时间:",time2-time1)
print("Bз树查询时间:",time4-time3)