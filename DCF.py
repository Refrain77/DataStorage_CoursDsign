import hashlib
import redis
import pandas as pd
import random
import json
hash_len=5#哈希表的长度
hash_range=2000#哈希范围
bucket1={}#用字典模拟哈希桶
bucket2={}
bucket3={}
bucket4={}
bucket_list=[bucket1,bucket2,bucket3,bucket4]
ptr_bucket=0
threshold=5#最大踢出次数
def hash1(data):
    md5=hashlib.md5()
    md5.update(str(data).encode())
    md5 = md5.hexdigest()
    md5 = int(md5, 16)
    md5=md5%10000
    return md5
def fingerprint(data):
    SHA_1=hashlib.sha1()
    SHA_1.update(data.encode())
    SHA_1=SHA_1.hexdigest()
    SHA_1=int(SHA_1,16)
    SHA_1%=1000
    SHA_1 = int(str(SHA_1).ljust(4, '0'))
    return SHA_1
def update(column, new):
    new_list = column + new
    unique_list = []
    seen_words = set()
    for item in new_list:
        if item is not None:
            # 尝试解析JSON
            try:
                item_list = json.loads(item)
            except json.JSONDecodeError:
                item_list = [item]
            # 分割并去重
            for inner_item in item_list:
                item_words = inner_item.split(', ')
                for word in item_words:
                    if word not in seen_words:
                        unique_list.append(word)
                        seen_words.add(word)
    return unique_list
def find(hash1, hash2, fingerprint):
    # 检查是否存在包含hash1或hash2的bucket
    bucket_found = False
    for bucket in bucket_list:
        if hash1 in bucket or hash2 in bucket:
            bucket_found = True
            break
    # 如果没有找到包含hash1或hash2的bucket，则返回False
    if not bucket_found:
        return False
    # 检查是否存在包含fingerprint的bucket
    for bucket in bucket_list:
        if fingerprint in bucket.get(hash1, []) or fingerprint in bucket.get(hash2, []):
            return True  # 如果找到fingerprint，返回True
    return False
def insert(key,value,bucket,num=hash_len):
    if key not in bucket:
        bucket[key]=[value]
        return True
    else:
        value_list=bucket[key]
        if len(value_list)+1<=num:
            if value not in value_list:
                value_list.append(value)
                return True
        else:#空间已满
            return False
def DCF(hash_1, hash_2, fingerprint, ptr):
    t_num=0
    time=0#第一次
    bucket=bucket_list[ptr]
    result =insert(hash_1,fingerprint,bucket)
    while not result:
        if time==0:
            result=insert(hash_2,fingerprint,bucket)
            if not result:
                r=random.randint(0, len(bucket[hash_2]) - 1)
                t=bucket[hash_2].pop(r)#产生一个随机数,踢出后的数为t
                bucket[hash_2].append(fingerprint)
                t_num+=1
                hash_2=t^hash_2#通过t和此时的h2解出t的hash值
                result=insert(hash_2,t,bucket)
                time+=1
                t_num+=1#踢出次数
        else:
            r = random.randint(0, len(bucket[hash_2]) - 1)
            t = bucket[hash_2].pop(r) # 产生一个随机数,踢出后的数为t
            bucket[hash_2].append(fingerprint)
            t_num += 1
            hash_2 = t ^ hash_2  # 通过t和此时的h2解出t的hash值
            if t_num==threshold or len(bucket)>hash_range:
                ptr+=1
                bucket=bucket_list[ptr]
                result = insert(hash_2,t,bucket)
            else:
                result=insert(hash_2,t,bucket)
    return ptr
if __name__ == '__main__':
    red = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    pool = redis.ConnectionPool()
    r_pool = redis.Redis(connection_pool=pool)
    red.flushdb()
    file_path = "E:\\电影评分数据.xlsx"
    data1 = pd.read_excel(file_path, header=0, sheet_name=0)
    data1 = data1.rename(columns={"genel": "genre", "Vote Average": "Vote_Average", "Vote Count": "Vote_Count"})
    data2 = pd.read_excel(file_path, header=0, sheet_name=1)
    data2 = data2.rename(columns={"Vote Average": "Vote_Average", "Vote Count": "Vote_Count"})
    df = pd.concat([data1, data2], ignore_index=True)
    df["Title"]=df["Title"].astype(str)
    print("开始过滤导入redis")
    for i in range(0,len(df)):
        t_num=0
        title=df.Title.iloc[i]
        new_genre=df.genre.iloc[i]
        f=fingerprint(title)
        h1=hash1(title)
        h2=h1^hash1(f)
        result=find(h1,h2,f)
        if result is True:#更新genre
            genre= red.hget(f'{title}', 'genre')
            genre_list=[]
            genre_list.append(genre)
            print(f"original_genre:{genre_list}")
            new_list=[]
            new_list.append(new_genre)
            new=update(genre_list,new_list)#更新数据
            print(f"new_genre:{new}")
            genre_json = json.dumps(new)
            red.hset(f'{title}', 'genre', str(genre_json))
        else:
            print(f"{title}不在过滤器中，加入过滤器")
            ptr=DCF(h1,h2,f,ptr_bucket)
            ptr_bucket=ptr
            key = f'{title}'
            red.hset(key,'Overview',str(df.Overview.iloc[i]))
            red.hset(key,'genre', str(new_genre))
            red.hset(key,'Vote_Average',str(df.Vote_Average.iloc[i]))
            red.hset(key,'Vote_Count',str(df.Vote_Count.iloc[i]))
    print("导入成功！")