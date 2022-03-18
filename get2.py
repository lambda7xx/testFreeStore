from  FreeStoreClient import FreeStore 

from config import keys,values ,WORKER_IP 

store = FreeStore(WORKER_IP) 
j = 5000 
while j < 100000:
    key = keys[j]
    get_value = store.getStr(key)

store.Delete() #删除请求
store.Close()