from  FreeStoreClient import FreeStore 

from config import keys,values ,WORKER_IP 

store = FreeStore(WORKER_IP) 
for i in range(10):
    key = keys[i]
    get_value = store.getStr(key)

store.Delete() #删除请求
store.Close()
