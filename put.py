from  FreeStoreClient import FreeStore 

from config import keys,values ,WORKER_IP 


store = FreeStore(WORKER_IP) 
for i in range(10000):
    key = keys[i]
    value = values[i]
    store.PutStr(key,value)

store.Close()