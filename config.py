WORKER_IP ="172.16.187.13"#workflow机器的ip 

key1 = "key1"
value1 = "1" * 10000

key2 ="key2"

value2 = "2" * 15000

key3 = "key3"
value3 = "3" * 18000

keys =[] 
key = "key"

values  = [] 
value = "1".encode() #转为bytes 

maxSize = 100000
for i in range(20):
    tkey = key + str(i)
    tvalue = value *  maxSize
    keys.append(tkey)
    values.append(tvalue)
