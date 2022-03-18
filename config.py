WORKER_IP ="127.0.0.1"#workflow机器的ip 

key1 = "key1"
value1 = "1" * 1000000

key2 ="key2"

value2 = "2" * 1500000

key3 = "key3"
value3 = "3" * 1800000

keys =[] 
key = "key"

values  = [] 
value = "1".encode() #转为bytes 

maxSize = 10000000
for i in range(10000):
    tkey = key + str(i)
    tvalue = value *  maxSize
    keys.append(tkey)
    values.append(tvalue).