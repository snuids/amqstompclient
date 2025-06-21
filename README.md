# amqstompclient

A stomp client in python3 for ActiveMQ using the stomp.py library. The client reconnects automatically when ActiveMQ misses heartbeats.
As of version 2.0 it is linked against stomp.py 8

## Example 1:
```python
from amqstompclient import amqstompclient
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

server={"ip":"127.0.0.1","port":"61613","login":"admin","password":"*****","heartbeats":(30000,30000)}

conn=amqstompclient.AMQClient(server
      , {"name":"TEST","version":"1.0.0","lifesign":"/topic/COUCOU"}
      ,["/queue/QTEST1","/queue/QTEST2","/topic/TTEST1","/topic/TTEST2"])        

while True:
    time.sleep(5)
    conn.send_life_sign()
```

## Example 2:
Using a message received callback.
```python
from amqstompclient import amqstompclient
import logging
import time

def callback(destination, message,headers):
    logger.info("Received:"+message)
    conn.send_message("/topic/TTEST2","FROMCALLBACK")


logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

server={"ip":"127.0.0.1","port":"61613","login":"admin","password":"*****","heartbeats":(30000,30000)}

conn=amqstompclient.AMQClient(server
        , {"name":"TEST","version":"1.0.0","lifesign":"/topic/HELLO"}
        ,["/queue/QTEST1","/queue/QTEST2","/topic/HELLO"]
        ,callback=callback)        

while True:
    time.sleep(5)
    conn.send_life_sign()
```

![AMQC](https://github.com/snuids/amqstompclient/raw/master/images/AMQC1.jpg)


## Early Ack

It is possible to set earlyack to True in order to ack the message before calling the callback. This way, the message will not be rolled back if the callback fails.

{"ip":activemq_address,"port":activemq_port,"login":activemq_user,
            "password":activemq_password,"heartbeats":(30000,30000),"earlyack":True}
