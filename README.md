# amqstompclient

A stomp client in python for ActiveMQ using the stomp.py library. The client reconnects automatically when ActiveMQ misses heartbeats.

Example:
```python
from amqstompclient import amqstompclient
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

server={"ip":"127.0.0.1","port":"61613","login":"admin","password":"ictcs-s4fab"}

conn=amqstompclient.AMQClient(server
      , {"name":"TEST","version":"1.0.0","lifesign":"/topic/COUCOU"}
      ,["/queue/QTEST1","/queue/QTEST2","/topic/TTEST1","/topic/TTEST2"])        

while True:
    time.sleep(5)
    conn.send_life_sign()
```
