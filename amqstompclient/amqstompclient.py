import stomp
import logging
import json
import sys
import datetime

logger = logging.getLogger(__name__)
amqclientversion = "1.1.0"


class AMQClient():

    def __init__(self, server, module, subscription, callback=None):
        logger.debug("#=-" * 20)
        logger.debug("#=- Starting AMQ Connection" + amqclientversion)
        logger.debug("#=-" * 20)
        logger.debug("#=- Module  :" + module["name"])
        logger.debug("#=- IP      :" + server["ip"])
        logger.debug("#=- Port    :" + str(server["port"]))
        logger.debug("#=- Login   :" + server["login"])
        logger.debug("#=- Password:" + ("*" * len(server["password"])))
        logger.debug("#=- Subscription:" + str(subscription))
        logger.debug("#=-" * 20)

        self.starttime = datetime.datetime.now()

        self.conn = None
        self.sent = {}
        self.subscription = subscription
        self.callback = callback
        self.server = server
        self.module = module
        self.heartbeaterrors = 0
        self.connections = 0

        self.create_connection()

    def disconnect(self):
        logger.info("#=- Disconnecting...")
        self.conn.disconnect()

    def create_connection(self):
        logger.info("#=- Creating connection.")
        heartbeats = (10000, 20000)

        if "heartbeats" in self.server:
            heartbeats = self.server["heartbeats"]

        self.conn = stomp.Connection(
            [(self.server["ip"], self.server["port"])], heartbeats=heartbeats, heart_beat_receive_scale=1.1)

        self.listener = AMQListener(self, self.callback)
        self.conn.set_listener('simplelistener', self.listener)
        logger.debug("#=- Starting connection...")
        self.conn.start()
        logger.debug("#=- Connection started.")
        self.conn.connect(self.server["login"],
                          self.server["password"],
                          wait=True,
                          headers={"client-id": self.module["name"]})
        logger.debug("#=- Login passed.")

        self.connections+=1
        curid = 1

        if(len(self.subscription) > 0):
            for sub in self.subscription:
                if len(sub) > 0:
                    logger.debug("#=- Subscribing to:" + sub)
                    self.conn.subscribe(destination=sub, id=curid, ack='client', headers={
                                        "activemq.prefetchSize": 1})
                    curid += 1

    def send_life_sign(self):
        if(self.listener != None):
            logger.debug("#=- Send Module Life Sign.")
            if("lifesign" in self.module):
                self.send_message(self.module["lifesign"], json.dumps({"error": "OK", "type": "lifesign", "module": self.module["name"], "version": self.module["version"],
                                                                       "alive": 1, "errors": self.listener.globalerrors,
                                                                       "internalerrors": self.listener.errors,
                                                                       "heartbeaterrors": self.heartbeaterrors,
                                                                       "eventtype": "lifesign", "messages": self.listener.globalmessages,
                                                                       "received": self.listener.received, "sent": self.sent,
                                                                       "amqclientversion": amqclientversion,
                                                                       "starttimets": self.starttime.timestamp(),
                                                                       "starttime": str(self.starttime),
                                                                       "connections":self.connections
                                                                       }))
            else:
                logger.error(
                    "Unable to send life sign. Target queue not defined in module parameters.")
        else:
            logger.error("#=- Unable to send life sign. No listener defined.")

    def send_message(self, destination, message, headers=None):
        logger.debug("#=- Send Message to " + destination +
                     ". LEN=" + str(len(message)))
        if destination not in self.sent:
            self.sent[destination] = 1
        else:
            self.sent[destination] += 1

        try:
            self.conn.send(
                body=message, destination=destination, headers=headers)
        except stomp.exception.NotConnectedException:
            logger.error("#=- Not connected exception raised. Reconnecting.")
            try:
                self.disconnect()
            except Exception:
                logger.error("#=- Unable to disconnect.")
            self.create_connection()

    def heartbeat_timeout(self):
        self.heartbeaterrors += 1
        for n in range(1, 31):
            try:
                logger.debug("#=- Reconnecting: Attempt %d" % n)
                self.create_connection()

                break
            except exception.ConnectFailedException:
                logger.error("#=- Reconnect attempt failed: %s" % e)
                time.sleep(2)


##################################################################################
# AMQ Listener
##################################################################################

class AMQListener(stomp.ConnectionListener):

    def __init__(self, amqconn,  callback):
        self.internal_conn = amqconn
        self.callback = callback
        self.globalerrors = 0
        self.errors = 0
        self.globalmessages = 0
        self.received = {}

    def on_error(self, headers, body):
        logger.error('#=- Received an error "%s"' % body)
        self.errors += 1

    def on_heartbeat_timeout(self):
        logger.error("#=- HEART BEAT TIMEOUT ERROR")
        self.internal_conn.heartbeat_timeout()

    def on_message(self, headers, message):
        destination = "NA"
        if("destination" in headers):
            destination = headers["destination"]
        logger.debug("#=->>>> Message received (" + destination +
                     ") PAYLOAD=" + str(len(message)))

        if destination not in self.received:
            self.received[destination] = 1
        else:
            self.received[destination] += 1

        self.globalmessages += 1

        try:
            if self.callback != None:
                self.callback(destination, message, headers)
            else:
                logger.warning("#=- No call back defined")

        except Exception:
            self.globalerrors += 1

            logger.error("ERROR:", exc_info=True)
            err = sys.exc_info()
            errstr = str(err[0]) + str(err[1]) + str(err[2])
            logger.error("ERROR:" + errstr)

        self.internal_conn.conn.ack(
            headers["message-id"], headers["subscription"])
        logger.debug("#=-<<<< Message handled")
