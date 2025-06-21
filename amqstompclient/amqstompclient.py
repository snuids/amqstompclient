import stomp
import logging
import json
import sys
import datetime
import time
import os

import stomp.utils

logger = logging.getLogger(__name__)
amqclientversion = "2.0.3"


##################################################################################
# AMQ Listener
##################################################################################

class AMQListener(stomp.ConnectionListener):
    """
    AMQListener is a listener class for handling STOMP protocol events in a message queue client.

    Args:
        amqconn: The AMQ connection object, used for acknowledging messages and handling errors.
        callback: A callable to process received messages. It should accept (destination, message, headers).

    Attributes:
        internal_conn: Reference to the AMQ connection object.
        callback: The message processing callback function.
        globalerrors: Counter for global errors encountered.
        errors: Counter for errors received via the on_error handler.
        globalmessages: Counter for total messages received.
        received: Dictionary tracking the number of messages received per destination.

    Methods:
        on_error(frame):
            Handles error frames received from the STOMP server. Logs the error, increments error counters, and notifies the connection.
        on_heartbeat_timeout():
            Handles heartbeat timeout events. Logs a warning and notifies the connection.
        on_message(frame):
            Handles incoming messages. Optionally acknowledges early, logs and tracks message statistics, invokes the callback, handles exceptions, and acknowledges the message if not early ack.
    """

    def __init__(self, amqconn,  callback):
        self.internal_conn = amqconn
        self.callback = callback
        self.globalerrors = 0
        self.errors = 0
        self.globalmessages = 0
        self.received = {}

    def on_error(self,  frame:stomp.utils.Frame):
        logger.error('#=- Received an error "%s"' % frame)
        self.errors += 1
        self.internal_conn.general_error()

    def on_heartbeat_timeout(self):
        logger.warning("#=- HEART BEAT TIMEOUT ERROR")
        self.internal_conn.heartbeat_timeout()

    def on_message(self,  frame:stomp.utils.Frame):
        headers = frame.headers
        message = frame.body
        if self.internal_conn.earlyack:
            logger.debug("Early ack")
            self.internal_conn.conn.ack(
                headers["message-id"], headers["subscription"])

        destination = "NA"
        if("destination" in headers):
            destination = headers["destination"]
        logger.debug("#=->>>> Message received (%s) PAYLOAD=%d", destination, len(message))

        if destination not in self.received:
            self.received[destination] = 1
        else:
            self.received[destination] += 1

        self.globalmessages += 1

        try:
            if self.callback is not None:
                self.callback(destination, message, headers)
            else:
                logger.warning("#=- No call back defined")

        except Exception as e:
            self.globalerrors += 1

            logger.error(f"ERROR:{e}", exc_info=True)
            err = sys.exc_info()
            errstr = str(err[0]) + str(err[1]) + str(err[2])
            logger.error(f"ERROR:{errstr}" )

        if not self.internal_conn.earlyack:
            self.internal_conn.conn.ack(
                headers["message-id"], headers["subscription"])
        logger.debug("#=-<<<< Message handled")


class AMQClient():
    """
    AMQClient is a client for connecting to an AMQ (ActiveMQ) server using the STOMP protocol.
    This class manages the connection lifecycle, subscriptions, message sending, and heartbeat monitoring.
    It supports automatic reconnection, error handling, and sending periodic life sign messages.
    Args:
        server (dict): Server connection parameters (ip, port, login, password, etc.).
        module (dict): Module information (name, version, lifesign queue, etc.).
        subscription (list): List of subscription destinations (queues/topics).
        callback (callable, optional): Callback function for message handling.
        heart_beat_receive_scale (float, optional): Heartbeat receive scale factor. Default is 2.0.
        listener_class (type, optional): Listener class to handle incoming messages. Default is AMQListener.
    Attributes:
        starttime (datetime): Timestamp when the client was started.
        heart_beat_receive_scale (float): Heartbeat receive scale factor.
        listener_class (type): Listener class used for message handling.
        conn (stomp.Connection): STOMP connection object.
        sent (dict): Counter of sent messages per destination.
        subscription (list): List of subscription destinations.
        callback (callable): Callback function for message handling.
        server (dict): Server connection parameters.
        module (dict): Module information.
        heartbeaterrors (int): Number of heartbeat errors encountered.
        connections (int): Number of connection attempts.
        earlyack (bool): Whether early acknowledgment is enabled.
        listener (AMQListener): Listener instance for handling messages.
    Methods:
        disconnect(): Disconnects from the AMQ server.
        create_connection(): Establishes a new connection and subscribes to destinations.
        send_life_sign(variables=None): Sends a life sign message to the configured queue.
        generate_life_sign(): Generates a dictionary with life sign information.
        send_message(destination, message, headers=None): Sends a message to a destination.
        heartbeat_timeout(): Handles heartbeat timeout events and triggers reconnection.
        general_error(): Handles unrecoverable errors and exits the process.
        listener_disconnect(): Handles listener disconnect events and triggers reconnection.
        reconnect_and_listen(): Attempts to reconnect to the server with retries.
    """

    def __init__(self, server, module, subscription, callback=None,heart_beat_receive_scale=2.0,
                 listener_class=AMQListener):
        
        logger.debug("#=-" * 20)
        logger.debug("#=- Starting AMQ Connection%s", amqclientversion)
        logger.debug("#=-" * 20)
        logger.debug("#=- Module       :%s", module["name"])
        logger.debug("#=- IP           :%s", server["ip"])
        logger.debug("#=- Port         :%s", server["port"])
        logger.debug("#=- Login        :%s", server["login"])
        logger.debug("#=- Password     :%s", "*" * len(server["password"]))
        logger.debug("#=- Subscription :%s", subscription)
        logger.debug("#=- Beat         :%s", heart_beat_receive_scale)
        logger.debug("#=- Listener     :%s", listener_class)
        
        self.starttime = datetime.datetime.now()
        self.heart_beat_receive_scale=heart_beat_receive_scale
        self.listener_class = listener_class

        self.conn = None
        self.sent = {}
        self.subscription = subscription
        self.callback = callback
        self.server = server
        self.module = module
        self.heartbeaterrors = 0
        self.connections = 0
        self.earlyack=False

        if "earlyack" in server:
            logger.info("Early ack set to true.")
            self.earlyack=server["earlyack"]

        logger.debug("#=- Subscription :%s", subscription)
        logger.debug("#=- Early Ack    :%s", self.earlyack)
        logger.debug("#=-" * 20)
        
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
            [(self.server["ip"], self.server["port"])], heartbeats=heartbeats,heart_beat_receive_scale=self.heart_beat_receive_scale)

        self.listener = self.listener_class(self, self.callback)
        self.conn.set_listener('simplelistener', self.listener)
        logger.debug("#=- Starting connection...")
#        self.conn.start()
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

    def send_life_sign(self,variables=None):
        if(self.listener != None):
            logger.debug("#=- Send Module Life Sign.")
            if("lifesign" in self.module):
                lifesignstruct = self.generate_life_sign()

                if variables !=None:
                    for key in variables:
                        lifesignstruct[key]=variables[key]

                try:
                    self.send_message(self,self.module["lifesign"], json.dumps(lifesignstruct))
                except:
                    self.send_message(self.module["lifesign"], json.dumps(lifesignstruct))
            else:
                logger.error(
                    "Unable to send life sign. Target queue not defined in module parameters.")
        else:
            logger.error("#=- Unable to send life sign. No listener defined.")

    def generate_life_sign(self):
        return {
            "error": "OK",
            "type": "lifesign",
            "eventtype": "lifesign",
            "module": self.module["name"],
            "version": self.module["version"],
            "alive": 1,
            "errors": self.listener.globalerrors,
            "internalerrors": self.listener.errors,
            "heartbeaterrors": self.heartbeaterrors,
            "messages": self.listener.globalmessages,
            "received": self.listener.received,
            "sent": self.sent,
            "amqclientversion": amqclientversion,
            "starttimets": self.starttime.timestamp(),
            "starttime": str(self.starttime),
            "connections": self.connections
        }

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
        except Exception:            
            
            logger.error("#=- Error raised while sending. ")
            logger.info("Sleeping 5 seconds and disconnects.")
            time.sleep(5)
            err = sys.exc_info()
            errstr = str(err[0]) + str(err[1]) + str(err[2])
            logger.error("ERROR:" + errstr)
            try:
                self.disconnect()
            except Exception:
                logger.error("#=- Unable to disconnect.")
            logger.info("Sleeping 5 seconds and reconnects.")
            time.sleep(5)
            self.create_connection()

    def heartbeat_timeout(self):
        self.heartbeaterrors += 1
        self.reconnect_and_listen()

    def general_error(self):        
        logger.error("#=- General Error. Exiting")
        time.sleep(5)
        os._exit(1)

    def listener_disconnect(self):
        self.reconnect_and_listen()

    def reconnect_and_listen(self):
        for n in range(1, 31):
            try:
                logger.debug("#=- Reconnecting: Attempt %d" % n)
                time.sleep(5)
                self.create_connection()

                break
            except Exception as e:
                logger.error("#=- Reconnect attempt failed: %s" % e)
