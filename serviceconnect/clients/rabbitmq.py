from ..utils import mergedicts
import pika
import logging
import json
import uuid
import datetime

LOGGER = logging.getLogger(__name__)


class RabbitClient:

    def __init__(self, config, consumeMessageFunction):
        self.config = config["amqp"]
        self.host = self.config["host"]
        self.handlers = config["handlers"]
        self.consumeMessageFunction = consumeMessageFunction
        self.connection = None
        self.channel = None
        self.connection = None
        self.channel = None
        self.closing = False
        self.consumer_tag = None
        self.connectedCallback = None

    def connect(self, connectedCallback=None):
        LOGGER.info('Connecting')
        self.connectedCallback = connectedCallback
        self.connection = pika.SelectConnection(
            pika.URLParameters(self.host),
            self.on_connection_open,
            stop_ioloop_on_close=False)
        return self.connection

    def send(self, endpoint, typeName, message, headers={}):
        body = json.dumps(message)
        newHeaders = self.__getHeaders(typeName, headers, endpoint, "Send")
        properties = pika.BasicProperties(headers=newHeaders,
                                          message_id=newHeaders["MessageId"])

        if "Priority" in newHeaders:
            properties.priority = newHeaders["Priority"]

        self.channel.basic_publish(exchange='',
                                   routing_key=endpoint,
                                   body=body,
                                   properties=properties)

    def publish(self, typeName, message, headers={}):
        body = json.dumps(message)
        newHeaders = self.__getHeaders(typeName, headers, "", "Publish")
        properties = pika.BasicProperties(headers=newHeaders,
                                          message_id=newHeaders["MessageId"])

        if "Priority" in newHeaders:
            properties.priority = newHeaders["Priority"]

        exchange = typeName.replace(".", "")

        def publishMessage(frame):
            self.channel.basic_publish(exchange=exchange,
                                       routing_key='',
                                       body=body,
                                       properties=properties)

        self.channel.exchange_declare(exchange=exchange,
                                      exchange_type="fanout",
                                      durable=True,
                                      callback=publishMessage)

    def __getHeaders(self, typeName, headers, queue, messageType):
        newHeaders = mergedicts({
            "DestinationAddress": queue,
            "MessageType": messageType,
            "SourceAddress": self.config["queue"]["name"],
            "TimeSent": datetime.datetime.now().isoformat(),
            "TypeName": typeName,
            "FullTypeName": typeName,
            "ConsumerType": "RabbitMQ",
            "Language": "Python3"
        }, headers)
        if "MessageId" not in headers:
            newHeaders["MessageId"] = str(uuid.uuid4())
        return newHeaders

    def on_connection_open(self, unused_connection):
        LOGGER.info('Connection opened')
        self.connection.add_on_close_callback(self.on_connection_closed)
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_connection_closed(self, connection, reply_code, reply_text):
        if self.closing:
            self.connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 second: (%s) %s',
                           reply_code, reply_text)
            self.connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        self.connection.ioloop.stop()
        if not self.closing:
            self.connection = self.connect()
            self.connection.ioloop.start()

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        channel.basic_qos(prefetch_count=self.config["prefetch"])
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)

        # Declare queue
        arguments = None
        config = self.config
        if config["queue"]["maxPriority"] is not None:
            arguments = {"x-max-priority": config["queue"]["maxPriority"]}
        channel.queue_declare(queue=config["queue"]["name"],
                              durable=config["queue"]["durable"],
                              exclusive=config["queue"]["exclusive"],
                              auto_delete=config["queue"]["autoDelete"],
                              arguments=arguments,
                              callback=self.setup_handlers)

    def setup_handlers(self, frame):
        LOGGER.info("Declared queue")
        channel = self.channel
        config = self.config
        self.count = len(self.handlers)

        if self.count == 0:
            self.declare_deadletter_queue()
            return

        def queue_bound(frame):
            self.count = self.count - 1
            if self.count == 0:
                LOGGER.info("Handlers setup")
                self.declare_deadletter_queue()

        def exchange_declared(frame):
            self.channel.queue_bind(
                queue=config["queue"]["name"],
                exchange=exchange,
                callback=queue_bound)

        # declare message type exchanges
        for key in self.handlers:
            exchange = key.replace(".", "")
            channel.exchange_declare(
                exchange=exchange,
                exchange_type="fanout",
                durable=True,
                callback=exchange_declared)

    def declare_deadletter_queue(self):
        channel = self.channel
        config = self.config
        retryQueue = config["queue"]["name"] + ".Retries"

        def queue_created(frame):
            LOGGER.info('Deadletter queue declared')
            # Bind queue to dead letter eachange
            channel.queue_bind(
              queue=config["queue"]["name"],
              exchange=dlExchange,
              routing_key=retryQueue,
              callback=self.declare_error_queue)

        def exchange_declared(frame):
            LOGGER.info("Deadletter exchange declared")
            # retry queue
            retryArguments = {
                "x-dead-letter-exchange": dlExchange,
                "x-message-ttl": config["retryDelay"]
            }
            channel.queue_declare(
                queue=retryQueue,
                durable=config["queue"]["durable"],
                arguments=retryArguments,
                callback=queue_created)

        # dead letter exchange
        dlExchange = config["queue"]["name"] + ".Retries.DeadLetter"
        channel.exchange_declare(exchange=dlExchange,
                                 exchange_type="direct",
                                 durable=True,
                                 callback=exchange_declared)

    def declare_error_queue(self, frame):
        channel = self.channel
        config = self.config

        def exchange_declared(frame):
            # Create error queue
            channel.queue_declare(
                queue=config["errorQueue"],
                durable=True,
                callback=self.declare_audit_queue)

        # Configure error exchange
        channel.exchange_declare(exchange=config["errorQueue"],
                                 exchange_type="direct",
                                 durable=False,
                                 callback=exchange_declared)

    def declare_audit_queue(self, frame):
        channel = self.channel
        config = self.config

        if config["auditEnabled"]:

            def exchange_declared(frame):
                channel.queue_declare(
                    queue=config["auditQueue"],
                    durable=True,
                    callback=self.startConsuming)

            # Configure audit exchange
            channel.exchange_declare(exchange=config["auditQueue"],
                                     exchange_type="direct",
                                     durable=False,
                                     callback=exchange_declared)
        else:
            self.startConsuming()

    def startConsuming(self, frame=None):
        channel = self.channel
        config = self.config

        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self.consumer_tag = channel.basic_consume(self.on_message,
                                                  config["queue"]["name"])

        LOGGER.info('Started consuming')
        if self.connectedCallback is not None:
            self.connectedCallback()

    def on_channel_closed(self, channel, reply_code, reply_text):
        LOGGER.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self.connection.close()

    def on_consumer_cancelled(self, method_frame):
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self.channel:
            self.channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        result = None
        try:
            headers = properties.headers
            message = json.loads(body)
            headers["TimeReceived"] = datetime.datetime.now().isoformat()
            typeName = headers["TypeName"]
            self.consumeMessageFunction(message, headers, typeName)
            headers["TimeProcessed"] = datetime.datetime.now().isoformat()
        except Exception as e:
            print(e)
            result = {"exception": str(e), "success": False}

        try:
            if result is not None:
                retryCount = 0
                if "RetryCount" in headers:
                    retryCount = headers["RetryCount"]

                if retryCount < self.config["maxRetries"]:
                    retryCount = retryCount + 1
                    headers["RetryCount"] = retryCount
                    retryQueue = self.config["queue"]["name"] + ".Retries"
                    properties = pika.BasicProperties(
                        headers=headers,
                        message_id=headers["MessageId"])
                    self.channel.basic_publish(exchange='',
                                               routing_key=retryQueue,
                                               body=body,
                                               properties=properties)
                else:
                    headers["Exception"] = result["exception"]
                    properties = pika.BasicProperties(
                        headers=headers,
                        message_id=headers["MessageId"])
                    errorQueue = self.config["errorQueue"]
                    self.channel.basic_publish(exchange='',
                                               routing_key=errorQueue,
                                               body=body,
                                               properties=properties)
            else:
                if self.config["auditEnabled"]:
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=self.config["auditQueue"],
                        body=body,
                        properties=properties)

        except Exception as e:
            LOGGER.error('Error processig message %s', str(e))
        finally:
            self.channel.basic_ack(basic_deliver.delivery_tag)

    def stop_consuming(self):
        if self.channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self.channel.basic_cancel(self.on_cancelok, self.consumer_tag)

    def on_cancelok(self, unused_frame):
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.channel.close()

    def run(self, connectedCallback=None):
        self.connect(connectedCallback)
        self.connection.ioloop.start()

    def stop(self):
        LOGGER.info('Stopping')
        self.closing = True
        self.stop_consuming()
        self.connection.ioloop.start()
        LOGGER.info('Stopped')

    def close_connection(self):
        LOGGER.info('Closing connection')
        self.connection.close()
