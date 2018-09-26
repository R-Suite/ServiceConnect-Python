from serviceconnect.bus import Bus
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)

bus = None


def handler(message, headers, typeName):
    LOGGER.info("Received sent message")
    print(typeName)
    print(message)
    bus.publish("PythonTestType2", {"Data": 123})


def handler2(message, headers, typeName):
    LOGGER.info("Received published message")
    print(typeName)
    print(message)


def afterFilter(message, headers, typeName):
    LOGGER.info("After filter")
    return True


def beforeFilter(message, headers, typeName):
    LOGGER.info("Before filter")
    return True


def outgoingFilter(message, headers, typeName):
    LOGGER.info("Outgoing filter")
    return True


config = {
    "amqp": {
        "queue": {"name": "python.test"},
    },
    "handlers": {
        "PythonTestType": handler,
        "PythonTestType2": handler2
    },
    "filters": {
      "after": [afterFilter],
      "before": [beforeFilter],
      "outgoing": [outgoingFilter]
    },
}
bus = Bus(config)


def on_connected():
    LOGGER.info("Connected")
    bus.send("python.test", "PythonTestType", {"Data": 123})
    # for i in range(0, 1000000):
    #     bus.send("python.test", "PythonTestType", {"Data": i})


bus.init(on_connected)
