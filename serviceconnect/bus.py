from .utils import mergedicts
from .settings import settings


class Bus:

    def __init__(self, config):
        self.config = mergedicts(settings, config)

    def init(self, callback=None):
        self.client = self.config["client"](self.config, self.__consumeMessage)
        try:
            self.client.run(callback)
        except KeyboardInterrupt:
            self.client.stop()

    def send(self, endpoint, typeName, message, headers={}):
        result = self.__processFilters(self.config["filters"]["outgoing"],
                                       message, headers, typeName)
        if result:
            self.client.send(endpoint, typeName, message, headers)

    def publish(self, typeName, message, headers={}):
        result = self.__processFilters(self.config["filters"]["outgoing"],
                                       message, headers, typeName)
        if result:
            self.client.publish(typeName, message, headers)

    def close(self):
        self.client.close()

    def __processFilters(self, filters, message, headers, typeName):
        for f in filters:
            if not f(message, headers, typeName):
                return False
        return True

    def __consumeMessage(self, message, headers, typeName):
        process = self.__processFilters(self.config["filters"]["before"],
                                        message, headers, typeName)
        if not process:
            return

        if typeName in self.config["handlers"]:
            handler = self.config["handlers"][typeName]
            handler(message, headers, typeName)

        process = self.__processFilters(self.config["filters"]["after"],
                                        message, headers, typeName)
        if not process:
            return
