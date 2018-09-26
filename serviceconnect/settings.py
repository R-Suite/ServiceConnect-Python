from .clients.rabbitmq import RabbitClient

settings = {
    "amqp": {
        "queue": {
            "name": None,
            "durable": True,
            "exclusive": False,
            "autoDelete": False,
            "noAck": False,
            "maxPriority": None
        },
        "host": "amqp://guest:guest@localhost",
        "retryDelay": 3000,
        "maxRetries": 3,
        "errorQueue": "errors",
        "auditQueue": "audit",
        "auditEnabled": False,
        "prefetch": 100
    },
    "filters": {
      "after": [],
      "before": [],
      "outgoing": []
    },
    "handlers": {
        # "message type": [ array of callbacks ]
    },
    "client": RabbitClient
}
