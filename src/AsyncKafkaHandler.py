import asyncio
import datetime
import json
import logging
from queue import Queue
from typing import Any

from aiokafka import AIOKafkaProducer
from logging.handlers import QueueHandler, QueueListener


class AsyncKafkaHandler(QueueHandler):

    def __init__(self,
                 app_name: str,
                 bootstrap_servers: str,
                 topic_name: str,
                 queue: Queue,
                 *args: tuple[Any],
                 **kwargs: dict[str, Any]
                 ) -> None:
        """
        :param bootstrap_servers: Kafka server "host:port"
        :param topic_name: Kafka topic name
        :param queue: log message queue
        :param args: args for AIOKafkaProducer
        :param kwargs: kwargs for AIOKafkaProducer
        """
        self.app_name = app_name
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            *args, **kwargs
        )
        super().__init__(queue)

    async def send_log(self, record: logging.LogRecord) -> None:
        log = {
            "app_name": self.app_name,
            "module": record.module,
            "level": record.levelname,
            "timestamp": str(datetime.datetime.now()),
            "message": record.msg
        }
        await self.producer.start()
        await self.producer.send_and_wait(self.topic_name, log)
        await self.producer.stop()

    def emit(self, record: logging.LogRecord) -> None:
        log_entry = self.prepare(record)
        asyncio.ensure_future(self.send_log(log_entry))


def get_kafka_async_logger(
        name: str,
        app_name: str,
        bootstrap_servers: str,
        topic_name: str,
        kafka_args: tuple[Any] = tuple(),
        kafka_kwargs: dict[str, Any] | None = None,
        *args, **kwargs
        ) -> logging.Logger:
    """
    :param app_name: app name
    :param name: Logger name
    :param topic_name: Kafka topic name to produce logs
    :param bootstrap_servers: Kafka url "host:port"
    :param kafka_args: args for AIOKafkaProducer
    :param kafka_kwargs: kwargs for AIOKafkaProducer
    :param args: logging basicConfig args
    :param kwargs: logging basicConfig kwargs
    :return: Logger with attached kafka async handler
    """

    if kafka_kwargs is None:
        kafka_kwargs = dict()

    logging.basicConfig(*args, **kwargs)

    # Create log message queue
    log_queue = Queue()

    # Create handler and listener to async send logs
    queue_handler = AsyncKafkaHandler(
        app_name=app_name,
        queue=log_queue,
        bootstrap_servers=bootstrap_servers,
        topic_name=topic_name,
        *kafka_args,
        **kafka_kwargs
    )
    queue_listener = QueueListener(log_queue, queue_handler)
    queue_listener.start()

    logger = logging.getLogger(name)
    logger.addHandler(queue_handler)
    return logger
