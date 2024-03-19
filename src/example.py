import asyncio
import logging

from AsyncKafkaHandler import get_kafka_async_logger

# Your kafka server
bootstrap_servers = 'localhost:9093'
# Your kafka topic name
topic_name = 'my-topic'


async def main():
    # Setup logger
    logger = get_kafka_async_logger(
        name="Kafka logger",
        bootstrap_servers=bootstrap_servers,
        topic_name=topic_name,
        level=logging.INFO
    )

    # Write logs
    logger.info('Init kafka logger1')
    logger.info({"someData": "Init kafka logger1"})

    # Waiting async log sending
    await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())
