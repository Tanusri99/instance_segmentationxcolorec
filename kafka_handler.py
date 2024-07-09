from kafka.admin import KafkaAdminClient, NewTopic
from logger import create_logger

logger, traces = create_logger("Kafka Handler")

def create_topic(topic: str):
    """Checks if an existing kafka topic with specified names exists,
    creates a new topic if it does not.

    Args:
        topic(str): name of kafka topic to be created

    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092", 
            client_id='handler'
        )
    except BaseException as b:
        logger.error(f"Failed to create Kafka admin client: {b}", exc_info=traces)
    else:
        if topic not in admin_client.list_topics():
            topic_list = []
            topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
            try:
                admin_client.create_topics(new_topics=topic_list, validate_only=False)
                logger.info(f"Created Kafka topic - {topic}")
            except BaseException as b:
                logger.error(f"Failed to create Kafka topic: {b}", exc_info=traces)
