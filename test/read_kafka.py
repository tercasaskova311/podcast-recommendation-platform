from kafka import KafkaConsumer, TopicPartition

from config.settings import KAFKA_URL, TOPIC_EPISODE_METADATA

N_LAST = 20                       # number of messages to fetch

def get_last_messages():
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_URL,
        enable_auto_commit=False,
        auto_offset_reset='latest',   # we'll override this per partition
        consumer_timeout_ms=5000,     # stop if no new messages
        value_deserializer=lambda v: v.decode('utf-8')
    )

    partitions = consumer.partitions_for_topic(TOPIC_EPISODE_METADATA)
    if not partitions:
        print(f"No partitions found for topic {TOPIC_EPISODE_METADATA}")
        return

    # Assign partitions manually
    topic_partitions = [TopicPartition(TOPIC_EPISODE_METADATA, p) for p in partitions]
    consumer.assign(topic_partitions)

    # Seek to last N messages
    for tp in topic_partitions:
        end_offset = consumer.end_offsets([tp])[tp]
        start_offset = max(end_offset - N_LAST, 0)
        consumer.seek(tp, start_offset)

    # Collect messages
    messages = []
    for msg in consumer:
        messages.append(msg.value)
        if len(messages) >= N_LAST:
            break

    consumer.close()

    print(f"\nLast {len(messages)} messages in topic '{TOPIC_EPISODE_METADATA}':")
    for i, m in enumerate(messages, 1):
        print(f"{i:02d}: {m}")

if __name__ == "__main__":
    get_last_messages()
