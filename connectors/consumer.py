import json
import requests
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer, KafkaException
from connectors.config import KafkaConfig
from connectors.connect import (
    create_source_connector,
    register_sink_value_schema,
    create_sink_connector,
)


def create_topics(admin, topics) -> None:
    # Create topic
    new_topics = [
        NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics
    ]
    fs = admin.create_topics(new_topics)

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))


def consume_messages(consumer) -> None:
    # Read messages from Kafka, print to stdout
    source_connector_created = False
    sink_value_schema_registered = False
    sink_connector_created = False
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")
        value_dict = json.loads(value)

        print(value_dict)
        if key == "source-connector" and not source_connector_created:
            create_source_connector(value_dict)
            source_connector_created = True
        else:
            if "index" in value_dict:
                if not sink_value_schema_registered:
                    register_sink_value_schema(value_dict["index"])
                    sink_value_schema_registered = True

                if not sink_connector_created:
                    create_sink_connector(value_dict)
                    sink_connector_created = True
            else:
                raise Exception("no index found")

            if (
                source_connector_created
                and sink_value_schema_registered
                and sink_connector_created
            ):
                print("Successfully configured connectors! Exiting listening loop")
                break


def produce_config_ready_message(producer) -> None:
    # Once the schema is registered, produce a special message to the readiness topic
    producer.produce(topic="_config_success", key="config_ready", value="true")
    producer.flush()
    print("Produced to config ready topic")


def main():
    kafka_config = KafkaConfig()
    consumer_conf = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "group.id": "_connector_config_group",
        "auto.offset.reset": "smallest",
        "allow.auto.create.topics": "true",
    }

    admin = AdminClient({"bootstrap.servers": kafka_config.bootstrap_servers})
    topics = ["_connector_config"]
    create_topics(admin, topics)

    consumer = Consumer(consumer_conf)
    consumer.subscribe(topics)
    consume_messages(consumer)

    producer = Producer({"bootstrap.servers": kafka_config.bootstrap_servers})
    produce_config_ready_message(producer)

    consumer.close()


if __name__ == "__main__":
    main()
