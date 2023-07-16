import requests
import time
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from connectors.config import KafkaConfig

kafka_config = KafkaConfig()
print(kafka_config.bootstrap_servers)
print(kafka_config.connect_server)
print(kafka_config.schema_registry_server)


def create_source_connector(conn: dict[str, str]) -> None:
    print("creating source connector...")
    try:
        url = f"{kafka_config.connect_server}/connectors"
        r = requests.post(
            url,
            json={
                "name": f'{conn["table_name"]}-connector',
                "config": {
                    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                    "plugin.name": "pgoutput",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",  # TODO: support avro
                    "database.hostname": f'{conn["host"]}',
                    "database.port": f'{conn["port"]}',
                    "database.user": f'{conn["user"]}',
                    "database.password": f'{conn["password"]}',
                    "database.dbname": f'{conn["dbname"]}',
                    "table.include.list": f'{conn["schema_name"]}.{conn["table_name"]}',
                    "transforms": "unwrap",
                    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                    "transforms.unwrap.drop.tombstones": "false",
                    "transforms.unwrap.delete.handling.mode": "rewrite",
                    "topic.prefix": f'{conn["table_name"]}',
                },
            },
        )
    except Exception as e:
        # TODO: handle
        print(e)


def register_sink_value_schema(index: str) -> None:
    print("registering sink schema...")
    schema_str = """
    {
    "name": "embedding",
    "type": "record",
    "fields": [
        {
        "name": "doc",
        "type": {
            "type": "array",
            "items": "float"
        }
        },
        {
        "name": "metadata",
        "type": {
            "type": "array",
            "items": "string"
        },
        "default": []
        }
    ]
    }
    """

    while True:
        try:
            time.sleep(1)  # Wait for 1 second before retrying
            avro_schema = Schema(schema_str, "AVRO")
            sr = SchemaRegistryClient({"url": kafka_config.schema_registry_server})
            subject_name = f"{index}-value"
            sr.register_schema(subject_name, avro_schema)
            break

        except requests.exceptions.ConnectionError:
            print("Schema registry server is not yet available, retrying...")

    print("Schema written successfully")


def create_sink_connector(conn: dict[str, str]) -> None:
    print("creating sink connector...")
    try:
        url = f"{kafka_config.connect_server}/connectors"
        r = requests.post(
            url,
            json={
                "name": f"sink-connector",
                "config": {
                    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
                    "topics": f'{conn["index"]}',
                    "key.ignore": "true",
                    "name": "sink-connector",
                    "value.converter": "io.confluent.connect.avro.AvroConverter",
                    "value.converter.schema.registry.url": f"{kafka_config.schema_registry_server}",
                    "connection.url": f'{conn["host"]}',
                    "connection.username": f'{conn["user"]}',
                    "connection.password": f'{conn["password"]}',
                },
            },
        )
    except Exception as e:
        # TODO: handle
        print(e)
