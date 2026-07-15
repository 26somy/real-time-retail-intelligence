import argparse
import json
import os
import threading
import uuid
from datetime import datetime, timezone

from azure.eventhub import EventHubConsumerClient
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


load_dotenv()

CONSUMER_STARTED_AT = datetime.now(timezone.utc)

EVENTHUB_CONSUMER_CONNECTION_STRING = os.getenv("EVENTHUB_CONSUMER_CONNECTION_STRING")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BRONZE_CONTAINER_NAME = os.getenv("BRONZE_CONTAINER_NAME", "bronze")

STREAMS = {
    "sales": {
        "eventhub_name": os.getenv("SALES_EVENTHUB_NAME"),
        "bronze_folder": "sales",
    },
    "inventory": {
        "eventhub_name": os.getenv("INVENTORY_EVENTHUB_NAME"),
        "bronze_folder": "inventory",
    },
    "returns": {
        "eventhub_name": os.getenv("RETURN_EVENTHUB_NAME"),
        "bronze_folder": "returns",
    },
}


blob_service_client = BlobServiceClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING
)

container_client = blob_service_client.get_container_client(
    BRONZE_CONTAINER_NAME
)


def parse_event_time(event_time_value: str) -> datetime:
    if not event_time_value:
        return datetime.now(timezone.utc)

    parsed_time = datetime.fromisoformat(event_time_value.replace("Z", "+00:00"))

    if parsed_time.tzinfo is None:
        parsed_time = parsed_time.replace(tzinfo=timezone.utc)

    return parsed_time


def normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def get_event_status(enqueued_time: datetime) -> str:
    enqueued_time = normalize_datetime(enqueued_time)

    if enqueued_time < CONSUMER_STARTED_AT:
        return "OLD_REPLAY"

    return "LIVE"


def write_event_to_bronze(event_body: str, bronze_folder: str) -> str:
    event_json = json.loads(event_body)

    event_id = event_json.get("event_id", str(uuid.uuid4()))
    event_time = parse_event_time(event_json.get("event_time"))

    blob_name = (
        f"{bronze_folder}/"
        f"year={event_time.year}/"
        f"month={event_time.month:02d}/"
        f"day={event_time.day:02d}/"
        f"{event_id}.json"
    )

    container_client.upload_blob(
        name=blob_name,
        data=json.dumps(event_json, indent=2),
        overwrite=True,
    )

    return blob_name


def start_consumer(stream_name: str, eventhub_name: str, bronze_folder: str, start_position: str):
    def on_event(partition_context, event):
        event_body = event.body_as_str(encoding="UTF-8")
        blob_name = write_event_to_bronze(event_body, bronze_folder)

        enqueued_time = normalize_datetime(event.enqueued_time)
        event_status = get_event_status(enqueued_time)

        print(
            f"[{event_status}] [{stream_name}] "
            f"enqueued={enqueued_time.isoformat()} "
            f"consumer_started={CONSUMER_STARTED_AT.isoformat()} "
            f"partition={partition_context.partition_id} "
            f"sequence={event.sequence_number} "
            f"→ bronze/{blob_name}"
        )

    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=EVENTHUB_CONSUMER_CONNECTION_STRING,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
    )

    print(f"Listening to {eventhub_name} → bronze/{bronze_folder}/")

    with consumer:
        consumer.receive(
            on_event=on_event,
            starting_position=start_position,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Read Event Hub events and write raw JSON to Bronze."
    )

    parser.add_argument(
        "--start",
        choices=["latest", "earliest"],
        default="latest",
        help="latest = only new events, earliest = replay retained events",
    )

    args = parser.parse_args()

    if not EVENTHUB_CONSUMER_CONNECTION_STRING:
        raise ValueError("Missing EVENTHUB_CONSUMER_CONNECTION_STRING in .env")

    if not AZURE_STORAGE_CONNECTION_STRING:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING in .env")

    start_position = "@latest" if args.start == "latest" else "-1"

    threads = []

    for stream_name, config in STREAMS.items():
        thread = threading.Thread(
            target=start_consumer,
            args=(
                stream_name,
                config["eventhub_name"],
                config["bronze_folder"],
                start_position,
            ),
            daemon=True,
        )
        thread.start()
        threads.append(thread)

    print(f"Consumer started at: {CONSUMER_STARTED_AT.isoformat()}")
    print("Consumer is running. Press Ctrl + C to stop.")

    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nConsumer stopped.")


if __name__ == "__main__":
    main()