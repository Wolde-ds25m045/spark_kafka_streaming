import json
from pathlib import Path

from confluent_kafka import Consumer
from utils import ccloud_lib


CONFIG_FILE = Path(__file__).with_name("kafka.config")
TOPIC_NAME = "endlich_ferien"
OUTPUT_FILE = Path(__file__).with_name("recommendations.json")


def get_score(action_type):
    if action_type == "Like":
        return 3
    if action_type == "Play":
        return 1
    if action_type == "Skip":
        return -1
    if action_type == "Dislike":
        return -3
    return 0

def calculate_user_preferences(events):
    genre_scores = {}
    artist_scores = {}
    album_scores = {}

    for event in events:
        score = get_score(event.get("action_type"))

        genre_id = event.get("genre_id")
        genre_name = event.get("genre_name")
        if genre_id is not None:
            if genre_id not in genre_scores:
                genre_scores[genre_id] = {
                    "id": genre_id,
                    "name": genre_name,
                    "score": 0,
                }
            genre_scores[genre_id]["score"] += score

        artist_id = event.get("artist_id")
        artist_name = event.get("artist_name")
        if artist_id is not None:
            if artist_id not in artist_scores:
                artist_scores[artist_id] = {
                    "id": artist_id,
                    "name": artist_name,
                    "score": 0,
                }
            artist_scores[artist_id]["score"] += score

        album_id = event.get("album_id")
        album_name = event.get("album_name")
        if album_id is not None:
            if album_id not in album_scores:
                album_scores[album_id] = {
                    "id": album_id,
                    "name": album_name,
                    "score": 0,
                }
            album_scores[album_id]["score"] += score

    def best_score(scores):
        if not scores:
            return None
        
        best_item = max(scores.values(), key=lambda item: item["score"])

        if best_item["score"] <= 0:
            return None

        return best_item

    return {
        "favorite_genre": best_score(genre_scores),
        "favorite_artist": best_score(artist_scores),
        "favorite_album": best_score(album_scores),
    }


def load_existing_recommendations():
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, "r", encoding="utf-8") as file:
            return json.load(file)
    return {}


def save_recommendations(recommendations):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(recommendations, file, indent=2)


def main():
    kafka_config = ccloud_lib.read_ccloud_config(str(CONFIG_FILE))
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(
        kafka_config.copy()
    )

    consumer_conf["group.id"] = "recommendation_processor_group"
    consumer_conf["auto.offset.reset"] = "earliest"

    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_NAME])

    user_events = {}
    recommendations = load_existing_recommendations()

    print(f"Listening to Kafka topic: {TOPIC_NAME}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            user_id = event["user_id"]

            if user_id not in user_events:
                user_events[user_id] = []

            user_events[user_id].append(event)

            print(f"Received event for {user_id}: {event['action_type']}")

            if len(user_events[user_id]) >= 10:
                preferences = calculate_user_preferences(user_events[user_id])

                recommendations[user_id] = {
                    "event_count": len(user_events[user_id]),
                    "message": "User has enough interactions for recommendations.",
                    "preferences": preferences,
                    "latest_event": event,
                }

                save_recommendations(recommendations)
                print(f"Updated recommendations for {user_id}")

    except KeyboardInterrupt:
        print("Stopping recommendation processor.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()