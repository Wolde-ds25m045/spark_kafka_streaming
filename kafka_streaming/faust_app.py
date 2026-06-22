
from pathlib import Path

import faust

from faust_music_events import MusicEvent
from utils.faust_helpers import build_app_kwargs, ensure_faust_topics, load_kafka_config

CONFIG_FILE = Path(__file__).with_name("kafka.config")
APP_ID = "music_stream_processor_mo"
SOURCE_TOPIC = "music-fhtw"

kafka_app_config = load_kafka_config(CONFIG_FILE)
ensure_faust_topics(kafka_app_config, APP_ID, [SOURCE_TOPIC], ["song_plays"])

app = faust.App(APP_ID, **build_app_kwargs(kafka_app_config))

topic = app.topic(SOURCE_TOPIC, value_type=MusicEvent)
song_plays = app.Table("song_plays", default=int)


@app.agent(topic)
async def process(stream):
    async for event in stream:
        song_plays[event.userId] += 1
        print(f"User {event.userId} has listened to {song_plays[event.userId]} songs.")
