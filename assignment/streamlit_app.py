import streamlit as st
import psycopg2
import random
import json
from datetime import datetime, timezone
from psycopg2.extras import RealDictCursor
from confluent_kafka import Producer
from pathlib import Path
from utils import ccloud_lib
import uuid

# ---------------------------------------------------------
# Database connection details
# ---------------------------------------------------------

# Database connection details
HOST = "fhtw-big-data.postgres.database.azure.com"
DATABASE = "music_store"
USER = "student"
PASSWORD = "reRZ2pjg1WxqlwjU"

# Establish a connection to the database
@st.cache_resource
def get_connection():
    conn = psycopg2.connect(
        host=HOST,
        dbname=DATABASE,
        user=USER,
        password=PASSWORD
    )
    return conn


# ---------------------------------------------------------
# Kafka connection details
# ---------------------------------------------------------

CONFIG_FILE = Path(__file__).with_name("kafka.config")
TOPIC_NAME = "endlich_ferien"

# Kafka Config
@st.cache_resource
def get_kafka_config():
    return ccloud_lib.read_ccloud_config(str(CONFIG_FILE))

# Kafka Producer
@st.cache_resource
def get_kafka_producer():
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(
        get_kafka_config().copy()
    )
    return Producer(producer_conf)

# ---------------------------------------------------------
# Getting tracks from DB
# ---------------------------------------------------------
def get_random_track(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT t.id AS track_id,
                    t.name AS track_name, 
                    t.milliseconds,
                    t.unit_price,
                    al.id AS album_id,
                    al.title AS album_name,
                    a.id AS artist_id,
                    a.name AS artist_name
            FROM public.tracks t 
            JOIN public.albums al ON t.album_id = al.id
            JOIN public.artists a ON al.artist_id = a.id
            ORDER BY random()
            LIMIT 1;
        """)
        track = cur.fetchone()
    return dict(track) if track else None


def load_new_track(conn):
    """
    Stores the currently displayed track in session state.

    This is important because Streamlit reruns the whole script
    after every button click. Without session_state, the displayed
    track could change before the event is sent.
    """
    st.session_state.current_track = get_random_track(conn)


# ---------------------------------------------------------
# User interaction event
# ---------------------------------------------------------

# Create user interaction event
def create_event(user_id, track, action_type):
    """
    Creates a structured event dictionary for Kafka.

    action_event is either "Play", "Like", "Dislike", "Skip"
    """

    event = {
        "user_id": user_id,
        "track_id": track["track_id"],
        "action_type": action_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),

        # Optional metadata
        "track_name": track["track_name"],
        "artist_id": track["artist_id"],
        "artist_name": track["artist_name"],
        "album_id": track["album_id"],
        "album_name": track["album_name"],
        "track_length_ms": track["milliseconds"],
        "unit_price": float(track["unit_price"]) if track["unit_price"] is not None else None
    }

    return event

# Send event to Kafka
def send_event_to_kafka(producer, event):
    """
    Sends one user interaction event to the Kafka topic.
    The event is converted to JSON before sending.
    """

    try:
        producer.produce(
            topic=TOPIC_NAME,
            key=str(event["user_id"]),
            value=json.dumps(event).encode("utf-8")
        )

        # Ensures the event is actually sent immediately.
        producer.flush()

        return True

    except Exception as e:
        st.error(f"Failed to send event to Kafka: {e}")
        return False
 

# ---------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------
def main():
    st.title("Music Recommendations using Kafka Events")

    conn = get_connection()
    producer = get_kafka_producer()

    # Create a persistent user ID for the current browser session
    if "user_id" not in st.session_state:
        st.session_state.user_id = f"user_{uuid.uuid4().hex[:8]}"

    # Store the current track so it does not change after every button click
    if "current_track" not in st.session_state:
        load_new_track(conn)
    
    # Sidebar user settings
    st.sidebar.header("User Settings")
    st.session_state.user_id = st.sidebar.text_input(
        "User ID",
        value=st.session_state.user_id
    )

    track = st.session_state.current_track

    if track:
        st.header(track["track_name"])
        st.subheader(f"Artist: {track['artist_name']}")
        st.write(f"Album: {track['album_name']}")

        if track.get("unit_price") is not None:
            st.write(f"Price: ${track['unit_price']}")

        st.divider()

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if st.button("Play", icon="🎵"):
                event = create_event(
                    user_id=st.session_state.user_id,
                    track=track,
                    action_type="Play"
                )

                if send_event_to_kafka(producer, event):
                    st.info("Playing track. Event sent to Kafka.")

        with col2:
            if st.button("Like", icon="✅"):
                event = create_event(
                    user_id=st.session_state.user_id,
                    track=track,
                    action_type="Like"
                )

                if send_event_to_kafka(producer, event):
                    st.success("You liked the track. Event sent to Kafka.")
                

        with col3:
            if st.button("Dislike", icon="❌"):
                event = create_event(
                    user_id=st.session_state.user_id,
                    track=track,
                    action_type="Dislike"
                )

                if send_event_to_kafka(producer, event):
                    st.error("You disliked the track. Event sent to Kafka.")


        with col4:
            if st.button("Skip/Next", icon="⏭️"):
                event = create_event(
                    user_id=st.session_state.user_id,
                    track=track,
                    action_type="Skip"
                )

                if send_event_to_kafka(producer, event):
                    st.warning("Track skipped. Event sent to Kafka.")

                # Load another track after skip
                load_new_track(conn)
                st.rerun()

    else:
        st.warning("No track found in the database.")

    # st.title("Track Recommender")
    # track = get_random_track(conn)
    # if track:
    #     st.header(f"Track: {track[0]}")
    #     st.subheader(f"Artist: {track[1]}")
        
    #     if st.button("Thumbs Up"):
    #         st.success("You liked the track!")
    #     if st.button("Thumbs Down"):
    #         st.error("You disliked the track!")
    #     if st.button("Play"):
    #         st.info("Playing the track!")

if __name__ == "__main__":
    main()