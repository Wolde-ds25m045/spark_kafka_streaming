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
RECOMMENDATIONS_FILE = Path(__file__).with_name("recommendations.json")
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
                    a.name AS artist_name,
                    g.id AS genre_id,
                    g.name AS genre_name
            FROM public.tracks t 
            JOIN public.albums al ON t.album_id = al.id
            JOIN public.artists a ON al.artist_id = a.id
            LEFT JOIN public.genres g ON t.genre_id = g.id
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

def get_recommendations_by_genre(conn, track, limit=5):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                t.id AS track_id,
                t.name AS track_name,
                al.title AS album_name,
                a.name AS artist_name,
                g.name AS genre_name,
                t.unit_price
            FROM public.tracks t
            JOIN public.albums al ON t.album_id = al.id
            JOIN public.artists a ON al.artist_id = a.id
            LEFT JOIN public.genres g ON t.genre_id = g.id
            WHERE t.genre_id = %s
              AND t.id <> %s
            ORDER BY random()
            LIMIT %s;
        """, (track["genre_id"], track["track_id"], limit))

        recommendations = cur.fetchall()

    return [dict(row) for row in recommendations]


def get_favorite_genre_from_events(events):
    genre_scores = {}

    for event in events:
        genre_id = event.get("genre_id")
        genre_name = event.get("genre_name")
        action_type = event.get("action_type")

        if genre_id is None:
            continue

        if action_type == "Like":
            score = 3
        elif action_type == "Play":
            score = 1
        elif action_type == "Skip":
            score = -1
        elif action_type == "Dislike":
            score = -3
        else:
            score = 0

        if genre_id not in genre_scores:
            genre_scores[genre_id] = {
                "genre_id": genre_id,
                "genre_name": genre_name,
                "score": 0,
            }

        genre_scores[genre_id]["score"] += score

    if not genre_scores:
        return None

    favorite_genre = max(
        genre_scores.values(),
        key=lambda genre: genre["score"]
    )

    return favorite_genre
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
        "genre_id": track["genre_id"],
        "genre_name": track["genre_name"],
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
 
def load_processed_recommendations():
    """
    Reads recommendations generated by the external Kafka processor.
    """
    if not RECOMMENDATIONS_FILE.exists():
        return {}

    try:
        with open(RECOMMENDATIONS_FILE, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception:
        return {}
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

    # Count user interactions in this Streamlit session
    if "action_count" not in st.session_state:
        st.session_state.action_count = 0

    # Store user events locally for recommendation logic
    if "user_events" not in st.session_state:
        st.session_state.user_events = []

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
        st.write(f"Genre: {track['genre_name']}")

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
                    st.session_state.action_count += 1
                    st.session_state.user_events.append(event)
                    st.info("Playing track. Event sent to Kafka.")

        with col2:
            if st.button("Like", icon="✅"):
                event = create_event(
                    user_id=st.session_state.user_id,
                    track=track,
                    action_type="Like"
                )

                if send_event_to_kafka(producer, event):
                    st.session_state.action_count += 1
                    st.session_state.user_events.append(event)
                    st.success("You liked the track. Event sent to Kafka.")

                # Load another track after like
                load_new_track(conn)
                st.rerun()
                

        with col3:
            if st.button("Dislike", icon="❌"):
                event = create_event(
                    user_id=st.session_state.user_id,
                    track=track,
                    action_type="Dislike"
                )

                if send_event_to_kafka(producer, event):
                    st.session_state.action_count += 1
                    st.session_state.user_events.append(event)
                    st.error("You disliked the track. Event sent to Kafka.")

                # Load another track after dislike
                load_new_track(conn)
                st.rerun()


        with col4:
            if st.button("Skip/Next", icon="⏭️"):
                event = create_event(
                    user_id=st.session_state.user_id,
                    track=track,
                    action_type="Skip"
                )

                if send_event_to_kafka(producer, event):
                    st.session_state.action_count += 1
                    st.session_state.user_events.append(event)
                    st.warning("Track skipped. Event sent to Kafka.")

                # Load another track after skip
                load_new_track(conn)
                st.rerun()

    else:
        st.warning("No track found in the database.")

    st.divider()
    st.write(f"Actions in this session: {st.session_state.action_count}")

    if st.button("Reset session counter"):
        st.session_state.action_count = 0
        st.session_state.user_events = []
        st.rerun()

    if st.session_state.action_count < 10:
        st.info("Interact with at least 10 tracks to unlock recommendations.")
    else:
        st.success("You have enough interactions for recommendations.")
        processed_recommendations = load_processed_recommendations()
        processed_user_recommendation = processed_recommendations.get(st.session_state.user_id)

        if processed_user_recommendation:
            st.subheader("Processed Kafka recommendation summary")

            preferences = processed_user_recommendation.get("preferences", {})

            favorite_genre_from_kafka = preferences.get("favorite_genre")
            favorite_artist = preferences.get("favorite_artist")
            favorite_album = preferences.get("favorite_album")

            if favorite_genre_from_kafka:
                st.write(
                    f"Favorite genre from Kafka stream: "
                    f"**{favorite_genre_from_kafka['name']}**"
                )

            if favorite_artist:
                st.write(
                    f"Favorite artist from Kafka stream: "
                    f"**{favorite_artist['name']}**"
                )

            if favorite_album:
                st.write(
                    f"Favorite album from Kafka stream: "
                    f"**{favorite_album['name']}**"
                )
        else:
            st.info("The external Kafka processor has not written recommendations for this user yet.")

        favorite_genre = get_favorite_genre_from_events(st.session_state.user_events)

        if favorite_genre:
            recommendation_seed_track = {
                "genre_id": favorite_genre["genre_id"],
                "track_id": track["track_id"],
            }
            recommendations = get_recommendations_by_genre(conn, recommendation_seed_track)
            st.write(
                f"Based on your interactions, your strongest genre is: "
                f"**{favorite_genre['genre_name']}**"
            )
        else:
            recommendations = get_recommendations_by_genre(conn, track)

        st.subheader("Recommended tracks from your preferred genre")

        for rec in recommendations:
            st.write(
                f"**{rec['track_name']}** by {rec['artist_name']} "
                f"— Album: {rec['album_name']} — Genre: {rec['genre_name']} "
                f"— Price: ${rec['unit_price']}"
            )


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