import random
import time

# Simulación de evento de video streaming
def generate_streaming_event():
    resolutions = ['480p', '720p', '1080p', '4K']
    genres = ['Drama', 'Comedy', 'Action', 'Documentary', 'Horror', 'Sci-Fi']
    regions = ['US', 'MX', 'IN', 'UK', 'DE', 'JP', 'BR']
    video_ids = [f'vid_{i:03d}' for i in range(1, 101)]

    event = {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "video_id": random.choice(video_ids),
        "watch_time_seconds": random.randint(30, 3600),
        "resolution": random.choice(resolutions),
        "bitrate_kbps": random.randint(800, 8000),
        "buffering_events": random.randint(0, 5),
        "paused": random.choice([True, False]),
        "skipped": random.choice([True, False]),
        "genre": random.choice(genres),
        "region": random.choice(regions),
        "recommended": random.choice([True, False]),
        "timestamp": time.time()
    }

    return event
