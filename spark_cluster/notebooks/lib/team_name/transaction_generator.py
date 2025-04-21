from datetime import datetime
import random

# Function to generate transactions data
def generate_transaction_data():
    sensor_ids = ['origin1', 'origin2', 'origin3']
    return {
        'origin_id': random.choice(sensor_ids),
        'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'amount': random.uniform(13.0, 5673.0)
    }
