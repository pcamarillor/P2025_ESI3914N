#!/usr/bin/env python3
import time
import random
import os
import string

# Configuration
output_dir = "/home/jovyan/notebooks/data/traffic_data/input"
os.makedirs(output_dir, exist_ok=True)

def generate_random_traffic_line():
        # Generate random data
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        vehicle_id = f"{random.choice(string.ascii_uppercase)}{random.choice(string.ascii_uppercase)}{random.choice(string.ascii_uppercase)}-{random.randint(0,999)}"
        speed = random.randint(0, 119)
        latitude = f"{random.randint(0, 89)}.{random.randint(0, 98):02d}"
        longitude = f"-{random.randint(0, 179)}.{random.randint(0, 98):02d}"
        log_line = f"{timestamp} {vehicle_id} {speed} ({latitude},{longitude})"
        return log_line

while True:
    
    # Write to file
    filename = f"{output_dir}/data_{int(time.time())}.log"
    with open(filename, 'a') as f:
        n = random.randint(30,300)
        # Create a random number of lines
        _ = [f.write(generate_random_traffic_line() + "\n") for i in range(n)]

    # Wait 5 seconds
    time.sleep(5)