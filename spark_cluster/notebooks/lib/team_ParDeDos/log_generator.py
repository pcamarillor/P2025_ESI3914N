# This file should generate random entry logs to simulate a web server with this format:
"""
2025-05-20 10:00:00 | WARN | Disk usage 85% | server-node-1
2025-05-20 10:00:00 | ERROR | Error 500 | server-node-2 
"""

import random, datetime

log_messages = {
    "WARN": [
        "Disk usage 85%",
        "CPU temperature high",
        "Memory usage at critical level",
        "Network latency increased"
    ],
    "ERROR": [
        "Database connection lost",
        "Unauthorized login attempt detected"
    ],
    "INFO": [
        "Service restarted successfully"
    ]
}

log_messages = [
    "Disk usage 85%",
    "CPU temperature high",
    "Memory usage at critical level",
    "Service restarted successfully",
    "Unauthorized login attempt detected",
    "Network latency increased",
    "Database connection lost"
]

server_nodes = ["server-node-1", "server-node-2", "server-node-3"]

def generate_log():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_level = random.choice(list(log_messages.keys()))
    message = random.choice(log_messages[log_level])
    node = random.choice(server_nodes)

    log_entry = f"{timestamp} | {log_level} | {message} | {node}"
    return log_entry
    