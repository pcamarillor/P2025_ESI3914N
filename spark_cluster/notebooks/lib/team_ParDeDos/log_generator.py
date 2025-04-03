# This file should generate random entry logs to simulate a web server with this format:
"""
2025-05-20 10:00:00 | WARN | Disk usage 85% | server-node-1
2025-05-20 10:00:00 | ERROR | Error 500 | server-node-2
"""
import random
import datetime
import os
import time


class LogGenerator:

    def __init__(
        self,
        server_nodes,
        log_messages,
        log_dir="/home/jovyan/notebooks/data/structured_streaming_files/",
    ):
        self.server_nodes = server_nodes
        self.log_messages = log_messages
        self.log_dir = log_dir
        os.makedirs(
            self.log_dir, exist_ok=True
        )  # Creates the directory if it doesn't exist

    def generate_log(self) -> str:
        """Randomly generate log messages."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_level = random.choice(list(self.log_messages.keys()))
        message = random.choice(self.log_messages[log_level])
        node = random.choice(self.server_nodes)

        log_entry = f"{timestamp} | {log_level} | {message} | {node}"
        return log_entry

    def write_log_to_file(self):
        """Writes the logs in a specific directory with a specific name the the file."""
        filename = filename = (
            f"{self.log_dir}/log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        with open(filename, "w") as f:
            log_entry = self.generate_log()
            f.write(f"{log_entry}\n")

    def start_streaming(self, num_logs=100, interval=2):
        """Generates a determined number of logs in a defined interval."""
        for _ in range(num_logs):
            self.write_log_to_file()
            time.sleep(interval)  # Simulate the generations of the logs


if __name__ == "__main__":
    server_nodes = ["server-node-1", "server-node-2", "server-node-3"]
    log_messages = {
        "WARN": [
            "Disk usage 85%",
            "CPU temperature high",
            "Memory usage at critical level",
            "Network latency increased",
        ],
        "ERROR": [
            "Database connection lost",
            "Unauthorized login attempt detected",
            "Imminent Explosion",
        ],
        "INFO": ["Service restarted successfully"],
    }
    log_dir = "/home/jovyan/notebooks/data/structured_streaming_log_files/"

    log_generator = LogGenerator(server_nodes, log_messages, log_dir)
    log_generator.start_streaming(50)
