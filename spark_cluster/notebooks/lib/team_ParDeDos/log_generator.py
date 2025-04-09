import random
import datetime
import os
import time


class LogGenerator:
    def __init__(self, server_nodes, log_messages, log_dir, logs_per_file=10):
        self.server_nodes = server_nodes
        self.log_messages = log_messages
        self.log_dir = log_dir
        self.logs_per_file = logs_per_file  # Número de logs por archivo
        os.makedirs(self.log_dir, exist_ok=True)

    def generate_log(self) -> str:
        """Genera un mensaje de log aleatorio."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_level = random.choice(list(self.log_messages.keys()))
        message = random.choice(self.log_messages[log_level])
        node = random.choice(self.server_nodes)
        return f"{timestamp} | {log_level} | {message} | {node}"

    def write_log_to_file(self):
        """Escribe 'logs_per_file' logs en un solo archivo."""
        filename = f"{self.log_dir}/log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        with open(filename, "w") as f:
            for _ in range(self.logs_per_file):
                f.write(self.generate_log() + "\n")
        print(f"Generated {self.logs_per_file} logs in {filename}")  # Mensaje de depuración

    def start_streaming(self, interval=2):
        """Genera logs indefinidamente hasta que el usuario interrumpa el proceso."""
        try:
            while True:
                self.write_log_to_file()
                time.sleep(interval)  # Espera antes de generar el siguiente archivo
        except KeyboardInterrupt:
            print("\nLog generation stopped by user.")


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

    log_generator = LogGenerator(server_nodes, log_messages, log_dir, logs_per_file=10)
    log_generator.start_streaming(interval=5)
