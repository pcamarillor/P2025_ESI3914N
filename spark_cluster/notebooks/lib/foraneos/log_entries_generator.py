import json
import random
from datetime import datetime

def generate_log_entry(log_messages):
    category = random.choice(list(log_messages.keys()))
    message = random.choice(log_messages[category])
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    server_node = "server-node-{}".format( random.randint(1,9))
    return "{} | {} | {} | {}".format(timestamp, category, message, server_node)

if __name__ == "__main__":
    log_messages = {
        "WARN": [
            "Disk usage 85%", "Memory usage high", "CPU temperature warning", "Network latency detected", "Disk space running low",
            "Slow database response", "High number of connections", "Service restart needed", "Potential security risk detected", "System time drift detected",
            "Configuration file modified", "Unexpected traffic spike", "Software update required", "Possible memory leak", "Process taking too long",
            "Filesystem corruption detected", "Swap usage high", "Database connection unstable", "CPU usage 95%", "High error rate in logs",
            "Unusual login activity", "VPN connection unstable", "Packet loss detected", "Background job running long", "SSL certificate expiring soon",
            "High CPU load average", "Hardware temperature rising", "Excessive API calls detected", "Unusual outbound traffic", "Potential data inconsistency",
            "New device detected on network", "Backup process slow", "Unexpected reboot detected", "Service response delayed", "Email queue backlog growing",
            "Too many open files", "DNS resolution failures increasing", "Insufficient file handles available", "Low available entropy", "Message queue delay growing",
            "High garbage collection time", "Unexpected spike in failed jobs", "Slow response from external service", "Cloud resource limits approaching",
            "New SSH key detected", "Filesystem read-only mode", "Unusual cron job execution", "Increasing error rate in API", "Possible DoS attack detected"
        ],
        "ERROR": [
            "500 Internal Server Error", "Application crashed", "Database connection failed", "Memory allocation failed", "File system full",
            "Service not responding", "Disk I/O error", "Critical process terminated", "Kernel panic detected", "Network unreachable",
            "Data corruption detected", "Unauthorized access attempt", "Service authentication failure", "Critical security vulnerability detected",
            "Application timeout error", "Data inconsistency detected", "Severe performance degradation", "System out of disk space", "Disk read failure",
            "Process out of memory", "Service startup failure", "System clock sync failure", "Hardware failure detected", "Unexpected service termination",
            "Database corruption detected", "Cannot write to log file", "Backup process failed", "Configuration error detected", "API service failure",
            "Cloud resource exhausted", "Container crash detected", "SSL handshake failure", "Unauthorized API request", "Broken pipe error",
            "File permissions issue", "Kernel module load failure", "Network interface down", "Firewall rule misconfiguration", "Unexpected shutdown detected",
            "Critical hardware component failure", "Log file write failure", "Too many authentication failures", "Service deployment failure",
            "Unexpected data format in input", "Job queue failure", "Data loss detected", "Service loop detected", "Major dependency failure",
            "Unrecoverable system error"
        ],
        "INFO": [
            "User login successful", "Service started", "Scheduled job executed", "Configuration file updated", "New user registered",
            "Backup completed successfully", "Cache cleared", "Database query executed", "File uploaded successfully", "Password changed successfully",
            "System update completed", "API request processed", "New device connected", "System rebooted", "Log rotation completed",
            "Session expired", "Cron job completed", "Network interface reset", "System boot completed", "New connection established",
            "Certificate renewed successfully", "Service configuration reloaded", "Authentication token issued", "User session ended", "Scheduled maintenance started",
            "Email sent successfully", "Data export completed", "Cloud instance started", "File download completed", "Scheduled report generated",
            "System diagnostics completed", "Service cache refreshed", "Access granted", "Load balancer updated", "Deployment completed",
            "Autoscaling event triggered", "Resource usage within limits", "Database optimization completed", "API key issued",
            "User preferences updated", "Security scan completed", "Monitoring alert resolved", "System shutdown completed",
            "Configuration backup created", "New firewall rule applied", "Filesystem check completed", "New admin user added",
            "Audit log entry recorded", "Server software version updated", "System performance check completed", "User logged out"
        ],
        "DEBUG": [
            "Executing query: SELECT * FROM users", "Cache miss for key: user:1234", "Request headers: {user-agent: Mozilla}", "Debugging session started",
            "Function process_data() called with args: (10, 20)", "Received API request: GET /status", "Retrying database connection...", "Memory allocation: 1024MB",
            "Executing background job ID: 42", "Processing batch 5 of 10", "Attempting to acquire lock on resource", "User input validation passed",
            "Generated token: abc123", "Connecting to external API: example.com", "Request timeout set to 30s", "Thread pool size adjusted to 8",
            "Reading configuration from /etc/config.yaml", "Temporary file created: /tmp/tmpfile1", "Performance benchmark started", "Heap size: 2048MB",
            "Loading user preferences from database", "Releasing lock on resource", "Generating session ID: xyz789", "Debug mode enabled",
            "Initializing new connection pool", "Response payload: true", "File read operation took 120ms", "Checking for software updates",
            "CPU utilization: 45%", "Attempting connection retry #3", "Clearing temporary cache", "API response status: 200 OK",
            "Setting environment variable: DEBUG=true", "Triggering log rotation", "Reading data chunk: 256KB", "Parsing JSON response",
            "Verifying SSL certificate", "Starting new transaction", "Rolling back transaction due to error", "HTTP request latency: 85ms",
            "Sending email notification", "Execution time: 5.3s", "Revalidating cache entry", "Checking for memory leaks", "Processing image upload",
            "Spawning new worker thread", "Compiling configuration settings", "Exiting debug mode", "Switching to maintenance mode"
        ]
    }
    
    print(generate_log_entry(log_messages))
