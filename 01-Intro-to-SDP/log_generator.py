import json
import os
import random
import uuid
from datetime import datetime, timedelta


def generate_mock_web_logs(
    output_dir: str,
    num_files: int = 5,
    records_per_file: int = 100
):
    """
    Generates mock JSON log files simulating web traffic logs.
    Each file will contain a CloudWatch-like JSON structure with log events.

    :param output_dir: Directory where JSON files will be saved.
    :param num_files: Number of files to generate.
    :param records_per_file: Number of log records per file.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Example data pools
    pages = ["/", "/products", "/products/123", "/products/345", "/checkout", "/about"]
    methods = ["GET", "POST"]
    statuses = [200, 200, 200, 404, 500]  # weighted toward 200
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X)"
    ]
    ip_blocks = ["192.168.1.", "10.0.0.", "172.16.0."]

    for f in range(num_files):
        log_events = []
        start_time = datetime.now() - timedelta(minutes=10)

        for i in range(records_per_file):
            event_time = start_time + timedelta(seconds=i * 5)  # spaced logs
            log_events.append({
                "id": str(uuid.uuid4()),
                "timestamp": int(event_time.timestamp() * 1000),
                "message": {
                    "ip": random.choice(ip_blocks) + str(random.randint(1, 254)),
                    "method": random.choice(methods),
                    "page": random.choice(pages),
                    "status": random.choice(statuses),
                    "user_agent": random.choice(user_agents),
                    "response_time_ms": random.randint(20, 1200)
                }
            })

        log_data = {
            "owner": str(uuid.uuid4()),
            "logGroup": "/aws/website/ecommerce",
            "logStream": f"web-traffic-{f}",
            "logEvents": log_events
        }

        file_path = os.path.join(output_dir, f"web_logs_{f}.json")
        with open(file_path, "w") as f_out:
            json.dump(log_data, f_out, indent=2)

if __name__ == '__main__':
    generate_mock_web_logs()