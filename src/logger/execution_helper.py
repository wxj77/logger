import time
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import subprocess
import json

import logging
import logstash
import socket
import os
import threading
import queue

# Logstash host and port
LOGSTASH_HOST = os.getenv("LOGSTASH_HOST", '192.168.1.219')
LOGSTASH_PORT = os.getenv("LOGSTASH_PORT", 5000) # logger
LOGSTASH_PORT_2 = os.getenv("LOGSTASH_PORT", 5002) # execution_times

def create_logstash_socket(host, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        return sock
    except Exception as e:
        print(f"Error creating socket connection to Logstash: {e}")
        return None

# Function to get the IP address of the hostname
def get_ip_address():
    try:
        # Create a socket to determine the current IP address
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            # Connect to a public DNS server (Google's) to get the local IP
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception as e:
        # Fallback to localhost if there's an error
        return "127.0.0.1"
    
def get_hostname():
    return socket.gethostname()

ip_address = get_ip_address()
hostname = get_hostname()

class LogstashClient:
    """
    A client to send log messages to Logstash via a persistent TCP connection.
    """
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.queue = queue.Queue()
        self.running = True
        self.thread = threading.Thread(target=self._process_queue, daemon=True)
        self.thread.start()

    def send(self, log_entry):
        """
        Add a log entry to the queue for processing.
        """
        self.queue.put(log_entry)

    def _process_queue(self):
        """
        Process the queue and send log messages to Logstash.
        """
        while self.running or not self.queue.empty():
            try:
                log_entry = self.queue.get(timeout=1)  # Wait for log entries
                if log_entry is None:
                    continue
                
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((self.host, self.port))
                    sock.sendall(log_entry.encode('utf-8'))
            except queue.Empty:
                continue  # No log entries to process
            except Exception as e:
                print(f"Error sending log entry to Logstash: {e}, {log_entry}")

    def stop(self):
        """
        Stop the background thread and ensure all messages are processed.
        """
        self.running = False
        self.thread.join()

class AsyncLogstashHandler(logging.Handler):
    """
    Asynchronous logging handler that sends logs to a Logstash instance via TCP.
    """
    def __init__(self, host, port, timeout=5):
        super().__init__()
        self.logstash_client = LogstashClient(LOGSTASH_HOST, LOGSTASH_PORT)

    def emit(self, record):
        """
        Add the log record to the queue.
        """
        try:
            log_entry = self.format(record)
            # Send to Logstash via the client
            self.logstash_client.send(log_entry)
        except Exception as e:
            print(f"Error queuing log entry: {e}")


class CustomFormatter(logging.Formatter):
    def __init__(self, fmt):
        super().__init__(fmt)
        self.hostname = hostname
        self.ip_address = ip_address

    def format(self, record):
        record.hostname = self.hostname
        record.ip_address = self.ip_address
        return super().format(record)


# Configure the logger
def get_logger():
    logger = logging.getLogger("PythonLogger")
    logger.setLevel(logging.DEBUG)

    formatter = CustomFormatter(
        '{"message": "%(message)s", "level": "%(levelname)s", "timestamp": "%(asctime)s", "name": "%(name)s", "hostname": "%(hostname)s", "ip_address": "%(ip_address)s"}'
    )
    
    # Add the Logstash handler
    logstash_handler = AsyncLogstashHandler(LOGSTASH_HOST, LOGSTASH_PORT)
    logstash_handler.setFormatter(formatter)
    logger.addHandler(logstash_handler)

    # Add a console handler for debugging
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

logger = get_logger()

logstash_client_2 = LogstashClient(LOGSTASH_HOST, LOGSTASH_PORT_2)
def log_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            data = {
            "function": func.__name__,
            "timestamp": start_time,
            "hostname": hostname, 
            "ip_address": ip_address,
        }
            log_entry = json.dumps(data)
            logstash_client_2.send(log_entry)
        except Exception as e:
            print(f"Error sending message to Logstash: {e}, {log_entry}")
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        try: 
            data= {
            "function": func.__name__,
            "duration": duration,
            "start_time": start_time,
            "end_time": end_time,
        }
            log_entry = json.dumps(data)
            logstash_client_2.send(log_entry)
        except Exception as e:
            print(f"Error sending message to Logstash: {e}, {log_entry}")
        return result
    return wrapper

