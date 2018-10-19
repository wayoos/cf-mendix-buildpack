#!/usr/bin/env python3

import asyncio
import collections
import logging
import json
import sys
import threading

import requests

from lib.buildpackutil import get_logs_storage_url

log = logging.getLogger(__name__)
# TODO: check if using the default logger works or if we need to import the
# logger from m2ee as in lib/metrics.py


def send_to_logs_storage_server(lines):
    # TODO: make this async?
    # TODO: buffering on failure (wrap this in a class?)
    # TODO: wrap this in a class which stores the logs-storage-server URL in
    # memory
    logs_url = get_logs_storage_url()
    # TODO: split each line into a dict of timestamp and line
    dict_to_post = {"log_lines": lines}
    json_lines = json.dumps(dict_to_post)
    try:
        response = requests.post(logs_url, json=json_lines, timeout=10)
    except Exception as e:
        log.debug("Failed to send metrics to trends server.", exc_info=True)
        # Do buffering here


class RingoThread(threading.Thread):
    def __init__(
        self,
        flush_callable,
        interval=1,
        max_buffer_size=1000 ** 2,
        max_storage_length=1000 ** 2,
        chunk_size=1000,
    ):
        self.interval = interval
        self.max_buffer_size = max_buffer_size
        self.max_storage_length = max_storage_length
        self.chunk_size = chunk_size
        self.flush_callable = flush_callable

    def run(self):
        run_logger_buffer_flusher(
            self.interval,
            self.max_buffer_size,
            self.max_storage_length,
            self.chunk_size,
            self.flush_callable,
        )


class LogBufferFlusher:
    def __init__(self, interval, max_buffer_size, max_storage_length):
        self.buffer = collections.deque(maxlen=max_storage_length)

        self.buffer_size = 0
        self.interval = interval
        self.max_buffer_size = max_buffer_size
        self.num_of_lines_to_flush = 0
        self.eof = False
        self.chunk_size = 1000

    def flush_buffer(self):
        if self.num_of_lines_to_flush > 0:
            line = self.buffer.popleft()
            sys.stdout.write(line)
            self.buffer_size -= len(line)
            self.num_of_lines_to_flush -= 1
        else:
            self.loop.remove_writer(sys.stdout.fileno())

            if self.eof:
                self.loop.stop()

    def schedule_flush_buffer(self):
        self.num_of_lines_to_flush = len(self.buffer)
        self.loop.add_writer(sys.stdout.fileno(), self.flush_buffer)

    def buffer_loglines(self):
        line = sys.stdin.readline()
        if line:
            self.buffer.append(line)
            self.buffer_size += len(line)

            if self.buffer_size > self.max_buffer_size:
                self.schedule_flush_buffer()
        else:
            self.eof = True

    def timeout_flush_buffer(self):
        self.schedule_flush_buffer()
        self.loop.call_later(self.interval, self.timeout_flush_buffer)

    def buffer_and_flush_logs(self):
        self.loop = asyncio.get_event_loop()

        # Add event handlers to main loop
        self.loop.add_reader(sys.stdin.fileno(), self.buffer_loglines)
        self.loop.call_later(self.interval, self.timeout_flush_buffer)

        # Run forever
        self.loop.run_forever()

    def close(self):
        self.loop.close()


class ChunkedLogBufferFlusher(LogBufferFlusher):
    def __init__(
        self,
        interval,
        max_buffer_size,
        max_storage_length,
        chunk_size,
        flush_callable,
    ):
        super().__init__(interval, max_buffer_size, max_storage_length)
        self.chunk_size = chunk_size
        self.flush_callable = flush_callable

    def flush_buffer(self):
        if self.num_of_lines_to_flush > 0:
            if self.num_of_lines_to_flush > self.chunk_size:
                flush_up_to = self.chunk_size
            else:
                flush_up_to = self.num_of_lines_to_flush
            lines = []
            for x in range(0, flush_up_to):
                line = self.buffer.popleft()
                lines.append(line)
                self.buffer_size -= len(line)
                self.num_of_lines_to_flush -= 1
            self.flush_callable(lines)
        else:
            self.loop.remove_writer(sys.stdout.fileno())
            if self.eof:
                self.loop.stop()


def run_logger_buffer_flusher(
    interval, max_buffer_size, max_storage_length, cls=LogBufferFlusher
):
    logger_buffer_flusher = cls(interval, max_buffer_size, max_storage_length)

    try:
        logger_buffer_flusher.buffer_and_flush_logs()
    except KeyboardInterrupt:
        pass
    finally:
        logger_buffer_flusher.close()
