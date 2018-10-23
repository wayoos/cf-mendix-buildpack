#!/usr/bin/env python3

import asyncio
import collections
import logging
import json
import sys
import threading

import requests

log = logging.getLogger(__name__)
# TODO: check if using the default logger works or if we need to import the
# logger from m2ee as in lib/metrics.py


class LogsServerEmitter:
    def __init__(self, target_url):
        """Inside this class, we want to have a ring buffer storing lines that
        haven't yet been sent to the logs-storage-server.

        We want to cap memory usage of this at some arbitrary limit, say
        100mb. This (roughly) equates to 400,000 log lines, assuming an average
        size of 256 byes / line.
        """
        self.target_url = target_url
        self.ring_buffer = collections.deque(maxlen=1)
        self.buffer_size = 0

    def add_to_buffer(self, lines):
        for line in lines:
            self.ring_buffer.append(lines)
            # We assume only ASCII chars; since this is probably faster than
            # encoding to UTF-8 and checking bytes. If someone logs only in
            # Chinese, then they will use more memory than desired, yolo.
            self.buffer_size.append(len(line))

    def emit(self, lines):
        # TODO: make this async?
        # TODO: buffering on failure (wrap this in a class?)
        # TODO: split each line into a dict of timestamp and line
        dict_to_post = {"log_lines": lines}
        json_lines = json.dumps(dict_to_post)
        try:
            response = requests.post(
                self.target_url, json=json_lines, timeout=10
            )
        except Exception as e:
            log.debug(
                "Failed to send metrics to trends server.", exc_info=True
            )
            # Do buffering here
        if response.status_code == 200:
            return

        log.debug(
            "Posting logs to logs storage server failed. Got status code %s "
            "for URL %s, with body %s.",
            response.status_code,
            self.target_url,
            response.text,
        )
        # Do buffering here


class RingoThread(threading.Thread):
    def __init__(
        self,
        filename,
        target_url,
        interval=1,
        max_buffer_size=1000 ** 2,
        max_storage_length=1000 ** 2,
        chunk_size=1000,
    ):
        self.interval = interval
        self.filename = filename
        self.max_buffer_size = max_buffer_size
        self.max_storage_length = max_storage_length
        self.chunk_size = chunk_size

        self.logs_emitter = LogsServerEmitter(target_url=target_url)

    def run(self):
        run_logger_buffer_flusher(
            self.interval,
            self.max_buffer_size,
            self.max_storage_length,
            self.filename,
            cls=ChunkedLogBufferFlusher,
            chunk_size=self.chunk_size,
            flush_callable=self.logs_emitter.emit,
        )
        # TODO: do the same for the emitter


class LogBufferFlusher:
    def __init__(
        self, interval, max_buffer_size, max_storage_length, filename=None
    ):
        self.buffer = collections.deque(maxlen=max_storage_length)
        if filename:
            self.input_file_object = open(filename, "r")
        else:
            self.input_file_object = sys.stdin

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
        line = self.input_file_object.readline()
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
        self.loop.add_reader(
            self.input_file_object.fileno(), self.buffer_loglines
        )
        self.loop.call_later(self.interval, self.timeout_flush_buffer)

        # Run forever
        self.loop.run_forever()

    def close(self):
        self.loop.close()
        # Might cause problems if we close stdin and then other things try and
        # read from it later. I don't know if this will ever happen, though.
        self.input_file_object.close()


class ChunkedLogBufferFlusher(LogBufferFlusher):
    def __init__(
        self,
        interval,
        max_buffer_size,
        max_storage_length,
        filename,
        chunk_size,
        flush_callable,
    ):
        super().__init__(
            interval, max_buffer_size, max_storage_length, filename
        )
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
            # TODO: I think we need to remove the dependency on stdout
            self.loop.remove_writer(sys.stdout.fileno())
            if self.eof:
                self.loop.stop()


def run_logger_buffer_flusher(
    interval,
    max_buffer_size,
    max_storage_length,
    filename,
    cls=LogBufferFlusher,
    **kwargs,
):
    logger_buffer_flusher = cls(
        interval, max_buffer_size, max_storage_length, filename, **kwargs
    )

    try:
        logger_buffer_flusher.buffer_and_flush_logs()
    except KeyboardInterrupt:
        pass
    finally:
        logger_buffer_flusher.close()
