#!/usr/bin/env python3

import collections
import argparse
import sys
import asyncio


class LogBufferFlusher:
    def __init__(self, interval, max_buffer_size, max_storage_length):
        self.buffer = collections.deque(maxlen=max_storage_length)

        self.buffer_size = 0
        self.interval = interval
        self.max_buffer_size = max_buffer_size
        self.num_of_lines_to_flush = 0
        self.eof = False

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


def run_logger_buffer_flusher(interval, max_buffer_size, max_storage_length):
    logger_buffer_flusher = LogBufferFlusher(
        interval, max_buffer_size, max_storage_length
    )

    try:
        logger_buffer_flusher.buffer_and_flush_logs()
    except KeyboardInterrupt:
        pass
    finally:
        logger_buffer_flusher.close()


if __name__ == "__main__":
    # Parse command-line options
    parser = argparse.ArgumentParser(
        description="Buffers a log stream provided as stdin until a max size or "
        "timeout treshold has been reached, then outputs it"
    )
    parser.add_argument(
        "--max-buffer-size",
        type=int,
        help="max buffer size (in bytes)",
        default=400,
    )
    parser.add_argument(
        "--max-storage-length",
        type=int,
        help="max amount of lines to store",
        default=1000,
    )
    parser.add_argument(
        "--interval", type=int, help="flush interval (in seconds)", default=3
    )

    args = parser.parse_args()
    run_logger_buffer_flusher(
        args.interval, args.max_buffer_size, args.max_storage_length
    )
