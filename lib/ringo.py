#!/usr/bin/env python3

import _thread
import asyncio
import collections
import datetime
import json
import logging
import os
import sys
import threading

import requests

log = logging.getLogger(__name__)
# TODO: check if using the default logger works or if we need to import the
# logger from m2ee as in lib/metrics.py


class Ringo:
    def __init__(self, target_url, input_filename, **kwargs):
        self.target_url = target_url
        self.input_filename = input_filename
        self.kwargs = kwargs
        self.logs_server_emitter_thread = LogsServerEmitterThread(
            self.target_url
        )
        self.log_buffer_flusher_thread = LogBufferFlusherThread(
            filename=self.input_filename,
            flush_callable=self.logs_server_emitter_thread.logs_server_emitter.add_to_buffer,
        )

    def run(self):
        log.log(1, "Hello from %s", sys._getframe().f_code.co_name)
        self.logs_server_emitter_thread.daemon = True
        self.logs_server_emitter_thread.start()

        log.log(1, "Hello from %s", sys._getframe().f_code.co_name)
        self.log_buffer_flusher_thread.daemon = True
        self.log_buffer_flusher_thread.start()
        log.log(1, "returning yo")

    def stop(self):
        # TODO: remove
        self.logs_server_emitter_thread.stop()


class LogsServerEmitterThread(threading.Thread):
    def __init__(self, target_url):
        super().__init__()
        self.logs_server_emitter = LogsServerEmitter(target_url)

    def run(self):
        try:
            self.logs_server_emitter.run()
        except Exception as e:
            log.critical(
                "Unhandled failure in log server emitter, panicking.",
                exc_info=True,
            )
            _thread.interrupt_main()
        finally:
            # TODO: do we need a close?
            pass

    def stop(self):
        # TODO remove?
        self.logs_server_emitter.stop()


class LogBufferFlusherThread(threading.Thread):
    def __init__(self, filename, flush_callable):
        super().__init__()
        self.log_buffer_flusher = LogBufferFlusher(
            filename=filename, flush_callable=flush_callable
        )

    def run(self):
        try:
            self.log_buffer_flusher.run()
        except Exception as e:
            log.critical(
                "Unhandled failure in log buffer flusher, panicking.",
                exc_info=True,
            )
            _thread.interrupt_main()
        finally:
            log.warning("doei")


class LogsServerEmitter:
    def __init__(self, target_url):
        """Inside this class, we want to have a ring buffer storing lines that
        haven't yet been sent to the logs-storage-server.

        We want to cap memory usage of this at some arbitrary limit, say
        100mb. This (roughly) equates to 400,000 log lines, assuming an average
        size of 256 byes / line.
        """
        # TODO: make all this shit configurable
        self.max_buffer_size = 100 * 1000 * 1000  # 100mb ish
        self._target_url = target_url

        self._buffer = collections.deque()
        self._buffer_size = 0
        self._chunk_size = 1000  # TODO: don't hardcode
        self._loop = None

    def stop(self):
        # TODO: remove?
        self.loop.close()

    def run(self):
        log.log(1, "Hello from %s", sys._getframe().f_code.co_name)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.loop.call_later(1, self._flush_buffer)
        self.loop.run_forever()

    def add_to_buffer(self, line):
        log.log(1, "Hello from %s", sys._getframe().f_code.co_name)
        if len(line) > self.max_buffer_size:
            log.warning(
                "MENDIX LOGGING FRAMEWORK: "
                "Gigantic line with length %s chars was added. This is larger "
                "than max buffer size %s. Discarding.",
                len(line),
                self.max_buffer_size,
            )
            return
        new_buffer_size = self._buffer_size + len(line)
        removed_count = 0

        if new_buffer_size >= self.max_buffer_size:
            while new_buffer_size >= self.max_buffer_size:
                removed_line = self._buffer.popleft()
                new_buffer_size -= len(removed_line)
                removed_count += 1
            log.info(
                "MENDIX LOGGING FRAMEWORK: "
                "Buffer was full with size %s. Removed %s lines to make space",
                new_buffer_size,
                removed_count,
            )

        self._buffer.append(line)
        # We assume only ASCII chars; since this is probably faster than
        # encoding to UTF-8 and checking bytes. If someone logs only in
        # Chinese, then they will use more memory than desired, yolo.
        self._buffer_size += len(line)
        log.log(
            1,
            "Added line to buffer. Items in buffer %s. Buffer length %s chars",
            len(self._buffer),
            self._buffer_size,
        )

    def _flush_buffer(self):
        log.log(1, "Hello from %s", sys._getframe().f_code.co_name)
        log.log(
            1, "Flushing buffer. Current items in buffer %s", len(self._buffer)
        )
        if len(self._buffer) > 0:
            if len(self._buffer) > self._chunk_size:
                # If there are still messages left, and
                flush_up_to = self._chunk_size
            else:
                flush_up_to = len(self._buffer)
            log.log(
                1,
                "We will flush %s lines. Total lines to flush is %s",
                flush_up_to,
                len(self._buffer),
            )
            lines = []
            for x in range(0, flush_up_to):
                line = self._buffer.popleft()
                lines.append(line)
            self._emit(lines)
            if len(self._buffer) > 0:
                log.log(
                    1, "Buffer is not yet empty. Calling flush buffer soon."
                )
                self.loop.call_soon(self._flush_buffer)
                return
        else:
            log.log(1, "Buffer is empty, nothing to do.")

        log.log(1, "Scheduling next flush call for 1s time.")
        # TODO: add an interval instead of hardcoding
        self.loop.call_later(1, self._flush_buffer)

    def _rebuffer_lines(self, lines):
        log.info("Rebuffering %s lines", len(lines))
        self._buffer.extendleft(lines)

    def _emit(self, lines):
        log.log(1, "Hello from %s", sys._getframe().f_code.co_name)
        # TODO: make this async (or a future)?
        # TODO: split each line into a dict of timestamp and line (but where?)
        dict_to_post = {"log_lines": lines}
        try:
            # TODO: configurable timeout
            log.log(
                1, "Posting to %s with body %s", self._target_url, dict_to_post
            )
            response = requests.post(
                self._target_url, json=dict_to_post, timeout=10
            )
        except Exception as e:
            log.debug("Failed to send metrics to logs server.", exc_info=True)
            self._rebuffer_lines(lines)

        if response.status_code == 200:
            return

        log.debug(
            "Posting logs to logs storage server failed. Got status code %s "
            "for URL %s, with body %s.",
            response.status_code,
            self._target_url,
            response.text,
        )
        self._rebuffer_lines(lines)


class LogBufferFlusher:
    def __init__(self, filename=None, flush_callable=sys.stdout.write):
        if filename:
            log.log(
                1, "Setting up log buffer flusher with filename %s", filename
            )
            self.input_file_object = os.fdopen(
                os.open(filename, os.O_RDONLY | os.O_NONBLOCK)
            )
        else:
            log.log(1, "Setting up log buffer flusher with stdin")
            self.input_file_object = sys.stdin

        self.flush_callable = flush_callable
        self.timestamp_length = len("2018-10-26 11:23:41.479")

    def buffer_loglines(self):
        log.log(1, "Hello from %s", sys._getframe().f_code.co_name)
        while True:
            line = self.input_file_object.readline()
            if line:
                try:
                    timestamp = line[0 : self.timestamp_length]
                    body = line[self.timestamp_length + 1 :]
                except IndexError as e:
                    log.warning(
                        "Failed to extract timestamp from log line, using current time instead",
                        exc_info=True,
                    )
                    timestamp = datetime.datetime.now().strftime(
                        "%Y-%m-%d %H-%M-%S.%f"
                    )[0 : self.timestamp_length]
                    body = line

                sys.stdout.write(body)
                log.log(1, "sending line to emitter %s", line)
                self.flush_callable({"timestamp": timestamp, "line": body})
            else:
                log.info("EOF - no more data should follow.")
                self.loop.remove_reader(self.input_file_object.fileno())
                return

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.add_reader(
            self.input_file_object.fileno(), self.buffer_loglines
        )
        self.loop.run_forever()
