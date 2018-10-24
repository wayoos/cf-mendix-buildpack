#!/usr/bin/env python3

import _thread
import asyncio
import collections
import json
import logging
import os
import sys
import threading

import requests

log = logging.getLogger(__name__)
logging.basicConfig()
log.setLevel(logging.DEBUG)
# TODO: check if using the default logger works or if we need to import the
# logger from m2ee as in lib/metrics.py


class Ringo:
    def __init__(self, target_url, input_filename, **kwargs):
        self.target_url = target_url
        self.input_filename = input_filename
        self.kwargs = kwargs

    def run(self):
        log.info("Hello from %s", sys._getframe().f_code.co_name)
        self.logs_server_emitter_thread = LogsServerEmitterThread(
            self.target_url
        )
        self.logs_server_emitter_thread.daemon = True
        self.logs_server_emitter_thread.start()

        log.info("Hello from %s", sys._getframe().f_code.co_name)
        self.log_buffer_flusher_thread = LogBufferFlusherThread(
            filename=self.input_filename,
            flush_callable=self.logs_server_emitter_thread.logs_server_emitter.add_to_buffer,
        )
        self.log_buffer_flusher_thread.daemon = True
        self.log_buffer_flusher_thread.start()
        log.info("returning yo")

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
            self.log_buffer_flusher.buffer_and_flush_logs()
        except Exception as e:
            log.critical(
                "Unhandled failure in log buffer flusher, panicking.",
                exc_info=True,
            )
            _thread.interrupt_main()
        finally:
            self.log_buffer_flusher.close()


class LogsServerEmitter:
    def __init__(self, target_url):
        """Inside this class, we want to have a ring buffer storing lines that
        haven't yet been sent to the logs-storage-server.

        We want to cap memory usage of this at some arbitrary limit, say
        100mb. This (roughly) equates to 400,000 log lines, assuming an average
        size of 256 byes / line.
        """
        self._target_url = target_url

        self._buffer = collections.deque()
        self._buffer_size = 0
        self._chunk_size = 1000  # TODO: don't hardcode
        self._num_of_lines_to_flush = 0
        self._loop = None

    def stop(self):
        # TODO: remove?
        self.loop.close()

    def run(self):
        log.info("Hello from %s", sys._getframe().f_code.co_name)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.loop.call_later(1, self._flush_buffer)
        self.loop.run_forever()

    def add_to_buffer(self, line):
        log.info("Hello from %s", sys._getframe().f_code.co_name)
        self._buffer.append(line)
        # We assume only ASCII chars; since this is probably faster than
        # encoding to UTF-8 and checking bytes. If someone logs only in
        # Chinese, then they will use more memory than desired, yolo.
        self._buffer_size += len(line)
        # TODO: threadsafe counter?
        # TODO: do we even need the counter?
        self._num_of_lines_to_flush += 1

    def _flush_buffer(self):
        log.info("Hello from %s", sys._getframe().f_code.co_name)
        if len(self._buffer) > 0:
            if self._num_of_lines_to_flush > self._chunk_size:
                # If there are still messages left, and
                flush_up_to = self._chunk_size
            else:
                flush_up_to = self._num_of_lines_to_flush
            lines = []
            for x in range(0, flush_up_to):
                line = self._buffer.popleft()
                lines.append(line)
                self._buffer_size -= len(line)
                self._num_of_lines_to_flush -= 1
            self._emit(lines)
            # TODO: fallback logic
        else:
            return

        #  schedule a new run for INTERVAL time?

    def _emit(self, lines):
        logger.info("Hello from %s", sys._getframe().f_code.co_name)
        # TODO: make this async (or a future)?
        # TODO: split each line into a dict of timestamp and line (but where?)
        dict_to_post = {"log_lines": lines}
        json_lines = json.dumps(dict_to_post)
        try:
            response = requests.post(
                self.target_url, json=json_lines, timeout=10
            )
        except Exception as e:
            log.info("Failed to send metrics to trends server.", exc_info=True)
            # Do buffering here
        if response.status_code == 200:
            return

        log.info(
            "Posting logs to logs storage server failed. Got status code %s "
            "for URL %s, with body %s.",
            response.status_code,
            self.target_url,
            response.text,
        )
        # Do buffering here


class LogBufferFlusher:
    def __init__(
        self,
        filename=None,
        flush_callable=sys.stdout.write,
        interval=1,
        max_buffer_size=1000 ** 2,
        max_storage_length=1000 ** 2,
        chunk_size=1000,
    ):
        self.buffer = collections.deque(maxlen=max_storage_length)
        if filename:
            self.input_file_object = os.fdopen(
                os.open(filename, os.O_RDONLY | os.O_NONBLOCK)
            )
        else:
            self.input_file_object = sys.stdin

        self.buffer_size = 0
        self.interval = interval
        self.max_buffer_size = max_buffer_size
        self.flush_callable = flush_callable
        self.num_of_lines_to_flush = 0
        self.eof = False
        self.chunk_size = 1000

    def flush_buffer(self):
        log.info("Hello from LogFufferFlusher flush buffer")
        if self.num_of_lines_to_flush > 0:
            line = self.buffer.popleft()
            self.flush_callable(line)
            self.buffer_size -= len(line)
            self.num_of_lines_to_flush -= 1
        else:
            if self.flush_callable == sys.stdout.write:
                log.warning("lalala")
                self.loop.remove_writer(sys.stdout.fileno())
            self.loop.remove_writer(sys.stdout.fileno())

            if self.eof:
                self.loop.stop()

    def schedule_flush_buffer(self):
        self.num_of_lines_to_flush = len(self.buffer)
        self.loop.add_writer(sys.stdout.fileno(), self.flush_buffer)

    def buffer_loglines(self):
        log.info("Hello from %s", sys._getframe().f_code.co_name)
        while True:
            line = self.input_file_object.readline()
            if line:
                log.info("buffering line %s", line)
                self.buffer.append(line)
                self.buffer_size += len(line)

                if self.buffer_size > self.max_buffer_size:
                    self.schedule_flush_buffer()
            else:
                log.warning("EOF?")
                self.eof = True
                return

    def timeout_flush_buffer(self):
        self.schedule_flush_buffer()
        self.loop.call_later(self.interval, self.timeout_flush_buffer)

    def buffer_and_flush_logs(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

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
    except Exception as e:
        log.critical(
            "Unhandled failure in log buffer flusher, panicking.",
            exc_info=True,
        )
        _thread.interrupt_main()
    finally:
        logger_buffer_flusher.close()
