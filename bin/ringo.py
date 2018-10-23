#!/usr/bin/env python3

import argparse

# TODO maybe this breaks
from lib.ringo import run_logger_buffer_flusher

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
