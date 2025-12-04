#!/usr/bin/env python3
"""
Command-line interface for extracting and processing chat data.
"""

import argparse
from pathlib import Path

from .extract import extract_chats_data


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Extract and process chat data from Slack JSON exports")
    parser.add_argument("src_folder", type=Path, help="Path to the directory containing raw JSON chat files")
    parser.add_argument("dst_path", type=Path, help="Path to the output JSON file for processed chat data")

    args = parser.parse_args()

    # Validate input directory exists
    if not args.src_folder.exists():
        print(f"Error: Source folder '{args.src_folder}' does not exist")
        return 1

    if not args.src_folder.is_dir():
        print(f"Error: '{args.src_folder}' is not a directory")
        return 1

    try:
        extract_chats_data(args.src_folder, args.dst_path)
        return 0
    except Exception as e:
        print(f"Error processing chat data: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
