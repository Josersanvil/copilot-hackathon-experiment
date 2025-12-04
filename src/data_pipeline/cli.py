#!/usr/bin/env python3
"""
Command-line interface for extracting and processing chat data.
"""

import argparse
from pathlib import Path

from .extract import add_humor_scores_to_existing_data, extract_chats_data


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Extract and process chat data from Slack JSON exports")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Extract command
    extract_parser = subparsers.add_parser("extract", help="Extract chat data from raw JSON files")
    extract_parser.add_argument("src_folder", type=Path, help="Path to the directory containing raw JSON chat files")
    extract_parser.add_argument("dst_path", type=Path, help="Path to the output JSON file for processed chat data")
    extract_parser.add_argument(
        "--humor-scores", action="store_true", help="Calculate humor scores using LLM (requires GitHub Copilot CLI)"
    )

    # Add humor scores command
    humor_parser = subparsers.add_parser("add-humor", help="Add humor scores to existing processed chat data")
    humor_parser.add_argument("json_file", type=Path, help="Path to the processed chat JSON file")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 1

    try:
        if args.command == "extract":
            # Validate input directory exists
            if not args.src_folder.exists():
                print(f"Error: Source folder '{args.src_folder}' does not exist")
                return 1

            if not args.src_folder.is_dir():
                print(f"Error: '{args.src_folder}' is not a directory")
                return 1

            extract_chats_data(args.src_folder, args.dst_path, calculate_humor_scores=args.humor_scores)

        elif args.command == "add-humor":
            # Validate input file exists
            if not args.json_file.exists():
                print(f"Error: JSON file '{args.json_file}' does not exist")
                return 1

            if not args.json_file.is_file():
                print(f"Error: '{args.json_file}' is not a file")
                return 1

            add_humor_scores_to_existing_data(args.json_file)

        return 0
    except Exception as e:
        print(f"Error processing chat data: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
