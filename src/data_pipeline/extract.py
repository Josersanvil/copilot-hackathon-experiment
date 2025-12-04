import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from .utils import get_humor_score_for_message


def extract_chats_data(src_folder: Path, dst_path: Path, calculate_humor_scores: bool = False) -> None:
    """
    Extract and clean chat data from JSON files in a source folder.

    Args:
        src_folder: Path to the directory containing JSON files
        dst_path: Path to the output JSON file where cleaned data will be saved
        calculate_humor_scores: Whether to calculate humor scores using LLM (default: False)
    """
    all_records = []
    # Store all data from all files first to handle cross-file threads
    all_data = []

    # Ensure the output directory exists
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    # Count and process JSON files
    json_files = [f for f in os.listdir(src_folder) if f.endswith(".json")]
    total_files = len(json_files)
    print(f"Found {total_files} JSON files to process")

    for i, filename in enumerate(json_files, 1):
        print(f"Processing file {i}/{total_files}: {filename}")
        with open(src_folder / filename) as f:
            data = json.load(f)
            all_data.extend(data)

    # Build a lookup for replies across all files
    thread_lookup = {}
    for entry in all_data:
        thread_ts = entry.get("thread_ts")
        ts = entry.get("ts")
        # Only treat as reply if thread_ts exists and is different from ts
        if thread_ts and ts != thread_ts:
            thread_lookup.setdefault(thread_ts, []).append(entry)

    # Process all entries
    for entry in all_data:
        ts = entry.get("ts")
        thread_ts = entry.get("thread_ts")
        # Only process parent messages (where ts == thread_ts or thread_ts missing)
        if not thread_ts or ts == thread_ts:
            message = entry.get("text", "")
            username = entry.get("user_profile", {}).get("real_name", "")
            user_id = entry.get("user", "")
            # Convert timestamp to datetime
            timestamp = float(ts)
            dt = datetime.fromtimestamp(timestamp)
            datetime_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            reactions = entry.get("reactions", [])

            # Use Slack's built-in reply_count field if available
            reply_count = entry.get("reply_count", 0)

            # Aggregate all reactions for this message
            reaction_types = []
            total_reaction_count = 0
            if reactions:
                for reaction in reactions:
                    reaction_types.append(reaction.get("name", ""))
                    total_reaction_count += reaction.get("count", 0)

            # Calculate enriched fields
            mentioned_users = extract_mentioned_users(message)
            month = dt.strftime("%Y-%m")
            monday = dt - timedelta(days=dt.weekday())
            week = monday.strftime("%Y-%m-%d")

            # Calculate humor score if requested
            quality_score_from_llm = None
            if calculate_humor_scores and message.strip():
                print(f"Calculating humor score for message: {message[:50]}...")
                quality_score_from_llm = get_humor_score_for_message(message, username)

            # Output one row per message with aggregated reactions and enriched data
            all_records.append(
                {
                    "message_id": ts,
                    "user_id": user_id,
                    "message": message,
                    "username": username,
                    "datetime": datetime_str,
                    "reaction_type": reaction_types if reaction_types else None,
                    "number_of_reaction": total_reaction_count,
                    "reply_count": reply_count,
                    "mentioned_users": mentioned_users,
                    "month": month,
                    "week": week,
                    "quality_score_from_llm": quality_score_from_llm,
                }
            )

    # Save to a single pretty-printed JSON file
    with open(dst_path, "w") as f:
        json.dump(all_records, f, indent=2, ensure_ascii=False)
    print(f"Cleaned data written to {dst_path}")


def add_humor_scores_to_existing_data(json_file_path: Path) -> None:
    """
    Add humor scores to existing processed chat data.

    Args:
        json_file_path: Path to the processed chat JSON file
    """
    print(f"Loading existing data from {json_file_path}")

    with open(json_file_path) as f:
        data = json.load(f)

    total_messages = len(data)
    print(f"Found {total_messages} messages to process")

    for i, record in enumerate(data, 1):
        message = record.get("message", "")
        username = record.get("username", "")

        # Skip if already has a score or no message content
        if record.get("quality_score_from_llm") is not None or not message.strip():
            continue

        print(f"Processing message {i}/{total_messages}: {message[:50]}...")
        score = get_humor_score_for_message(message, username)
        record["quality_score_from_llm"] = score

    # Save updated data
    print(f"Saving updated data to {json_file_path}")
    with open(json_file_path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print("Humor scores added successfully!")


def extract_mentioned_users(message: str) -> list[str] | None:
    """
    Extract usernames that were mentioned in a message.

    Args:
        message: The message text

    Returns:
        List of mentioned usernames or None if no mentions found
    """
    if not message:
        return None

    # Pattern to match Slack user mentions: <@U123456789>
    user_mention_pattern = r"<@U[A-Z0-9]+>"
    mentions = re.findall(user_mention_pattern, message)

    if not mentions:
        return None

    # Extract just the user IDs (remove <@ and >)
    user_ids = [mention[2:-1] for mention in mentions]

    # For now, return the user IDs as the usernames
    # In a real scenario, you'd want to map these to actual usernames
    # from a user lookup table or user_profile data
    return user_ids
