import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl


def extract_chats_data(src_folder: Path, dst_path: Path) -> None:
    """
    Extract and clean chat data from JSON files in a source folder.

    Args:
        src_folder: Path to the directory containing JSON files
        dst_path: Path to the output JSON file where cleaned data will be saved
    """
    all_records = []
    # Store all data from all files first to handle cross-file threads
    all_data = []

    # Ensure the output directory exists
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    for filename in os.listdir(src_folder):
        if filename.endswith(".json"):
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
                }
            )

    # Save to a single pretty-printed JSON file
    with open(dst_path, "w") as f:
        json.dump(all_records, f, indent=2, ensure_ascii=False)
    print(f"Cleaned data written to {dst_path}")


def generate_enriched_data(chats_data_path: Path, dst_path: Path) -> None:
    """
    Generate enriched chat data with additional calculated fields.

    Args:
        chats_data_path: Path to the processed chat data JSON file
        dst_path: Path to the output enriched JSON file
    """
    # Load the processed chat data
    with open(chats_data_path) as f:
        chat_data = json.load(f)

    enriched_records = []

    for record in chat_data:
        # Extract mentioned users using regex
        mentioned_users = extract_mentioned_users(record.get("message", ""))

        # Parse datetime to calculate month and week
        datetime_str = record.get("datetime", "")
        if datetime_str:
            dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

            # Calculate month
            month = dt.strftime("%Y-%m")

            # Calculate week (Monday of the week)
            monday = dt - timedelta(days=dt.weekday())
            week = monday.strftime("%Y-%m-%d")
        else:
            month = None
            week = None

        # Create enriched record
        enriched_record = {
            **record,  # Include all original fields
            "mentioned_users": mentioned_users,
            "month": month,
            "week": week,
        }

        enriched_records.append(enriched_record)

    # Ensure the output directory exists
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    # Save enriched data
    with open(dst_path, "w") as f:
        json.dump(enriched_records, f, indent=2, ensure_ascii=False)
    print(f"Enriched data written to {dst_path}")


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
