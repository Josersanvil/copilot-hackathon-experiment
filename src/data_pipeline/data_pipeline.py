import os
import json
from datetime import datetime
import polars as pl

# Directory containing the JSON files
data_dir = "../../random_phrase_ot_week"
output_json = "../../results/cleaned_slack_data.json"

all_records = []
# Store all data from all files first to handle cross-file threads
all_data = []

# Count files processed
files_processed = 0
total_messages = 0

for filename in os.listdir(data_dir):
    if filename.endswith(".json"):
        files_processed += 1
        print(f"Processing file {files_processed}: {filename}")
        with open(os.path.join(data_dir, filename), "r") as f:
            data = json.load(f)
            print(f"  - Found {len(data)} messages in {filename}")
            total_messages += len(data)
            all_data.extend(data)

print(f"\nTotal files processed: {files_processed}")
print(f"Total messages found: {total_messages}")

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
        
        # Output one row per message with aggregated reactions
        all_records.append({
            "message": message,
            "username": username,
            "datetime": datetime_str,
            "reaction_type": reaction_types if reaction_types else None,
            "number_of_reaction": total_reaction_count,
            "reply_count": reply_count
        })

# Save to a single pretty-printed JSON file
with open(output_json, "w") as f:
    json.dump(all_records, f, indent=2, ensure_ascii=False)
print(f"\nCleaned data written to {output_json}")
print(f"Total parent messages extracted: {len(all_records)}")
print(f"Processing complete!")
