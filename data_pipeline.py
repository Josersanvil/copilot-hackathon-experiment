import os
import json
from datetime import datetime

# Directory containing the JSON files
data_dir = "random_phrase_ot_week"
output_json = "results/cleaned_slack_data.json"

all_records = []
# Store all data from all files first to handle cross-file threads
all_data = []

for filename in os.listdir(data_dir):
    if filename.endswith(".json"):
        with open(os.path.join(data_dir, filename), "r") as f:
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
print(f"Cleaned data written to {output_json}")
