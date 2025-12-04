# Humor Score Analysis

This module adds sentiment analysis capabilities to chat messages, specifically scoring how funny each message is on a scale from 1-10 using GitHub Copilot CLI.

## Features

- **LLM-powered humor scoring**: Uses GitHub Copilot CLI to analyze and score message humor
- **Flexible integration**: Can be used during initial extraction or added to existing data
- **Robust error handling**: Gracefully handles API failures with fallback scores
- **CLI and Python API**: Available through both command-line interface and Python functions

## Prerequisites

- GitHub Copilot CLI must be installed and available in your PATH
- Run `copilot --version` to verify installation

## Usage

### Adding humor scores during extraction

```bash
# Extract chat data with humor scores
uv run python -m src.data_pipeline.cli extract chats/raw/ chats/processed/chat.json --humor-scores
```

### Adding humor scores to existing data

```bash
# Add humor scores to already processed data
uv run python -m src.data_pipeline.cli add-humor chats/processed/chat.json
```

### Python API

```python
from src.data_pipeline.utils import get_humor_score_for_message
from src.data_pipeline.extract import add_humor_scores_to_existing_data

# Score a single message
score = get_humor_score_for_message("That's hilarious!", "John Doe")
print(f"Humor score: {score}/10")

# Add scores to existing dataset
add_humor_scores_to_existing_data(Path("chat.json"))
```

## How It Works

1. **Prompt Engineering**: Each message is sent to the LLM with context about the "random phrase of the week" Slack channel
2. **Score Extraction**: The LLM response is parsed to extract a numeric score (1-10)
3. **Error Handling**: If the LLM is unavailable or returns invalid responses, a default score of 5 is used
4. **Data Integration**: Scores are added as `quality_score_from_llm` field in the JSON data

## Humor Scoring Criteria

The LLM is instructed to use the following scale:
- **1-3**: Not funny, mundane, or purely informational
- **4-6**: Mildly amusing, decent workplace humor
- **7-8**: Quite funny, would make most people chuckle
- **9-10**: Hilarious, exceptional workplace humor

## Output Format

The humor score is added to each message record:

```json
{
  "message_id": "1721393093.255859",
  "user_id": "U04PBEP498B",
  "message": "That's just an Incident-as-Usual",
  "username": "Xander van den Berg",
  "datetime": "2024-07-19 14:44:53",
  "quality_score_from_llm": 7,
  ...
}
```

## Demo

Run the demonstration script to see the functionality in action:

```bash
uv run python demo_humor_scores.py
```

## Testing

Comprehensive tests are included for all functionality:

```bash
# Run tests for humor scoring utilities
uv run pytest tests/data_pipeline/test_utils.py -v

# Run tests for extraction with humor scores
uv run pytest tests/data_pipeline/test_extract.py -v

# Run all tests
uv run pytest
```

## Error Handling

The system gracefully handles various error conditions:

- **CLI not available**: Returns default score (5) and logs warning
- **Invalid LLM responses**: Attempts to parse various score formats, defaults to 5
- **Empty messages**: Skips scoring for empty or whitespace-only messages
- **Existing scores**: Skips messages that already have scores

## Performance Considerations

- Each message requires an LLM API call, so processing large datasets may take time
- Consider processing in batches or during off-peak hours
- The system includes progress indicators for long-running operations