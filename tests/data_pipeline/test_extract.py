import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from data_pipeline.extract import (
    _is_within_date_range,
    add_humor_scores_to_existing_data,
    extract_chats_data,
    extract_mentioned_users,
)

CHATS_DATA_FOLDER = Path(__file__).parent.parent.parent / "chats" / "test_chat"
CHAT_RAW_FOLDER = CHATS_DATA_FOLDER / "raw"
CHAT_OUTPUT_FOLDER = CHATS_DATA_FOLDER / "processed"


def test_extract_chats_data():
    # Create output directory if it doesn't exist
    CHAT_OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    # Call the function with folder (src) and file path (dst)
    extract_chats_data(CHAT_RAW_FOLDER, CHAT_OUTPUT_FOLDER / "chat.json")
    assert (CHAT_OUTPUT_FOLDER / "chat.json").exists()

    # Verify that the extracted data includes enriched fields
    with open(CHAT_OUTPUT_FOLDER / "chat.json") as f:
        data = json.load(f)

    # Should have at least one record
    assert len(data) > 0

    # Check that enriched fields are present
    first_record = data[0]
    assert "mentioned_users" in first_record
    assert "month" in first_record
    assert "week" in first_record
    assert "user_id" in first_record

    # Verify the format of enriched fields
    if first_record["mentioned_users"] is not None:
        assert isinstance(first_record["mentioned_users"], list)
    assert isinstance(first_record["month"], str)
    assert isinstance(first_record["week"], str)

    # Verify date formats
    assert len(first_record["month"]) == 7  # YYYY-MM format
    assert len(first_record["week"]) == 10  # YYYY-MM-DD format


def test_extract_mentioned_users():
    """Test the extract_mentioned_users function with various message formats."""

    # Test message with single mention
    message_single = "Hello <@U123456789>, how are you?"
    result = extract_mentioned_users(message_single)
    assert result == ["U123456789"]

    # Test message with multiple mentions
    message_multiple = '"How should I put it... do you want to go to heaven?!" <@U987654321> to <@U555666777>'
    result = extract_mentioned_users(message_multiple)
    assert result == ["U987654321", "U555666777"]

    # Test message with no mentions
    message_no_mentions = "This is a regular message with no mentions"
    result = extract_mentioned_users(message_no_mentions)
    assert result is None

    # Test empty message
    result = extract_mentioned_users("")
    assert result is None

    # Test None message
    result = extract_mentioned_users(None)
    assert result is None

    # Test message with mixed content and mentions
    message_mixed = "Check out <@U111222333> and also see what <@U444555666> thinks about this!"
    result = extract_mentioned_users(message_mixed)
    assert result == ["U111222333", "U444555666"]

    # Test message with mention-like text but not actual mentions
    message_fake = "This looks like <@notreal> but isn't a valid mention"
    result = extract_mentioned_users(message_fake)
    assert result is None


def test_extract_chats_data_with_humor_scores():
    """Test that extract_chats_data can calculate humor scores when requested."""
    # Create output directory if it doesn't exist
    CHAT_OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    # Mock the humor score function to return predictable values
    with patch("data_pipeline.extract.get_humor_score_for_message", return_value=7):
        extract_chats_data(CHAT_RAW_FOLDER, CHAT_OUTPUT_FOLDER / "chat_with_scores.json", calculate_humor_scores=True)

    assert (CHAT_OUTPUT_FOLDER / "chat_with_scores.json").exists()

    # Verify that the humor scores are included
    with open(CHAT_OUTPUT_FOLDER / "chat_with_scores.json") as f:
        data = json.load(f)

    # Should have at least one record
    assert len(data) > 0

    # Check that humor score field is present
    first_record = data[0]
    assert "quality_score_from_llm" in first_record

    # If the message is not empty, it should have a score
    if first_record["message"].strip():
        assert first_record["quality_score_from_llm"] == 7
    else:
        assert first_record["quality_score_from_llm"] is None


def test_extract_chats_data_without_humor_scores():
    """Test that extract_chats_data works without calculating humor scores (default behavior)."""
    # Create output directory if it doesn't exist
    CHAT_OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    extract_chats_data(CHAT_RAW_FOLDER, CHAT_OUTPUT_FOLDER / "chat_no_scores.json")

    assert (CHAT_OUTPUT_FOLDER / "chat_no_scores.json").exists()

    # Verify that humor scores are None when not calculated
    with open(CHAT_OUTPUT_FOLDER / "chat_no_scores.json") as f:
        data = json.load(f)

    # Should have at least one record
    assert len(data) > 0

    # Check that humor score field is present but None
    first_record = data[0]
    assert "quality_score_from_llm" in first_record
    assert first_record["quality_score_from_llm"] is None


def test_add_humor_scores_to_existing_data():
    """Test adding humor scores to existing processed data."""
    # Create a temporary file with test data
    test_data = [
        {
            "message_id": "123456789.123456",
            "user_id": "U123456789",
            "message": "This is a funny test message",
            "username": "Test User",
            "datetime": "2024-01-01 12:00:00",
            "reaction_type": None,
            "number_of_reaction": 0,
            "reply_count": 0,
            "mentioned_users": None,
            "month": "2024-01",
            "week": "2024-01-01",
        },
        {
            "message_id": "123456789.123457",
            "user_id": "U123456790",
            "message": "",  # Empty message should not get a score
            "username": "Another User",
            "datetime": "2024-01-01 12:01:00",
            "reaction_type": None,
            "number_of_reaction": 0,
            "reply_count": 0,
            "mentioned_users": None,
            "month": "2024-01",
            "week": "2024-01-01",
        },
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(test_data, f, indent=2)
        temp_path = Path(f.name)

    try:
        # Mock the humor scoring function and print to avoid output during tests
        with patch("data_pipeline.extract.get_humor_score_for_message", return_value=8), patch("builtins.print"):
            add_humor_scores_to_existing_data(temp_path)

        # Read the updated data
        with open(temp_path) as f:
            updated_data = json.load(f)

        # Check that humor scores were added appropriately
        assert len(updated_data) == 2

        # First message should have a score
        assert updated_data[0]["quality_score_from_llm"] == 8

        # Second message (empty) should not have a score
        assert "quality_score_from_llm" not in updated_data[1] or updated_data[1]["quality_score_from_llm"] is None

    finally:
        # Clean up the temporary file
        temp_path.unlink()


def test_add_humor_scores_skips_existing_scores():
    """Test that add_humor_scores_to_existing_data skips messages that already have scores."""
    # Create test data with one message already having a score
    test_data = [
        {
            "message_id": "123456789.123456",
            "user_id": "U123456789",
            "message": "This message already has a score",
            "username": "Test User",
            "datetime": "2024-01-01 12:00:00",
            "reaction_type": None,
            "number_of_reaction": 0,
            "reply_count": 0,
            "mentioned_users": None,
            "month": "2024-01",
            "week": "2024-01-01",
            "quality_score_from_llm": 9,  # Already has a score
        },
        {
            "message_id": "123456789.123457",
            "user_id": "U123456790",
            "message": "This message needs a score",
            "username": "Another User",
            "datetime": "2024-01-01 12:01:00",
            "reaction_type": None,
            "number_of_reaction": 0,
            "reply_count": 0,
            "mentioned_users": None,
            "month": "2024-01",
            "week": "2024-01-01",
        },
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(test_data, f, indent=2)
        temp_path = Path(f.name)

    try:
        # Mock the humor scoring function and print to avoid output during tests
        with (
            patch("data_pipeline.extract.get_humor_score_for_message", return_value=7) as mock_score,
            patch("builtins.print"),
        ):
            add_humor_scores_to_existing_data(temp_path)

        # The scoring function should only be called once (for the message without a score)
        assert mock_score.call_count == 1

        # Read the updated data
        with open(temp_path) as f:
            updated_data = json.load(f)

        # Check that the existing score wasn't changed and new score was added
        assert updated_data[0]["quality_score_from_llm"] == 9  # Original score preserved
        assert updated_data[1]["quality_score_from_llm"] == 7  # New score added

    finally:
        # Clean up the temporary file
        temp_path.unlink()


def test_is_within_date_range():
    """Test the date range filtering helper function."""
    test_date = datetime(2024, 6, 15)

    # No date range specified - should include all
    assert _is_within_date_range(test_date, None, None) == True

    # Start date only
    assert _is_within_date_range(test_date, "2024-06-01", None) == True
    assert _is_within_date_range(test_date, "2024-06-15", None) == True
    assert _is_within_date_range(test_date, "2024-06-20", None) == False

    # End date only
    assert _is_within_date_range(test_date, None, "2024-06-20") == True
    assert _is_within_date_range(test_date, None, "2024-06-15") == True
    assert _is_within_date_range(test_date, None, "2024-06-10") == False

    # Both start and end dates
    assert _is_within_date_range(test_date, "2024-06-10", "2024-06-20") == True
    assert _is_within_date_range(test_date, "2024-06-15", "2024-06-15") == True
    assert _is_within_date_range(test_date, "2024-06-16", "2024-06-20") == False
    assert _is_within_date_range(test_date, "2024-06-10", "2024-06-14") == False


def test_add_humor_scores_with_date_filtering():
    """Test adding humor scores with date filtering."""
    test_data = [
        {
            "message_id": "123.456",
            "user_id": "U123",
            "message": "Message from June 1st",
            "username": "Alice",
            "datetime": "2024-06-01 12:00:00",
            "quality_score_from_llm": None,
        },
        {
            "message_id": "123.457",
            "user_id": "U124",
            "message": "Message from June 15th",
            "username": "Bob",
            "datetime": "2024-06-15 12:00:00",
            "quality_score_from_llm": None,
        },
        {
            "message_id": "123.458",
            "user_id": "U125",
            "message": "Message from June 30th",
            "username": "Charlie",
            "datetime": "2024-06-30 12:00:00",
            "quality_score_from_llm": None,
        },
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(test_data, f, indent=2)
        temp_path = Path(f.name)

    try:
        # Test with date range that should only include middle message
        with (
            patch("data_pipeline.extract.get_humor_score_for_message", return_value=8) as mock_score,
            patch("builtins.print"),
        ):
            add_humor_scores_to_existing_data(
                temp_path,
                humor_score_start_date="2024-06-10",
                humor_score_end_date="2024-06-20",
                max_workers=1,  # Use single worker for predictable testing
            )

        # Should only score one message (the middle one)
        assert mock_score.call_count == 1

        # Read the updated data
        with open(temp_path) as f:
            updated_data = json.load(f)

        # Check that only the middle message got scored
        assert updated_data[0]["quality_score_from_llm"] is None  # June 1st - outside range
        assert updated_data[1]["quality_score_from_llm"] == 8  # June 15th - within range
        assert updated_data[2]["quality_score_from_llm"] is None  # June 30th - outside range

    finally:
        temp_path.unlink()


def test_add_humor_scores_threading():
    """Test that multithreading works correctly."""
    # Create several messages that need scoring
    test_data = []
    for i in range(5):
        test_data.append(
            {
                "message_id": f"123.{i}",
                "user_id": f"U{i}",
                "message": f"Test message {i}",
                "username": f"User{i}",
                "datetime": "2024-06-15 12:00:00",
                "quality_score_from_llm": None,
            }
        )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(test_data, f, indent=2)
        temp_path = Path(f.name)

    try:
        # Mock scoring function with small delay to test threading
        def mock_score_with_delay(message, username):
            import time

            time.sleep(0.1)  # Small delay to simulate LLM call
            return 6

        with (
            patch("data_pipeline.extract.get_humor_score_for_message", side_effect=mock_score_with_delay) as mock_score,
            patch("builtins.print"),
        ):
            # Use 2 workers for threading test
            add_humor_scores_to_existing_data(temp_path, max_workers=2)

        # All messages should be scored
        assert mock_score.call_count == 5

        # Read the updated data
        with open(temp_path) as f:
            updated_data = json.load(f)

        # All messages should have scores
        for record in updated_data:
            assert record["quality_score_from_llm"] == 6

    finally:
        temp_path.unlink()
