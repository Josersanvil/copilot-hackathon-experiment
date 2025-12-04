import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from data_pipeline.extract import add_humor_scores_to_existing_data, extract_chats_data, extract_mentioned_users

CHATS_DATA_FOLDER = Path(__file__).parent.parent.parent / "chats" / "test_chat"
CHAT_RAW_FOLDER = CHATS_DATA_FOLDER / "raw"
CHAT_OUTPUT_FOLDER = CHATS_DATA_FOLDER / "processed"


def test_extract_chats_data():
    # Create output directory if it doesn't exist
    CHAT_OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    # Call the function with folder (src) and file path (dst)
    extract_chats_data(CHAT_RAW_FOLDER, CHAT_OUTPUT_FOLDER / "chat.json", calculate_humor_scores=True)
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
