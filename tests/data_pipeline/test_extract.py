import json
import tempfile
from pathlib import Path

from data_pipeline.extract import extract_chats_data, extract_mentioned_users

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
