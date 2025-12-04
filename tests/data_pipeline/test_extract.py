import json
import tempfile
from pathlib import Path

from data_pipeline.extract import extract_chats_data, extract_mentioned_users, generate_enriched_data

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


def test_generate_enriched_data():
    """Test the generate_enriched_data function."""

    # Create sample chat data
    sample_data = [
        {
            "message_id": "1680600936.358669",
            "message": '"How should I put it... do you want to go to heaven?!" <@U987654321> to <@U555666777>',
            "username": "John Doe",
            "datetime": "2023-04-04 11:35:36",
            "reaction_type": ["joy"],
            "number_of_reaction": 5,
            "reply_count": 1,
        },
        {
            "message_id": "1680687336.123456",
            "message": "Regular message with no mentions",
            "username": "Alice Johnson",
            "datetime": "2023-04-05 11:35:36",
            "reaction_type": ["laughing"],
            "number_of_reaction": 3,
            "reply_count": 0,
        },
    ]

    # Create temporary files
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as input_file:
        json.dump(sample_data, input_file, indent=2)
        input_path = Path(input_file.name)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as output_file:
        output_path = Path(output_file.name)

    try:
        # Call the function
        generate_enriched_data(input_path, output_path)

        # Verify output file exists
        assert output_path.exists()

        # Load and verify the enriched data
        with open(output_path) as f:
            enriched_data = json.load(f)

        assert len(enriched_data) == 2

        # Test first record (with mentions)
        first_record = enriched_data[0]
        assert first_record["message_id"] == "1680600936.358669"
        assert first_record["mentioned_users"] == ["U987654321", "U555666777"]
        assert first_record["month"] == "2023-04"
        assert first_record["week"] == "2023-04-03"  # Monday of that week

        # Verify all original fields are preserved
        assert first_record["username"] == "John Doe"
        assert first_record["datetime"] == "2023-04-04 11:35:36"
        assert first_record["reaction_type"] == ["joy"]
        assert first_record["number_of_reaction"] == 5
        assert first_record["reply_count"] == 1

        # Test second record (no mentions)
        second_record = enriched_data[1]
        assert second_record["message_id"] == "1680687336.123456"
        assert second_record["mentioned_users"] is None
        assert second_record["month"] == "2023-04"
        assert second_record["week"] == "2023-04-03"  # Same week as first message

        # Verify all original fields are preserved
        assert second_record["username"] == "Alice Johnson"
        assert second_record["datetime"] == "2023-04-05 11:35:36"

    finally:
        # Clean up temporary files
        input_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)


def test_generate_enriched_data_with_edge_cases():
    """Test the generate_enriched_data function with edge cases."""

    # Test with record that has missing datetime
    sample_data = [
        {
            "message_id": "1680600936.358669",
            "message": "Test message <@U123456789>",
            "username": "Test User",
            "datetime": "",  # Empty datetime
            "reaction_type": None,
            "number_of_reaction": 0,
            "reply_count": 0,
        }
    ]

    # Create temporary files
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as input_file:
        json.dump(sample_data, input_file, indent=2)
        input_path = Path(input_file.name)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as output_file:
        output_path = Path(output_file.name)

    try:
        # Call the function
        generate_enriched_data(input_path, output_path)

        # Load and verify the enriched data
        with open(output_path) as f:
            enriched_data = json.load(f)

        assert len(enriched_data) == 1
        record = enriched_data[0]

        # Should have mentioned users but null month/week due to empty datetime
        assert record["mentioned_users"] == ["U123456789"]
        assert record["month"] is None
        assert record["week"] is None

    finally:
        # Clean up temporary files
        input_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)
