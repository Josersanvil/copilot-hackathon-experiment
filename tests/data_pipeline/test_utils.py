import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.utils import extract_humor_score, get_humor_score_for_message, get_llm_response


class TestGetLLMResponse:
    """Test the get_llm_response function."""

    def test_get_llm_response_success(self):
        """Test successful CLI call."""
        mock_result = MagicMock()
        mock_result.stdout = "Score: 7\n"
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            result = get_llm_response("Test prompt")

            assert result == "Score: 7"
            mock_run.assert_called_once_with(
                ["copilot", "-p", "Test prompt"], capture_output=True, text=True, check=True
            )

    def test_get_llm_response_command_not_found(self):
        """Test when copilot command is not found."""
        with patch("subprocess.run", side_effect=FileNotFoundError()):
            with pytest.raises(FileNotFoundError, match="GitHub Copilot CLI not found"):
                get_llm_response("Test prompt")

    def test_get_llm_response_cli_error(self):
        """Test when CLI returns an error."""
        error = subprocess.CalledProcessError(1, ["copilot"], stderr="Error message")

        with patch("subprocess.run", side_effect=error), pytest.raises(subprocess.CalledProcessError):
            get_llm_response("Test prompt")


class TestExtractHumorScore:
    """Test the extract_humor_score function."""

    def test_extract_score_with_colon_format(self):
        """Test extracting score from 'Score: X' format."""
        assert extract_humor_score("Score: 7") == 7
        assert extract_humor_score("Rating: 9") == 9
        assert extract_humor_score("The score: 3 for this message") == 3

    def test_extract_score_with_fraction_format(self):
        """Test extracting score from 'X/10' format."""
        assert extract_humor_score("8/10") == 8
        assert extract_humor_score("I'd give it a 6 / 10") == 6
        assert extract_humor_score("Rating: 4/10") == 4

    def test_extract_score_with_out_of_format(self):
        """Test extracting score from 'X out of 10' format."""
        assert extract_humor_score("7 out of 10") == 7
        assert extract_humor_score("I'd rate this 9 out of 10") == 9

    def test_extract_score_standalone_digit(self):
        """Test extracting standalone digits."""
        assert extract_humor_score("8") == 8
        assert extract_humor_score("The answer is 6") == 6
        assert extract_humor_score("This deserves a 10") == 10

    def test_extract_score_multiple_digits_takes_first_valid(self):
        """Test that when multiple digits exist, the first valid one is taken."""
        assert extract_humor_score("Between 3 and 8, I'd choose 7") == 3
        assert extract_humor_score("Not a 15, but definitely a 8") == 8

    def test_extract_score_invalid_range_ignored(self):
        """Test that scores outside 1-10 range are ignored."""
        assert extract_humor_score("Score: 15") == 5  # Should default to 5
        assert extract_humor_score("Rating: 0") == 5  # Should default to 5

    def test_extract_score_no_valid_score_defaults_to_five(self):
        """Test that when no valid score is found, it defaults to 5."""
        assert extract_humor_score("This is funny but no score") == 5
        assert extract_humor_score("") == 5
        assert extract_humor_score("abcd xyz") == 5

    def test_extract_score_edge_cases(self):
        """Test edge cases for score extraction."""
        assert extract_humor_score("1") == 1
        assert extract_humor_score("10") == 10
        assert extract_humor_score("Score: 1") == 1
        assert extract_humor_score("10/10") == 10


class TestGetHumorScoreForMessage:
    """Test the get_humor_score_for_message function."""

    def test_get_humor_score_success(self):
        """Test successful humor score calculation."""
        with patch("data_pipeline.utils.get_llm_response", return_value="Score: 8"):
            score = get_humor_score_for_message("This is a funny joke!", "John Doe")
            assert score == 8

    def test_get_humor_score_with_username(self):
        """Test that username is included in prompt when provided."""
        with patch("data_pipeline.utils.get_llm_response", return_value="7") as mock_llm:
            get_humor_score_for_message("Test message", "TestUser")

            # Check that the prompt includes the username
            called_prompt = mock_llm.call_args[0][0]
            assert "by TestUser" in called_prompt
            assert "Test message" in called_prompt

    def test_get_humor_score_without_username(self):
        """Test that function works without username."""
        with patch("data_pipeline.utils.get_llm_response", return_value="6") as mock_llm:
            score = get_humor_score_for_message("Test message")

            assert score == 6
            called_prompt = mock_llm.call_args[0][0]
            assert "by " not in called_prompt
            assert "Test message" in called_prompt

    def test_get_humor_score_handles_llm_error(self):
        """Test that LLM errors are handled gracefully."""
        with patch("data_pipeline.utils.get_llm_response", side_effect=subprocess.CalledProcessError(1, ["copilot"])):
            with patch("builtins.print"):  # Mock print to avoid output during tests
                score = get_humor_score_for_message("Test message")
                assert score == 5  # Should return default score

    def test_get_humor_score_handles_file_not_found(self):
        """Test that FileNotFoundError is handled gracefully."""
        with (
            patch("data_pipeline.utils.get_llm_response", side_effect=FileNotFoundError()),
            patch("builtins.print"),
        ):  # Mock print to avoid output during tests
            score = get_humor_score_for_message("Test message")
            assert score == 5  # Should return default score

    def test_prompt_contains_expected_elements(self):
        """Test that the generated prompt contains expected elements."""
        with patch("data_pipeline.utils.get_llm_response", return_value="7") as mock_llm:
            get_humor_score_for_message("Test funny message", "John")

            called_prompt = mock_llm.call_args[0][0]

            # Check that prompt contains key elements
            assert "random phrase of the week" in called_prompt.lower()
            assert "slack channel" in called_prompt.lower()
            assert "1 to 10" in called_prompt
            assert "Test funny message" in called_prompt
            assert "by John" in called_prompt
            assert "workplace humor" in called_prompt.lower()
