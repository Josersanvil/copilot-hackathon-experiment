import re
import subprocess
from typing import Optional


def get_llm_response(prompt: str) -> str:
    """
    Get a response from GitHub Copilot CLI using the provided prompt.

    Args:
        prompt: The prompt to send to the LLM

    Returns:
        The stdout response from the CLI call

    Raises:
        subprocess.CalledProcessError: If the CLI call fails
        FileNotFoundError: If the 'copilot' command is not found
    """
    try:
        # Run the copilot command with the prompt
        result = subprocess.run(["copilot", "-p", prompt], capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except FileNotFoundError:
        raise FileNotFoundError("GitHub Copilot CLI not found. Please ensure it's installed and in your PATH.")
    except subprocess.CalledProcessError as e:
        raise subprocess.CalledProcessError(e.returncode, e.cmd, f"CLI call failed with stderr: {e.stderr}")


def extract_humor_score(llm_response: str) -> int:
    """
    Extract the humor score (1-10) from the LLM response.

    Args:
        llm_response: The raw response from the LLM

    Returns:
        Integer score between 1 and 10

    Raises:
        ValueError: If no valid score can be extracted
    """
    # Look for patterns like "Score: 7", "7/10", "Rating: 8", etc.
    patterns = [
        r"(?:score|rating):\s*(\d+)",  # "Score: 7" or "Rating: 8"
        r"(\d+)\s*/\s*10",  # "7/10" or "8 / 10"
        r"(?:^|\s)(\d+)(?:\s|$)",  # Any standalone digit
        r"(\d+)\s*out\s*of\s*10",  # "7 out of 10"
    ]

    for pattern in patterns:
        match = re.search(pattern, llm_response.lower())
        if match:
            score = int(match.group(1))
            if 1 <= score <= 10:
                return score

    # If no pattern matches, try to find any digit between 1-10
    digits = re.findall(r"\b([1-9]|10)\b", llm_response)
    if digits:
        return int(digits[0])

    # Default to 5 if nothing can be extracted
    return 5


def get_humor_score_for_message(message: str, username: str = "") -> int:
    """
    Get a humor score (1-10) for a chat message from the "random phrase of the week" Slack channel.

    Args:
        message: The chat message to score
        username: Optional username of the message author

    Returns:
        Integer score between 1 and 10 indicating how funny the message is
    """
    # Create a well-crafted prompt for the LLM
    prompt = f"""You are analyzing messages from a workplace Slack channel called "random phrase of the week" where colleagues share funny quotes, witty remarks, and humorous observations from their daily work interactions.

Please rate the following message on a scale from 1 to 10 based on how funny or amusing it is:
- 1-3: Not funny, mundane, or purely informational
- 4-6: Mildly amusing, decent workplace humor
- 7-8: Quite funny, would make most people chuckle
- 9-10: Hilarious, exceptional workplace humor

Message{f" by {username}" if username else ""}: "{message}"

Please respond with just a single number from 1 to 10 representing the humor score."""

    try:
        llm_response = get_llm_response(prompt)
        return extract_humor_score(llm_response)
    except (subprocess.CalledProcessError, FileNotFoundError, ValueError) as e:
        # Log error and return default score
        print(f"Warning: Could not get humor score for message. Error: {e}")
        return 5  # Default neutral score
