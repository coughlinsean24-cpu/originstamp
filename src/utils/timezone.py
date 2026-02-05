"""
Timezone Utilities
All times displayed in Eastern Time (ET) for consistency
"""
from datetime import datetime
from typing import Dict
import pytz

DISPLAY_TIMEZONE = pytz.timezone('America/New_York')
UTC = pytz.UTC


def convert_to_et(utc_timestamp: datetime) -> Dict:
    """
    Convert UTC timestamp to Eastern Time

    Args:
        utc_timestamp: datetime object in UTC

    Returns:
        dict with timestamp_et and display_time
    """
    if utc_timestamp is None:
        return {
            "timestamp_et": None,
            "display_time": "Unknown"
        }

    # Ensure UTC timezone
    if utc_timestamp.tzinfo is None:
        utc_timestamp = UTC.localize(utc_timestamp)

    # Convert to Eastern Time
    eastern_dt = utc_timestamp.astimezone(DISPLAY_TIMEZONE)

    # Format display time: "Feb 5 at 9:32 AM ET"
    display_time = eastern_dt.strftime("%b %-d at %-I:%M %p ET")

    return {
        "timestamp_et": eastern_dt,
        "display_time": display_time
    }


def format_time_delta(original_time_et: datetime, current_time_et: datetime = None) -> str:
    """
    Calculate human-readable time difference

    Args:
        original_time_et: datetime in ET
        current_time_et: datetime in ET (defaults to now)

    Returns:
        str like "3h 47m ago" or "2d 5h ago"
    """
    if current_time_et is None:
        current_time_et = get_current_et()

    delta = current_time_et - original_time_et

    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        return "In the future"

    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60

    if days > 0:
        return f"{days}d {hours}h ago"
    elif hours > 0:
        return f"{hours}h {minutes}m ago"
    elif minutes > 0:
        return f"{minutes}m ago"
    else:
        return "Just now"


def format_time_delta_seconds(seconds: int) -> str:
    """Format seconds into human readable string"""
    if seconds < 0:
        return "In the future"
    if seconds < 60:
        return "Just now"

    minutes = seconds // 60
    hours = minutes // 60
    days = hours // 24

    if days > 0:
        return f"{days}d {hours % 24}h ago"
    elif hours > 0:
        return f"{hours}h {minutes % 60}m ago"
    else:
        return f"{minutes}m ago"


def get_current_et() -> datetime:
    """Get current time in Eastern Time"""
    return datetime.now(DISPLAY_TIMEZONE)


def get_current_utc() -> datetime:
    """Get current time in UTC"""
    return datetime.now(UTC)


def parse_twitter_timestamp(timestamp_str: str) -> datetime:
    """
    Parse Twitter's timestamp format to UTC datetime

    Twitter format: "Wed Oct 10 20:19:24 +0000 2018"
    """
    try:
        dt = datetime.strptime(timestamp_str, "%a %b %d %H:%M:%S %z %Y")
        return dt.astimezone(UTC)
    except ValueError:
        # Try ISO format
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.astimezone(UTC)
        except ValueError:
            return None
