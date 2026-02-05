"""
Digest Module - Batches headlines into periodic digest posts
Instead of spamming individual posts, groups them into readable digests
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from collections import deque
import threading

from src.config import DISPLAY_TIMEZONE
from src.bot import post_tweet
from src.utils.timezone import get_current_et, format_time_delta

logger = logging.getLogger(__name__)

# In-memory queue for pending headlines (thread-safe)
_pending_headlines: deque = deque(maxlen=50)
_lock = threading.Lock()

# Digest settings
DIGEST_INTERVAL_MINUTES = 15  # Post digest every 15 min
MIN_HEADLINES_FOR_DIGEST = 1  # Minimum headlines to trigger a digest
MAX_HEADLINES_PER_DIGEST = 5  # Max headlines in one tweet
IMPORTANCE_KEYWORDS = [
    'strike', 'attack', 'missile', 'drone', 'explosion', 'killed',
    'iran', 'israel', 'hezbollah', 'hamas', 'idf', 'irgc',
    'breaking', 'urgent', 'confirmed', 'official'
]


def calculate_importance(text: str, entities: List[Dict]) -> int:
    """
    Calculate importance score for a headline
    Higher score = more important
    """
    score = 0
    text_lower = text.lower()

    # Keyword matches
    for keyword in IMPORTANCE_KEYWORDS:
        if keyword in text_lower:
            score += 10

    # Entity count (more entities = more significant)
    score += len(entities) * 5

    # Location mentions are important
    location_entities = [e for e in entities if e.get('type') in ['GPE', 'LOC']]
    score += len(location_entities) * 8

    # Military org mentions are critical
    military_entities = [e for e in entities if e.get('type') == 'MILITARY_ORG']
    score += len(military_entities) * 15

    return score


def add_headline(headline_data: Dict):
    """
    Add a headline to the pending digest queue

    headline_data should contain:
    - text: The headline text
    - author: Who reported it
    - display_time: When it was reported (ET)
    - importance: Calculated importance score
    - entities: Extracted entities
    """
    text = headline_data.get('text', '')

    # Skip RTs - X blocks posting RT content as new tweets
    if text.startswith('RT @'):
        logger.debug(f"Skipping RT: {text[:50]}...")
        return

    # Skip replies
    if text.startswith('@'):
        logger.debug(f"Skipping reply: {text[:50]}...")
        return

    with _lock:
        # Calculate importance if not provided
        if 'importance' not in headline_data:
            headline_data['importance'] = calculate_importance(
                text,
                headline_data.get('entities', [])
            )

        headline_data['added_at'] = get_current_et()
        _pending_headlines.append(headline_data)

        logger.info(f"Queued headline (importance={headline_data['importance']}): {text[:50]}...")


def get_pending_headlines() -> List[Dict]:
    """Get all pending headlines, sorted by importance"""
    with _lock:
        headlines = list(_pending_headlines)

    # Sort by importance (highest first)
    headlines.sort(key=lambda x: x.get('importance', 0), reverse=True)
    return headlines


def clear_pending_headlines():
    """Clear the pending headlines queue"""
    with _lock:
        _pending_headlines.clear()


def format_digest_tweet(headlines: List[Dict]) -> str:
    """
    Format headlines into a single digest tweet

    Format:
    MIDDLE EAST UPDATE

    • Headline 1 - 12:15 PM ET (@source)
    • Headline 2 - 12:30 PM ET (@source)

    #MiddleEast #OSINT
    """
    if not headlines:
        return None

    lines = ["MIDDLE EAST UPDATE", ""]

    for h in headlines[:MAX_HEADLINES_PER_DIGEST]:
        # Truncate headline to fit
        text = h.get('text', '')
        # Remove URLs and clean up
        text = ' '.join(word for word in text.split() if not word.startswith('http'))

        # Get first sentence or truncate
        if '.' in text[:120]:
            text = text[:text.index('.', 0, 120) + 1]
        elif len(text) > 100:
            text = text[:97] + "..."

        time_str = h.get('display_time', 'Unknown')
        # Extract just the time part (e.g., "12:15 PM ET")
        if ' at ' in time_str:
            time_str = time_str.split(' at ')[1]

        author = h.get('author', 'unknown')

        lines.append(f"• {text}")
        lines.append(f"  ↳ {time_str} via @{author}")
        lines.append("")

    # Add hashtags
    lines.append("#MiddleEast #Iran #OSINT")

    tweet = "\n".join(lines)

    # Ensure under 280 chars
    if len(tweet) > 280:
        # Reduce to fewer headlines
        if len(headlines) > 1:
            return format_digest_tweet(headlines[:-1])
        else:
            # Single headline, truncate more aggressively
            tweet = tweet[:277] + "..."

    return tweet


def should_post_digest() -> bool:
    """Check if we have enough important headlines to post"""
    headlines = get_pending_headlines()

    if len(headlines) < MIN_HEADLINES_FOR_DIGEST:
        return False

    # Check if any headline is high importance (score > 30)
    high_importance = any(h.get('importance', 0) > 30 for h in headlines)

    # Post if we have high importance news OR enough regular headlines
    return high_importance or len(headlines) >= 3


def post_digest() -> Optional[str]:
    """
    Post a digest of pending headlines
    Returns tweet ID if posted, None otherwise
    """
    headlines = get_pending_headlines()

    if not headlines:
        logger.debug("No headlines to post")
        return None

    # Format the digest
    tweet_text = format_digest_tweet(headlines)

    if not tweet_text:
        return None

    # Post it
    try:
        tweet_id = post_tweet(tweet_text)

        if tweet_id:
            logger.info(f"Posted digest with {len(headlines)} headlines: {tweet_id}")
            clear_pending_headlines()
            return tweet_id
        else:
            logger.error("Failed to post digest")
            return None

    except Exception as e:
        logger.error(f"Error posting digest: {e}")
        return None


def maybe_post_digest() -> Optional[str]:
    """
    Check if conditions are met and post digest if so
    Called periodically by the worker
    """
    if should_post_digest():
        return post_digest()
    return None


# Track last digest time
_last_digest_time: Optional[datetime] = None


def check_digest_timer() -> Optional[str]:
    """
    Check if it's time for a scheduled digest post
    Posts every DIGEST_INTERVAL_MINUTES if there are headlines
    """
    global _last_digest_time

    now = get_current_et()

    if _last_digest_time is None:
        _last_digest_time = now
        return None

    elapsed = (now - _last_digest_time).total_seconds() / 60

    if elapsed >= DIGEST_INTERVAL_MINUTES:
        headlines = get_pending_headlines()
        if headlines:
            result = post_digest()
            _last_digest_time = now
            return result
        else:
            _last_digest_time = now  # Reset timer even if no headlines

    return None
