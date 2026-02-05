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
DIGEST_INTERVAL_MINUTES = 30  # Post digest every 30 min (poll faster, post less often)
MIN_HEADLINES_FOR_DIGEST = 1  # Minimum headlines to trigger a digest
MAX_HEADLINES_PER_DIGEST = 5  # Max headlines in one tweet
IMPORTANCE_KEYWORDS = [
    'strike', 'attack', 'missile', 'drone', 'explosion', 'killed',
    'iran', 'israel', 'hezbollah', 'hamas', 'idf', 'irgc',
    'breaking', 'urgent', 'confirmed', 'official'
]

# Minimum requirements for a headline to be newsworthy
MIN_TEXT_LENGTH = 80  # Skip short tweets (memes, reactions)
MIN_IMPORTANCE_SCORE = 20  # Skip low-importance content

# Words that indicate non-news content
SKIP_INDICATORS = [
    'thank you', 'thanks', 'congrats', 'happy birthday', 'rip ',
    'lol', 'lmao', 'haha', 'omg', '😂', '🤣', 'podcast', 'latest pod',
    'subscribe', 'follow me', 'check out', 'new episode',
    'we are live', 'going live', 'now live', 'live now', 'discussing',
    'tune in', 'watch live', 'stream', 'join us'
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


def is_newsworthy(text: str, entities: list, importance: int) -> tuple[bool, str]:
    """
    Check if a tweet is actual news vs meme/reaction/fluff
    Returns (is_newsworthy, reason)
    """
    # Remove URLs from text for length check
    clean_text = ' '.join(word for word in text.split() if not word.startswith('http'))

    # Too short = likely meme/reaction
    if len(clean_text) < MIN_TEXT_LENGTH:
        return False, f"too short ({len(clean_text)} chars)"

    # Check for non-news indicators
    text_lower = text.lower()
    for indicator in SKIP_INDICATORS:
        if indicator in text_lower:
            return False, f"contains '{indicator}'"

    # Must have minimum importance OR meaningful entities
    has_location = any(e.get('type') in ['GPE', 'LOC'] for e in entities)
    has_org = any(e.get('type') in ['ORG', 'MILITARY_ORG'] for e in entities)

    if importance < MIN_IMPORTANCE_SCORE and not (has_location or has_org):
        return False, f"low importance ({importance}) and no key entities"

    return True, "passed"


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

    # Calculate importance early for filtering
    entities = headline_data.get('entities', [])
    importance = headline_data.get('importance')
    if importance is None:
        importance = calculate_importance(text, entities)
        headline_data['importance'] = importance

    # Check if newsworthy
    is_news, reason = is_newsworthy(text, entities, importance)
    if not is_news:
        logger.debug(f"Skipping non-news ({reason}): {text[:50]}...")
        return

    with _lock:
        headline_data['added_at'] = get_current_et()

        # Deduplicate by event_id - keep the FIRST reporter only
        event_id = headline_data.get('event_id')
        if event_id:
            for i, existing in enumerate(_pending_headlines):
                if existing.get('event_id') == event_id:
                    # Same event - keep whichever was reported first
                    existing_time = existing.get('display_time', '')
                    new_time = headline_data.get('display_time', '')
                    logger.debug(f"Duplicate event {event_id}: existing @{existing.get('author')} vs new @{headline_data.get('author')}")
                    # Keep existing (it was first), skip new one
                    return

        _pending_headlines.append(headline_data)
        logger.info(f"Queued headline (importance={importance}): {text[:50]}...")


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


def format_digest_tweet(headlines: List[Dict]) -> Optional[str]:
    """
    Format headlines into a single digest tweet

    Emphasizes who broke the news first
    """
    if not headlines:
        return None

    # Post the top headline - this is whoever reported it FIRST
    h = headlines[0]
    text = h.get('text', '')

    # Remove URLs and clean up
    text = ' '.join(word for word in text.split() if not word.startswith('http'))

    # Remove emojis at the start (🔴, 🎯, ⭕️, etc.)
    while text and text[0] in '🔴🎯⭕️❌🎥🇮🇱🇺🇸⚓️📰🗞️▶️⚠️✓':
        text = text[1:].lstrip()

    # Get first sentence or truncate
    if '. ' in text[:200]:
        text = text[:text.index('. ', 0, 200) + 1]
    elif len(text) > 160:
        text = text[:157] + "..."

    time_str = h.get('display_time', '')
    # Extract just the time part (e.g., "5:18 PM ET")
    if ' at ' in time_str:
        time_str = time_str.split(' at ')[1]

    author = h.get('author', 'unknown')

    # Format: emphasize this is the first/original report
    attribution = f"First: @{author} • {time_str}"
    tweet = f"{text}\n\n{attribution}"

    # Ensure under 280 chars
    if len(tweet) > 280:
        max_text = 280 - len(f"\n\n{attribution}") - 3
        text = text[:max_text] + "..."
        tweet = f"{text}\n\n{attribution}"

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
