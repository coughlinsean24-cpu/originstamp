"""
X Bot Posting Module
Handles posting original reports, replies to reposts, and timeline threads
"""
import logging
from typing import Dict, List, Optional

import tweepy

from src.config import (
    X_API_KEY, X_API_SECRET, X_BEARER_TOKEN,
    X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET,
    POST_ORIGINAL_REPORTS, REPLY_TO_REPOSTS, POST_TIMELINE_THREADS,
    MIN_REPOSTS_FOR_THREAD
)
from src.database import get_event_timeline

logger = logging.getLogger(__name__)

# Initialize X API client
_client = None


def get_client() -> tweepy.Client:
    """Get authenticated tweepy client (singleton)"""
    global _client
    if _client is None:
        _client = tweepy.Client(
            bearer_token=X_BEARER_TOKEN,
            consumer_key=X_API_KEY,
            consumer_secret=X_API_SECRET,
            access_token=X_ACCESS_TOKEN,
            access_token_secret=X_ACCESS_TOKEN_SECRET,
            wait_on_rate_limit=True
        )
    return _client


def post_tweet(text: str, reply_to: str = None) -> Optional[str]:
    """
    Post a tweet

    Args:
        text: Tweet text (max 280 chars)
        reply_to: Tweet ID to reply to (optional)

    Returns:
        Posted tweet ID or None on failure
    """
    try:
        client = get_client()

        # Truncate if needed
        if len(text) > 280:
            text = text[:277] + "..."

        kwargs = {"text": text}
        if reply_to:
            kwargs["in_reply_to_tweet_id"] = reply_to

        response = client.create_tweet(**kwargs)

        if response.data:
            tweet_id = response.data['id']
            logger.info(f"Posted tweet: {tweet_id}")
            return str(tweet_id)
        return None

    except Exception as e:
        logger.error(f"Error posting tweet: {e}")
        return None


def format_reliability_stars(score: float) -> str:
    """Convert reliability score to star rating"""
    if score >= 0.95:
        return "★★★★★"
    elif score >= 0.85:
        return "★★★★☆"
    elif score >= 0.75:
        return "★★★☆☆"
    elif score >= 0.60:
        return "★★☆☆☆"
    else:
        return "★☆☆☆☆"


def post_original_report(event: Dict) -> Optional[str]:
    """
    Post original report tweet

    Args:
        event: Canonical event dict

    Returns:
        Posted tweet ID or None
    """
    if not POST_ORIGINAL_REPORTS:
        return None

    reliability = event.get('author_reliability', 0.85)
    stars = format_reliability_stars(reliability)

    # Build tweet text
    claim = event.get('claim_summary', event.get('original_text', ''))
    if len(claim) > 120:
        claim = claim[:117] + "..."

    tweet_text = f"""ORIGINAL REPORT

{claim}

First reported: {event.get('first_display_time', 'Unknown')}
Source: @{event.get('first_author', 'unknown')} ({stars})

#OriginStamp #OSINT"""

    return post_tweet(tweet_text)


def reply_to_repost(repost_tweet_id: str, original_event: Dict) -> Optional[str]:
    """
    Reply to a repost with detection notice

    Args:
        repost_tweet_id: Tweet ID of the repost
        original_event: Original canonical event dict

    Returns:
        Posted reply tweet ID or None
    """
    if not REPLY_TO_REPOSTS:
        return None

    tweet_text = f"""REPOST DETECTED

This was first reported {original_event.get('time_delta_display', 'earlier')}.

Original: {original_event.get('first_display_time', 'Unknown')}
First source: @{original_event.get('first_author', 'unknown')}
Reposted {original_event.get('repost_count', 0)} times since"""

    return post_tweet(tweet_text, reply_to=repost_tweet_id)


def post_timeline_thread(event_id: int) -> List[str]:
    """
    Post timeline thread for major events

    Args:
        event_id: Canonical event ID

    Returns:
        List of posted tweet IDs
    """
    if not POST_TIMELINE_THREADS:
        return []

    timeline = get_event_timeline(event_id)
    if not timeline or not timeline.get('event'):
        return []

    event = timeline['event']
    reposts = timeline.get('reposts', [])

    # Only post thread if enough reposts
    if len(reposts) < MIN_REPOSTS_FOR_THREAD:
        return []

    thread_ids = []

    # Thread starter
    claim = event.get('claim_summary', event.get('original_text', ''))
    if len(claim) > 100:
        claim = claim[:97] + "..."

    starter_text = f"""TIMELINE: {claim}

{event.get('first_display_time', 'Unknown')} - @{event.get('first_author', 'unknown')} first reported this

{len(reposts)} accounts have since covered this story."""

    starter_id = post_tweet(starter_text)
    if not starter_id:
        return []

    thread_ids.append(starter_id)
    previous_id = starter_id

    # Add key updates (max 5 for brevity)
    key_reposts = []

    # Prioritize updates and tier 1 sources
    for r in reposts:
        if r.get('classification') == 'UPDATE':
            key_reposts.append(r)
        elif r.get('author_tier', '').startswith('1'):
            key_reposts.append(r)

        if len(key_reposts) >= 5:
            break

    # If not enough key reposts, add others
    if len(key_reposts) < 5:
        for r in reposts:
            if r not in key_reposts:
                key_reposts.append(r)
            if len(key_reposts) >= 5:
                break

    for item in key_reposts:
        classification = item.get('classification', 'REPOST')
        emoji = "+" if classification == 'UPDATE' else "↻"

        reply_text = f"""{item.get('display_time', 'Unknown')} - @{item.get('author', 'unknown')}
{emoji} {classification}
{item.get('time_delta_display', '')}"""

        reply_id = post_tweet(reply_text, reply_to=previous_id)
        if reply_id:
            thread_ids.append(reply_id)
            previous_id = reply_id

    logger.info(f"Posted timeline thread with {len(thread_ids)} tweets for event {event_id}")
    return thread_ids


def post_contradiction_alert(event_id: int, claims: List[Dict]) -> Optional[str]:
    """
    Post alert about conflicting reports

    Args:
        event_id: Event ID for reference
        claims: List of conflicting claim dicts

    Returns:
        Posted tweet ID or None
    """
    if len(claims) < 2:
        return None

    claim1 = claims[0]
    claim2 = claims[1]

    # Truncate excerpts
    excerpt1 = claim1.get('excerpt', '')[:50]
    excerpt2 = claim2.get('excerpt', '')[:50]

    tweet_text = f"""CONFLICTING REPORTS

Two reliable sources disagree:

1. {claim1.get('display_time', '')} - @{claim1.get('author', '')}: "{excerpt1}..."

2. {claim2.get('display_time', '')} - @{claim2.get('author', '')}: "{excerpt2}..."

Exercise caution."""

    return post_tweet(tweet_text)


def post_verification_update(event_id: int, status: str) -> Optional[str]:
    """
    Post when an event gets verified by official/wire sources

    Args:
        event_id: Canonical event ID
        status: Verification status

    Returns:
        Posted tweet ID or None
    """
    timeline = get_event_timeline(event_id)
    if not timeline or not timeline.get('event'):
        return None

    event = timeline['event']

    status_emoji = {
        'official_confirmed': '✓ OFFICIALLY CONFIRMED',
        'wire_verified': '✓ WIRE SERVICE VERIFIED',
        'cross_verified': '✓ CROSS-VERIFIED',
        'disputed': '⚠ DISPUTED'
    }

    claim = event.get('claim_summary', '')[:100]

    tweet_text = f"""{status_emoji.get(status, 'STATUS UPDATE')}

"{claim}..."

Originally reported by @{event.get('first_author', '')}
at {event.get('first_display_time', '')}

#OriginStamp"""

    return post_tweet(tweet_text)


def get_my_tweets(count: int = 10) -> List[Dict]:
    """Get recent tweets from the bot account"""
    try:
        client = get_client()
        me = client.get_me()

        if not me.data:
            return []

        tweets = client.get_users_tweets(
            me.data.id,
            max_results=count,
            tweet_fields=['created_at', 'text']
        )

        if not tweets.data:
            return []

        return [
            {
                'id': str(t.id),
                'text': t.text,
                'created_at': t.created_at
            }
            for t in tweets.data
        ]

    except Exception as e:
        logger.error(f"Error getting bot tweets: {e}")
        return []
