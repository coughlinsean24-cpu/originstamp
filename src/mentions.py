"""
Mentions Handler Module
Responds to @newstimestamp mentions with news origin lookups
"""
import logging
import re
from typing import Dict, List, Optional
from datetime import datetime, timedelta

import tweepy

from src.config import (
    X_API_KEY, X_API_SECRET, X_BEARER_TOKEN,
    X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET,
    BOT_USERNAME
)
from src.database import search_events, get_db_connection, get_connection_type, dict_from_row
from src.bot import post_tweet

logger = logging.getLogger(__name__)

# Track which mentions we've already replied to
_replied_mention_ids = set()

# Last time we checked mentions
_last_mention_check = None


def get_client() -> tweepy.Client:
    """Get authenticated tweepy client"""
    return tweepy.Client(
        bearer_token=X_BEARER_TOKEN,
        consumer_key=X_API_KEY,
        consumer_secret=X_API_SECRET,
        access_token=X_ACCESS_TOKEN,
        access_token_secret=X_ACCESS_TOKEN_SECRET,
        wait_on_rate_limit=True
    )


def get_recent_mentions(since_id: str = None) -> List[Dict]:
    """
    Get recent mentions of the bot account

    Returns list of mentions with id, text, author
    """
    try:
        client = get_client()

        # Get our user ID
        me = client.get_me()
        if not me.data:
            logger.error("Could not get bot user info")
            return []

        user_id = me.data.id

        # Get mentions
        kwargs = {
            'id': user_id,
            'max_results': 10,
            'tweet_fields': ['created_at', 'author_id', 'conversation_id'],
            'expansions': ['author_id'],
            'user_fields': ['username']
        }

        if since_id:
            kwargs['since_id'] = since_id

        mentions = client.get_users_mentions(**kwargs)

        if not mentions.data:
            return []

        # Build author map from includes
        author_map = {}
        if mentions.includes and 'users' in mentions.includes:
            for user in mentions.includes['users']:
                author_map[user.id] = user.username

        results = []
        for mention in mentions.data:
            results.append({
                'id': str(mention.id),
                'text': mention.text,
                'author_id': mention.author_id,
                'author': author_map.get(mention.author_id, 'unknown'),
                'created_at': mention.created_at,
                'conversation_id': str(mention.conversation_id) if mention.conversation_id else None
            })

        return results

    except tweepy.errors.TooManyRequests:
        logger.warning("Rate limited checking mentions")
        return []
    except Exception as e:
        logger.error(f"Error getting mentions: {e}")
        return []


def extract_query(mention_text: str) -> Optional[str]:
    """
    Extract the search query from a mention

    Examples:
    - "@newstimestamp when was Iran ultimatum first reported?"
    - "@newstimestamp who first reported the strike on Lebanon?"
    - "@newstimestamp Iran talks"

    Returns the query string or None if can't parse
    """
    # Remove the bot mention
    text = re.sub(r'@\w+', '', mention_text).strip()

    # Remove common question prefixes
    patterns_to_remove = [
        r'^when was\s+',
        r'^who first reported\s+',
        r'^who reported\s+',
        r'^where did\s+',
        r'^what time was\s+',
        r'^first report(ed)?\s+',
        r'^origin of\s+',
        r'^source of\s+',
        r'\?$',
        r'first reported\??$',
    ]

    for pattern in patterns_to_remove:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()

    # If we have at least 3 characters, use it as the query
    if len(text) >= 3:
        return text

    return None


def search_news_origin(query: str) -> Optional[Dict]:
    """
    Search database for the first report matching the query

    Returns the first/original report info or None
    """
    # Search canonical events
    events = search_events(query, limit=5)

    if not events:
        # Try searching individual keywords
        keywords = query.split()
        for keyword in keywords:
            if len(keyword) >= 4:  # Skip short words
                events = search_events(keyword, limit=5)
                if events:
                    break

    if not events:
        return None

    # Return the most relevant result (first match, sorted by recency in search_events)
    # But we want the earliest, so let's get more detail
    event = events[0]

    return {
        'first_author': event.get('first_author'),
        'first_display_time': event.get('first_display_time'),
        'claim_summary': event.get('claim_summary') or event.get('original_text', '')[:150],
        'repost_count': event.get('repost_count', 0),
        'event_id': event.get('id')
    }


def format_origin_reply(query: str, result: Dict) -> str:
    """
    Format the reply tweet

    Be clear this is "earliest tracked" not absolute first ever
    """
    author = result.get('first_author', 'unknown')
    time_str = result.get('first_display_time', 'Unknown time')
    repost_count = result.get('repost_count', 0)

    # Truncate query for display
    display_query = query[:40] + "..." if len(query) > 40 else query

    # Build reply - be clear about scope
    reply = f'Earliest tracked report of "{display_query}":\n\n@{author} at {time_str}'

    if repost_count > 0:
        reply += f"\n\n{repost_count} other tracked sources followed."

    # Ensure under 280 chars
    if len(reply) > 280:
        reply = reply[:277] + "..."

    return reply


def reply_to_mention(mention: Dict, reply_text: str) -> Optional[str]:
    """
    Reply to a mention

    Returns the reply tweet ID or None on failure
    """
    try:
        client = get_client()

        response = client.create_tweet(
            text=reply_text,
            in_reply_to_tweet_id=mention['id']
        )

        if response.data:
            tweet_id = response.data['id']
            logger.info(f"Replied to mention {mention['id']}: {tweet_id}")
            return str(tweet_id)
        return None

    except tweepy.errors.Forbidden as e:
        logger.error(f"403 Forbidden replying to mention: {e}")
        return None
    except Exception as e:
        logger.error(f"Error replying to mention: {e}")
        return None


def process_mention(mention: Dict) -> bool:
    """
    Process a single mention - extract query, search, and reply

    Returns True if successfully processed, False otherwise
    """
    mention_id = mention['id']

    # Skip if already processed
    if mention_id in _replied_mention_ids:
        return False

    # Extract the query
    query = extract_query(mention['text'])

    if not query:
        logger.debug(f"Could not extract query from mention: {mention['text'][:50]}...")
        _replied_mention_ids.add(mention_id)
        return False

    logger.info(f"Processing mention from @{mention['author']}: query='{query}'")

    # Search for the origin
    result = search_news_origin(query)

    if not result:
        # Reply that we couldn't find it
        reply_text = f"Sorry, I couldn't find any reports matching \"{query[:30]}\" in my database. I track news from OSINT accounts and wire services."
        reply_to_mention(mention, reply_text)
        _replied_mention_ids.add(mention_id)
        return True

    # Format and send reply
    reply_text = format_origin_reply(query, result)
    reply_id = reply_to_mention(mention, reply_text)

    _replied_mention_ids.add(mention_id)

    if reply_id:
        logger.info(f"Successfully replied to @{mention['author']} about '{query}'")
        return True

    return False


def check_and_process_mentions() -> int:
    """
    Check for new mentions and process them

    Called periodically by the worker
    Returns number of mentions processed
    """
    global _last_mention_check

    # Don't check too frequently (rate limits)
    if _last_mention_check:
        elapsed = (datetime.utcnow() - _last_mention_check).total_seconds()
        if elapsed < 60:  # Check at most once per minute
            return 0

    _last_mention_check = datetime.utcnow()

    # Get recent mentions
    mentions = get_recent_mentions()

    if not mentions:
        return 0

    processed = 0
    for mention in mentions:
        # Skip our own tweets
        if mention['author'].lower() == BOT_USERNAME.lower():
            continue

        if process_mention(mention):
            processed += 1

    if processed > 0:
        logger.info(f"Processed {processed} mentions")

    return processed


def cleanup_old_mention_ids():
    """Clean up old mention IDs to prevent memory growth"""
    global _replied_mention_ids

    # Keep only last 1000 IDs
    if len(_replied_mention_ids) > 1000:
        _replied_mention_ids = set(list(_replied_mention_ids)[-500:])
