"""
Tweet Ingestion Module
Monitors X stream for tracked accounts and processes tweets
"""
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime

import tweepy

from src.config import (
    X_API_KEY, X_API_SECRET, X_BEARER_TOKEN,
    X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET,
    STREAM_RECONNECT_DELAY
)
from src.database import (
    get_tracked_accounts, insert_tweet, insert_entities, insert_urls,
    find_similar_tweets, create_canonical_event, add_repost,
    update_account_metrics, init_database, get_event_timeline
)
from src.bot import post_original_report, reply_to_repost
from src.config import POST_ORIGINAL_REPORTS, REPLY_TO_REPOSTS
from src.fingerprinting import create_tweet_fingerprint
from src.similarity import classify_tweet
from src.utils.timezone import convert_to_et, parse_twitter_timestamp

logger = logging.getLogger(__name__)


class TweetProcessor:
    """Processes incoming tweets"""

    def __init__(self):
        self.tracked_accounts = {}
        self.load_tracked_accounts()

    def load_tracked_accounts(self):
        """Load tracked accounts from database"""
        accounts = get_tracked_accounts()
        self.tracked_accounts = {
            a['account'].lower().lstrip('@'): a
            for a in accounts
        }
        logger.info(f"Loaded {len(self.tracked_accounts)} tracked accounts")

    def get_account_info(self, username: str) -> Optional[Dict]:
        """Get tracked account info"""
        return self.tracked_accounts.get(username.lower().lstrip('@'))

    def process_tweet(self, tweet_data: Dict) -> Dict:
        """
        Process a single tweet through the full pipeline

        Args:
            tweet_data: Raw tweet data from X API

        Returns:
            Processing result with classification
        """
        try:
            # Extract basic info
            tweet_id = str(tweet_data.get('id', ''))
            author = tweet_data.get('author', {}).get('username', '') or tweet_data.get('username', '')
            text = tweet_data.get('text', '')

            if not tweet_id or not author or not text:
                logger.warning(f"Missing required fields in tweet: {tweet_data}")
                return {"status": "error", "message": "Missing required fields"}

            # Get account info
            account_info = self.get_account_info(author)
            author_tier = account_info['tier'] if account_info else '3_SECONDARY'
            author_reliability = account_info['initial_reliability'] if account_info else 0.50

            # Parse timestamp
            created_at = tweet_data.get('created_at')
            if isinstance(created_at, str):
                timestamp_utc = parse_twitter_timestamp(created_at)
            elif isinstance(created_at, datetime):
                timestamp_utc = created_at
            else:
                timestamp_utc = datetime.utcnow()

            # Convert to ET
            et_info = convert_to_et(timestamp_utc)

            # Create fingerprint
            fingerprint = create_tweet_fingerprint({'text': text})

            # Build tweet record
            tweet_record = {
                'tweet_id': tweet_id,
                'author': author,
                'author_tier': author_tier,
                'author_reliability': author_reliability,
                'text': text,
                'text_normalized': fingerprint['text_normalized'],
                'text_hash': fingerprint['text_hash'],
                'event_hash': fingerprint['event_hash'],
                'timestamp_utc': timestamp_utc,
                'timestamp_et': et_info['timestamp_et'],
                'display_time': et_info['display_time'],
                'language': fingerprint['language'],
                'entities': fingerprint['entities'],
                'urls': fingerprint['urls'],
                'urls_canonical': fingerprint['urls_canonical'],
                'quoted_tweet_id': tweet_data.get('quoted_tweet_id'),
                'reply_to_tweet_id': tweet_data.get('in_reply_to_tweet_id'),
                'retweet_of_tweet_id': tweet_data.get('retweeted_tweet_id'),
            }

            # Find similar tweets
            similar_tweets = []
            for search_type, search_value in [
                ('text_hash', fingerprint['text_hash']),
                ('event_hash', fingerprint['event_hash']),
            ]:
                matches = find_similar_tweets(search_type, search_value)
                for m in matches:
                    if m['tweet_id'] != tweet_id and m not in similar_tweets:
                        similar_tweets.append(m)

            # Classify tweet
            classification = classify_tweet(tweet_record, similar_tweets)

            # Insert tweet into database
            tweet_db_id = insert_tweet(tweet_record)

            # Insert entities
            insert_entities(tweet_db_id, fingerprint['entities'])

            # Insert URLs
            insert_urls(tweet_db_id, fingerprint['urls'])

            # Handle classification
            if classification['status'] == 'ORIGINAL':
                # Create new canonical event
                event_id = create_canonical_event(
                    tweet_db_id,
                    fingerprint['event_hash'],
                    text[:200]
                )
                classification['canonical_event_id'] = event_id
                logger.info(f"NEW ORIGINAL: @{author} - {text[:50]}...")

                # Post to X about the original report
                if POST_ORIGINAL_REPORTS:
                    try:
                        event_data = {
                            'id': event_id,
                            'claim_summary': text[:200],
                            'first_display_time': et_info['display_time'],
                            'first_author': author,
                            'author_reliability': author_reliability
                        }
                        posted_id = post_original_report(event_data)
                        if posted_id:
                            logger.info(f"Posted original report: {posted_id}")
                    except Exception as e:
                        logger.error(f"Failed to post original report: {e}")

            elif classification['status'] in ['REPOST', 'UPDATE']:
                # Link to existing canonical event
                if classification.get('canonical_event_id'):
                    add_repost(
                        classification['canonical_event_id'],
                        tweet_db_id,
                        classification['status'],
                        classification['confidence'],
                        classification['time_delta_seconds'],
                        classification['time_delta_display'],
                        classification['added_new_info']
                    )

                    # Reply to repost if enabled
                    if REPLY_TO_REPOSTS and classification['status'] == 'REPOST':
                        try:
                            timeline = get_event_timeline(classification['canonical_event_id'])
                            if timeline and timeline.get('event'):
                                event = timeline['event']
                                event['time_delta_display'] = classification['time_delta_display']
                                event['repost_count'] = event.get('repost_count', 0)
                                reply_id = reply_to_repost(tweet_id, event)
                                if reply_id:
                                    logger.info(f"Replied to repost: {reply_id}")
                        except Exception as e:
                            logger.error(f"Failed to reply to repost: {e}")

                logger.info(
                    f"{classification['status']}: @{author} - "
                    f"Original by @{classification['original_source']} "
                    f"({classification['time_delta_display']})"
                )

            # Update account metrics
            update_account_metrics(author, classification['status'], author_tier)

            return {
                "status": "success",
                "tweet_id": tweet_id,
                "author": author,
                "classification": classification,
                "display_time": et_info['display_time']
            }

        except Exception as e:
            logger.error(f"Error processing tweet: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}


class OriginStampStream(tweepy.StreamingClient):
    """Custom streaming client for monitoring X"""

    def __init__(self, bearer_token, processor: TweetProcessor):
        super().__init__(bearer_token, wait_on_rate_limit=True)
        self.processor = processor
        self.tweet_count = 0

    def on_tweet(self, tweet):
        """Handle incoming tweet"""
        try:
            # Convert to dict format
            tweet_data = {
                'id': tweet.id,
                'text': tweet.text,
                'created_at': tweet.created_at,
                'author_id': tweet.author_id,
            }

            # Get author info if available
            if hasattr(tweet, 'includes') and 'users' in tweet.includes:
                for user in tweet.includes['users']:
                    if user.id == tweet.author_id:
                        tweet_data['username'] = user.username
                        tweet_data['author'] = {'username': user.username}
                        break

            result = self.processor.process_tweet(tweet_data)
            self.tweet_count += 1

            if self.tweet_count % 100 == 0:
                logger.info(f"Processed {self.tweet_count} tweets")

        except Exception as e:
            logger.error(f"Error in on_tweet: {e}", exc_info=True)

    def on_error(self, status_code):
        """Handle API errors"""
        logger.error(f"Stream error: {status_code}")
        if status_code == 420:
            # Rate limited - sleep longer
            time.sleep(STREAM_RECONNECT_DELAY * 2)
        return True  # Continue streaming

    def on_disconnect(self):
        """Handle disconnection"""
        logger.warning("Stream disconnected")
        return True


def get_api_client() -> tweepy.Client:
    """Get authenticated tweepy client"""
    return tweepy.Client(
        bearer_token=X_BEARER_TOKEN,
        consumer_key=X_API_KEY,
        consumer_secret=X_API_SECRET,
        access_token=X_ACCESS_TOKEN,
        access_token_secret=X_ACCESS_TOKEN_SECRET,
        wait_on_rate_limit=True
    )


def fetch_user_tweets(username: str, max_results: int = 10) -> List[Dict]:
    """
    Fetch recent tweets from a specific user

    Args:
        username: X username (without @)
        max_results: Maximum tweets to fetch

    Returns:
        List of tweet data dicts
    """
    client = get_api_client()

    try:
        # Get user ID
        user = client.get_user(username=username)
        if not user.data:
            logger.warning(f"User not found: {username}")
            return []

        user_id = user.data.id

        # Get tweets
        tweets = client.get_users_tweets(
            user_id,
            max_results=max_results,
            tweet_fields=['created_at', 'text', 'author_id', 'entities'],
            expansions=['author_id']
        )

        if not tweets.data:
            return []

        result = []
        for tweet in tweets.data:
            tweet_data = {
                'id': tweet.id,
                'text': tweet.text,
                'created_at': tweet.created_at,
                'author_id': tweet.author_id,
                'username': username,
                'author': {'username': username}
            }
            result.append(tweet_data)

        return result

    except Exception as e:
        logger.error(f"Error fetching tweets for {username}: {e}")
        return []


def poll_tracked_accounts(processor: TweetProcessor, interval_seconds: int = 60):
    """
    Poll tracked accounts for new tweets (alternative to streaming)

    Args:
        processor: TweetProcessor instance
        interval_seconds: Seconds between poll cycles
    """
    client = get_api_client()
    seen_tweet_ids = set()

    logger.info(f"Starting polling with {len(processor.tracked_accounts)} accounts")

    while True:
        try:
            for username, account_info in processor.tracked_accounts.items():
                try:
                    tweets = fetch_user_tweets(username, max_results=5)

                    for tweet_data in tweets:
                        tweet_id = str(tweet_data['id'])
                        if tweet_id not in seen_tweet_ids:
                            seen_tweet_ids.add(tweet_id)
                            processor.process_tweet(tweet_data)

                    # Small delay between users to avoid rate limits
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Error polling {username}: {e}")
                    continue

            # Cleanup old seen IDs (keep last 10000)
            if len(seen_tweet_ids) > 10000:
                seen_tweet_ids = set(list(seen_tweet_ids)[-5000:])

            logger.info(f"Poll cycle complete. Sleeping {interval_seconds}s...")
            time.sleep(interval_seconds)

        except Exception as e:
            logger.error(f"Poll cycle error: {e}", exc_info=True)
            time.sleep(interval_seconds)


def start_stream_monitor():
    """Start the X stream monitor (main entry point)"""
    # Initialize database
    init_database()

    # Create processor
    processor = TweetProcessor()

    if not processor.tracked_accounts:
        logger.warning("No tracked accounts found. Loading seed data...")
        from src.seed_accounts import seed_tracked_accounts
        seed_tracked_accounts()
        processor.load_tracked_accounts()

    # Try streaming first, fall back to polling
    try:
        if X_BEARER_TOKEN:
            logger.info("Starting X stream monitor...")
            stream = OriginStampStream(X_BEARER_TOKEN, processor)

            # Set up stream rules for tracked accounts
            # Note: X API v2 has limitations on filter rules
            # For production, you may need Elevated access

            # Start filtering
            stream.filter(
                tweet_fields=['created_at', 'author_id', 'text'],
                expansions=['author_id'],
                threaded=False
            )
        else:
            raise Exception("No bearer token, falling back to polling")

    except Exception as e:
        logger.warning(f"Streaming failed ({e}), using polling mode")
        poll_tracked_accounts(processor, interval_seconds=60)


def process_single_tweet(tweet_id: str) -> Dict:
    """
    Process a single tweet by ID (for testing/manual processing)

    Args:
        tweet_id: X tweet ID

    Returns:
        Processing result
    """
    client = get_api_client()
    processor = TweetProcessor()

    try:
        tweet = client.get_tweet(
            tweet_id,
            tweet_fields=['created_at', 'text', 'author_id'],
            expansions=['author_id']
        )

        if not tweet.data:
            return {"status": "error", "message": "Tweet not found"}

        # Get author username
        username = None
        if tweet.includes and 'users' in tweet.includes:
            for user in tweet.includes['users']:
                if user.id == tweet.data.author_id:
                    username = user.username
                    break

        tweet_data = {
            'id': tweet.data.id,
            'text': tweet.data.text,
            'created_at': tweet.data.created_at,
            'author_id': tweet.data.author_id,
            'username': username,
            'author': {'username': username}
        }

        return processor.process_tweet(tweet_data)

    except Exception as e:
        logger.error(f"Error processing tweet {tweet_id}: {e}")
        return {"status": "error", "message": str(e)}
