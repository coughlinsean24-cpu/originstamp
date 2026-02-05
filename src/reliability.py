"""
Account Reliability Scoring Module
Dynamic reliability calculation based on account behavior
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from src.database import get_db_connection, dict_from_row, get_connection_type

logger = logging.getLogger(__name__)


def calculate_reliability_score(account: str) -> float:
    """
    Calculate dynamic reliability score for an account

    Factors:
    - Original report ratio (40%)
    - Verification rate (30%)
    - False alarm rate (20%)
    - Account tier bonus (10%)

    Returns:
        Score from 0.00 to 1.00
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        if conn_type == 'postgresql':
            cur.execute("""
                SELECT * FROM account_metrics WHERE account = %s
            """, (account,))
        else:
            cur.execute("""
                SELECT * FROM account_metrics WHERE account = ?
            """, (account,))

        metrics = dict_from_row(cur.fetchone())

        if not metrics:
            return 0.50  # Default for unknown accounts

        total = metrics.get('total_tweets_tracked', 0)
        if total == 0:
            return 0.50

        originals = metrics.get('total_original_reports', 0)
        reposts = metrics.get('total_reposts', 0)
        updates = metrics.get('total_updates', 0)
        false_alarms = metrics.get('false_alarm_count', 0)
        tier = metrics.get('tier', '3_SECONDARY')

        # Factor 1: Original report ratio (40%)
        original_ratio = originals / total
        original_score = original_ratio * 0.40

        # Factor 2: Update contribution (30%)
        # Updates are valuable - shows they add information
        update_ratio = updates / total if total > 0 else 0
        update_score = min(update_ratio * 2, 0.30)  # Cap at 0.30

        # Factor 3: False alarm penalty (20%)
        false_alarm_ratio = false_alarms / total if total > 0 else 0
        false_alarm_score = (1 - false_alarm_ratio) * 0.20

        # Factor 4: Tier bonus (10%)
        tier_bonuses = {
            '1A_OSINT': 0.10,
            '1B_OFFICIAL': 0.10,
            '1C_WIRE': 0.10,
            '2_AMPLIFIER': 0.05,
            '3_SECONDARY': 0.02,
            '3_VERIFICATION': 0.08,
        }
        tier_score = tier_bonuses.get(tier, 0.02)

        # Calculate final score
        final_score = original_score + update_score + false_alarm_score + tier_score

        # Clamp to 0.00 - 1.00
        return max(0.00, min(1.00, final_score))


def update_reliability_scores():
    """
    Recalculate reliability scores for all tracked accounts
    Should be run periodically (e.g., daily)
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        # Get all accounts with metrics
        cur.execute("SELECT account FROM account_metrics")
        accounts = [row[0] if isinstance(row, tuple) else row['account']
                   for row in cur.fetchall()]

        updated = 0
        for account in accounts:
            score = calculate_reliability_score(account)

            if conn_type == 'postgresql':
                cur.execute("""
                    UPDATE account_metrics
                    SET reliability_score = %s, last_updated = NOW()
                    WHERE account = %s
                """, (score, account))
            else:
                cur.execute("""
                    UPDATE account_metrics
                    SET reliability_score = ?, last_updated = CURRENT_TIMESTAMP
                    WHERE account = ?
                """, (score, account))

            updated += 1

        conn.commit()
        logger.info(f"Updated reliability scores for {updated} accounts")


def get_reliability_leaderboard(limit: int = 50) -> List[Dict]:
    """
    Get accounts ranked by reliability score

    Returns:
        List of accounts with their metrics
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        if conn_type == 'postgresql':
            cur.execute("""
                SELECT
                    am.*,
                    ta.notes
                FROM account_metrics am
                LEFT JOIN tracked_accounts ta ON am.account = ta.account
                WHERE am.total_tweets_tracked >= 10
                ORDER BY am.reliability_score DESC NULLS LAST
                LIMIT %s
            """, (limit,))
        else:
            cur.execute("""
                SELECT
                    am.*,
                    ta.notes
                FROM account_metrics am
                LEFT JOIN tracked_accounts ta ON am.account = ta.account
                WHERE am.total_tweets_tracked >= 10
                ORDER BY am.reliability_score DESC
                LIMIT ?
            """, (limit,))

        return [dict_from_row(row) for row in cur.fetchall()]


def detect_verification_chain(event_id: int) -> Dict:
    """
    Analyze how a claim spread and was verified

    Returns:
        Dict with verification analysis
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        # Get the original event
        if conn_type == 'postgresql':
            cur.execute("""
                SELECT ce.*, t.author_tier, t.author_reliability
                FROM canonical_events ce
                JOIN tweets t ON ce.first_tweet_id = t.id
                WHERE ce.id = %s
            """, (event_id,))
        else:
            cur.execute("""
                SELECT ce.*, t.author_tier, t.author_reliability
                FROM canonical_events ce
                JOIN tweets t ON ce.first_tweet_id = t.id
                WHERE ce.id = ?
            """, (event_id,))

        event = dict_from_row(cur.fetchone())
        if not event:
            return {"status": "error", "message": "Event not found"}

        # Get all reposts ordered by time
        if conn_type == 'postgresql':
            cur.execute("""
                SELECT r.*, t.author, t.author_tier, t.author_reliability, t.display_time
                FROM reposts r
                JOIN tweets t ON r.repost_tweet_id = t.id
                WHERE r.canonical_event_id = %s
                ORDER BY t.timestamp_et ASC
            """, (event_id,))
        else:
            cur.execute("""
                SELECT r.*, t.author, t.author_tier, t.author_reliability, t.display_time
                FROM reposts r
                JOIN tweets t ON r.repost_tweet_id = t.id
                WHERE r.canonical_event_id = ?
                ORDER BY t.timestamp_et ASC
            """, (event_id,))

        reposts = [dict_from_row(row) for row in cur.fetchall()]

        # Analyze verification chain
        tier_1_count = sum(1 for r in reposts if r.get('author_tier', '').startswith('1'))
        wire_verified = any(r.get('author_tier') == '1C_WIRE' for r in reposts)
        official_verified = any(r.get('author_tier') == '1B_OFFICIAL' for r in reposts)

        # Calculate average reliability of verifiers
        reliabilities = [r.get('author_reliability', 0) for r in reposts if r.get('author_reliability')]
        avg_reliability = sum(reliabilities) / len(reliabilities) if reliabilities else 0

        # Determine verification status
        if official_verified:
            verification_status = 'official_confirmed'
        elif wire_verified:
            verification_status = 'wire_verified'
        elif tier_1_count >= 3:
            verification_status = 'cross_verified'
        elif tier_1_count >= 1:
            verification_status = 'partially_verified'
        else:
            verification_status = 'unverified'

        # Calculate time to first verification
        time_to_verification = None
        for r in reposts:
            if r.get('author_tier', '').startswith('1'):
                time_to_verification = r.get('time_delta_seconds')
                break

        return {
            "event_id": event_id,
            "first_author": event.get('first_author'),
            "first_author_tier": event.get('author_tier'),
            "verification_status": verification_status,
            "tier_1_verifiers": tier_1_count,
            "wire_verified": wire_verified,
            "official_verified": official_verified,
            "avg_verifier_reliability": round(avg_reliability, 2),
            "time_to_verification_seconds": time_to_verification,
            "total_reposts": len(reposts)
        }


def mark_false_alarm(event_id: int, reason: str = None):
    """
    Mark an event as a false alarm (claim not verified)
    Updates the original author's false alarm count
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        # Get event and author
        if conn_type == 'postgresql':
            cur.execute("""
                SELECT first_author FROM canonical_events WHERE id = %s
            """, (event_id,))
        else:
            cur.execute("""
                SELECT first_author FROM canonical_events WHERE id = ?
            """, (event_id,))

        result = cur.fetchone()
        if not result:
            return

        author = result[0] if isinstance(result, tuple) else result['first_author']

        # Update event status
        if conn_type == 'postgresql':
            cur.execute("""
                UPDATE canonical_events
                SET verification_status = 'disputed', last_updated = NOW()
                WHERE id = %s
            """, (event_id,))

            # Increment false alarm count
            cur.execute("""
                UPDATE account_metrics
                SET false_alarm_count = false_alarm_count + 1, last_updated = NOW()
                WHERE account = %s
            """, (author,))
        else:
            cur.execute("""
                UPDATE canonical_events
                SET verification_status = 'disputed', last_updated = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (event_id,))

            cur.execute("""
                UPDATE account_metrics
                SET false_alarm_count = false_alarm_count + 1, last_updated = CURRENT_TIMESTAMP
                WHERE account = ?
            """, (author,))

        conn.commit()
        logger.info(f"Marked event {event_id} as false alarm (author: {author})")
