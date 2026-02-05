"""
Database Operations Module
PostgreSQL operations for tweet storage and retrieval
"""
import os
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from contextlib import contextmanager

# Use psycopg2 for PostgreSQL
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# Fallback to SQLite for local development
import sqlite3

from src.config import DATABASE_URL, LOOKBACK_DAYS_PRIMARY


def get_connection_type():
    """Determine if using PostgreSQL or SQLite"""
    if DATABASE_URL and DATABASE_URL.startswith('postgresql') and HAS_PSYCOPG2:
        # Test if PostgreSQL is actually reachable
        try:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL)
            conn.close()
            return 'postgresql'
        except Exception:
            pass
    return 'sqlite'


@contextmanager
def get_db_connection():
    """Get database connection (PostgreSQL or SQLite)"""
    conn_type = get_connection_type()

    if conn_type == 'postgresql' and HAS_PSYCOPG2:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        try:
            yield conn
        finally:
            conn.close()
    else:
        # SQLite fallback for local development
        db_path = os.path.join(os.path.dirname(__file__), '..', 'originstamp.db')
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()


def dict_from_row(row):
    """Convert database row to dict"""
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    return dict(row)


def init_database():
    """Initialize database tables"""
    conn_type = get_connection_type()

    with get_db_connection() as conn:
        cur = conn.cursor()

        if conn_type == 'postgresql':
            # PostgreSQL schema
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tweets (
                    id BIGSERIAL PRIMARY KEY,
                    tweet_id VARCHAR(50) UNIQUE NOT NULL,
                    author VARCHAR(100) NOT NULL,
                    author_tier VARCHAR(20),
                    author_reliability DECIMAL(3,2),
                    text TEXT NOT NULL,
                    text_normalized TEXT,
                    text_hash VARCHAR(64),
                    event_hash VARCHAR(64),
                    timestamp_utc TIMESTAMP NOT NULL,
                    timestamp_et TIMESTAMP NOT NULL,
                    display_time VARCHAR(50),
                    language VARCHAR(10),
                    is_translation BOOLEAN DEFAULT FALSE,
                    translation_of BIGINT,
                    quoted_tweet_id VARCHAR(50),
                    reply_to_tweet_id VARCHAR(50),
                    retweet_of_tweet_id VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_timestamp_utc ON tweets(timestamp_utc)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_timestamp_et ON tweets(timestamp_et)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_author ON tweets(author)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_text_hash ON tweets(text_hash)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_event_hash ON tweets(event_hash)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_tweet_id ON tweets(tweet_id)")

        else:
            # SQLite schema
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tweets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id TEXT UNIQUE NOT NULL,
                    author TEXT NOT NULL,
                    author_tier TEXT,
                    author_reliability REAL,
                    text TEXT NOT NULL,
                    text_normalized TEXT,
                    text_hash TEXT,
                    event_hash TEXT,
                    timestamp_utc TEXT NOT NULL,
                    timestamp_et TEXT NOT NULL,
                    display_time TEXT,
                    language TEXT,
                    is_translation INTEGER DEFAULT 0,
                    translation_of INTEGER,
                    quoted_tweet_id TEXT,
                    reply_to_tweet_id TEXT,
                    retweet_of_tweet_id TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_timestamp_utc ON tweets(timestamp_utc)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_author ON tweets(author)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_text_hash ON tweets(text_hash)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tweets_event_hash ON tweets(event_hash)")

        # Media table
        if conn_type == 'postgresql':
            cur.execute("""
                CREATE TABLE IF NOT EXISTS media (
                    id BIGSERIAL PRIMARY KEY,
                    tweet_id BIGINT REFERENCES tweets(id) ON DELETE CASCADE,
                    media_url TEXT NOT NULL,
                    media_type VARCHAR(20),
                    perceptual_hash VARCHAR(64),
                    sha256_hash VARCHAR(64),
                    width INT,
                    height INT,
                    first_seen TIMESTAMP DEFAULT NOW()
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id INTEGER,
                    media_url TEXT NOT NULL,
                    media_type TEXT,
                    perceptual_hash TEXT,
                    sha256_hash TEXT,
                    width INTEGER,
                    height INTEGER,
                    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (tweet_id) REFERENCES tweets(id) ON DELETE CASCADE
                )
            """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_media_phash ON media(perceptual_hash)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_media_sha256 ON media(sha256_hash)")

        # Entities table
        if conn_type == 'postgresql':
            cur.execute("""
                CREATE TABLE IF NOT EXISTS entities (
                    id BIGSERIAL PRIMARY KEY,
                    tweet_id BIGINT REFERENCES tweets(id) ON DELETE CASCADE,
                    entity_type VARCHAR(50),
                    entity_value VARCHAR(255),
                    confidence DECIMAL(3,2)
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS entities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id INTEGER,
                    entity_type TEXT,
                    entity_value TEXT,
                    confidence REAL,
                    FOREIGN KEY (tweet_id) REFERENCES tweets(id) ON DELETE CASCADE
                )
            """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_entities_tweet_id ON entities(tweet_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_entities_type_value ON entities(entity_type, entity_value)")

        # URLs table
        if conn_type == 'postgresql':
            cur.execute("""
                CREATE TABLE IF NOT EXISTS urls (
                    id BIGSERIAL PRIMARY KEY,
                    tweet_id BIGINT REFERENCES tweets(id) ON DELETE CASCADE,
                    url_original TEXT,
                    url_expanded TEXT,
                    url_canonical TEXT,
                    domain VARCHAR(255)
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id INTEGER,
                    url_original TEXT,
                    url_expanded TEXT,
                    url_canonical TEXT,
                    domain TEXT,
                    FOREIGN KEY (tweet_id) REFERENCES tweets(id) ON DELETE CASCADE
                )
            """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_urls_canonical ON urls(url_canonical)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain)")

        # Canonical events table
        if conn_type == 'postgresql':
            cur.execute("""
                CREATE TABLE IF NOT EXISTS canonical_events (
                    id BIGSERIAL PRIMARY KEY,
                    event_hash VARCHAR(64) UNIQUE NOT NULL,
                    first_tweet_id BIGINT REFERENCES tweets(id),
                    first_timestamp_utc TIMESTAMP NOT NULL,
                    first_timestamp_et TIMESTAMP NOT NULL,
                    first_display_time VARCHAR(50),
                    first_author VARCHAR(100),
                    claim_summary TEXT,
                    verification_status VARCHAR(20) DEFAULT 'unverified',
                    repost_count INT DEFAULT 0,
                    update_count INT DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT NOW()
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS canonical_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_hash TEXT UNIQUE NOT NULL,
                    first_tweet_id INTEGER,
                    first_timestamp_utc TEXT NOT NULL,
                    first_timestamp_et TEXT NOT NULL,
                    first_display_time TEXT,
                    first_author TEXT,
                    claim_summary TEXT,
                    verification_status TEXT DEFAULT 'unverified',
                    repost_count INTEGER DEFAULT 0,
                    update_count INTEGER DEFAULT 0,
                    last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (first_tweet_id) REFERENCES tweets(id)
                )
            """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_hash ON canonical_events(event_hash)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON canonical_events(first_timestamp_et)")

        # Reposts table
        if conn_type == 'postgresql':
            cur.execute("""
                CREATE TABLE IF NOT EXISTS reposts (
                    id BIGSERIAL PRIMARY KEY,
                    canonical_event_id BIGINT REFERENCES canonical_events(id) ON DELETE CASCADE,
                    repost_tweet_id BIGINT REFERENCES tweets(id),
                    time_delta_seconds INT,
                    time_delta_display VARCHAR(50),
                    confidence_score DECIMAL(5,2),
                    classification VARCHAR(20),
                    added_new_info BOOLEAN DEFAULT FALSE,
                    new_info_summary TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS reposts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical_event_id INTEGER,
                    repost_tweet_id INTEGER,
                    time_delta_seconds INTEGER,
                    time_delta_display TEXT,
                    confidence_score REAL,
                    classification TEXT,
                    added_new_info INTEGER DEFAULT 0,
                    new_info_summary TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (canonical_event_id) REFERENCES canonical_events(id) ON DELETE CASCADE,
                    FOREIGN KEY (repost_tweet_id) REFERENCES tweets(id)
                )
            """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_reposts_canonical ON reposts(canonical_event_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reposts_tweet ON reposts(repost_tweet_id)")

        # Account metrics table
        if conn_type == 'postgresql':
            cur.execute("""
                CREATE TABLE IF NOT EXISTS account_metrics (
                    id BIGSERIAL PRIMARY KEY,
                    account VARCHAR(100) UNIQUE NOT NULL,
                    tier VARCHAR(20),
                    reliability_score DECIMAL(3,2),
                    total_tweets_tracked INT DEFAULT 0,
                    total_original_reports INT DEFAULT 0,
                    total_reposts INT DEFAULT 0,
                    total_updates INT DEFAULT 0,
                    total_corrections INT DEFAULT 0,
                    avg_time_to_verification_seconds INT,
                    false_alarm_count INT DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT NOW()
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS account_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account TEXT UNIQUE NOT NULL,
                    tier TEXT,
                    reliability_score REAL,
                    total_tweets_tracked INTEGER DEFAULT 0,
                    total_original_reports INTEGER DEFAULT 0,
                    total_reposts INTEGER DEFAULT 0,
                    total_updates INTEGER DEFAULT 0,
                    total_corrections INTEGER DEFAULT 0,
                    avg_time_to_verification_seconds INTEGER,
                    false_alarm_count INTEGER DEFAULT 0,
                    last_updated TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_account_metrics_account ON account_metrics(account)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_account_metrics_reliability ON account_metrics(reliability_score)")

        # Tracked accounts table
        if conn_type == 'postgresql':
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tracked_accounts (
                    id BIGSERIAL PRIMARY KEY,
                    account VARCHAR(100) UNIQUE NOT NULL,
                    tier VARCHAR(20) NOT NULL,
                    initial_reliability DECIMAL(3,2) DEFAULT 0.85,
                    date_added TIMESTAMP DEFAULT NOW(),
                    added_by VARCHAR(20) DEFAULT 'manual',
                    is_active BOOLEAN DEFAULT TRUE,
                    notes TEXT
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tracked_accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account TEXT UNIQUE NOT NULL,
                    tier TEXT NOT NULL,
                    initial_reliability REAL DEFAULT 0.85,
                    date_added TEXT DEFAULT CURRENT_TIMESTAMP,
                    added_by TEXT DEFAULT 'manual',
                    is_active INTEGER DEFAULT 1,
                    notes TEXT
                )
            """)

        conn.commit()
        print("Database tables initialized successfully")


def insert_tweet(tweet_data: Dict) -> int:
    """
    Insert tweet into database
    Returns: tweet database ID
    """
    with get_db_connection() as conn:
        cur = conn.cursor()

        # Check if tweet already exists
        cur.execute("SELECT id FROM tweets WHERE tweet_id = ?", (tweet_data['tweet_id'],))
        existing = cur.fetchone()
        if existing:
            return dict_from_row(existing)['id']

        conn_type = get_connection_type()

        if conn_type == 'postgresql':
            query = """
                INSERT INTO tweets (
                    tweet_id, author, author_tier, author_reliability,
                    text, text_normalized, text_hash, event_hash,
                    timestamp_utc, timestamp_et, display_time,
                    language, is_translation, translation_of,
                    quoted_tweet_id, reply_to_tweet_id, retweet_of_tweet_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
        else:
            query = """
                INSERT INTO tweets (
                    tweet_id, author, author_tier, author_reliability,
                    text, text_normalized, text_hash, event_hash,
                    timestamp_utc, timestamp_et, display_time,
                    language, is_translation, translation_of,
                    quoted_tweet_id, reply_to_tweet_id, retweet_of_tweet_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

        params = (
            tweet_data['tweet_id'],
            tweet_data['author'],
            tweet_data.get('author_tier'),
            tweet_data.get('author_reliability'),
            tweet_data['text'],
            tweet_data.get('text_normalized'),
            tweet_data.get('text_hash'),
            tweet_data.get('event_hash'),
            str(tweet_data['timestamp_utc']),
            str(tweet_data['timestamp_et']),
            tweet_data.get('display_time'),
            tweet_data.get('language'),
            tweet_data.get('is_translation', False),
            tweet_data.get('translation_of'),
            tweet_data.get('quoted_tweet_id'),
            tweet_data.get('reply_to_tweet_id'),
            tweet_data.get('retweet_of_tweet_id')
        )

        cur.execute(query, params)

        if conn_type == 'postgresql':
            tweet_db_id = cur.fetchone()['id']
        else:
            tweet_db_id = cur.lastrowid

        conn.commit()
        return tweet_db_id


def insert_entities(tweet_db_id: int, entities: List[Dict]):
    """Insert extracted entities"""
    if not entities:
        return

    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        for entity in entities:
            if conn_type == 'postgresql':
                cur.execute("""
                    INSERT INTO entities (tweet_id, entity_type, entity_value, confidence)
                    VALUES (%s, %s, %s, %s)
                """, (tweet_db_id, entity['type'], entity['value'], entity['confidence']))
            else:
                cur.execute("""
                    INSERT INTO entities (tweet_id, entity_type, entity_value, confidence)
                    VALUES (?, ?, ?, ?)
                """, (tweet_db_id, entity['type'], entity['value'], entity['confidence']))

        conn.commit()


def insert_urls(tweet_db_id: int, urls: List[Dict]):
    """Insert URLs"""
    if not urls:
        return

    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        for url in urls:
            if conn_type == 'postgresql':
                cur.execute("""
                    INSERT INTO urls (tweet_id, url_original, url_expanded, url_canonical, domain)
                    VALUES (%s, %s, %s, %s, %s)
                """, (tweet_db_id, url['original'], url['expanded'], url['canonical'], url['domain']))
            else:
                cur.execute("""
                    INSERT INTO urls (tweet_id, url_original, url_expanded, url_canonical, domain)
                    VALUES (?, ?, ?, ?, ?)
                """, (tweet_db_id, url['original'], url['expanded'], url['canonical'], url['domain']))

        conn.commit()


def find_similar_tweets(search_type: str, search_value: str,
                       lookback_days: int = None) -> List[Dict]:
    """
    Find similar tweets by various criteria

    Args:
        search_type: 'text_hash', 'event_hash', or 'entity'
        search_value: The value to search for
        lookback_days: How far back to search

    Returns:
        List of matching tweets
    """
    if lookback_days is None:
        lookback_days = LOOKBACK_DAYS_PRIMARY

    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        cutoff_time = datetime.utcnow() - timedelta(days=lookback_days)

        if search_type == 'text_hash':
            if conn_type == 'postgresql':
                cur.execute("""
                    SELECT t.*, ce.id as canonical_event_id
                    FROM tweets t
                    LEFT JOIN canonical_events ce ON t.id = ce.first_tweet_id
                    WHERE t.text_hash = %s AND t.timestamp_utc >= %s
                    ORDER BY t.timestamp_et ASC
                    LIMIT 20
                """, (search_value, cutoff_time))
            else:
                cur.execute("""
                    SELECT t.*, ce.id as canonical_event_id
                    FROM tweets t
                    LEFT JOIN canonical_events ce ON t.id = ce.first_tweet_id
                    WHERE t.text_hash = ? AND t.timestamp_utc >= ?
                    ORDER BY t.timestamp_et ASC
                    LIMIT 20
                """, (search_value, str(cutoff_time)))

        elif search_type == 'event_hash':
            if conn_type == 'postgresql':
                cur.execute("""
                    SELECT t.*, ce.id as canonical_event_id
                    FROM tweets t
                    LEFT JOIN canonical_events ce ON t.id = ce.first_tweet_id
                    WHERE t.event_hash = %s AND t.timestamp_utc >= %s
                    ORDER BY t.timestamp_et ASC
                    LIMIT 20
                """, (search_value, cutoff_time))
            else:
                cur.execute("""
                    SELECT t.*, ce.id as canonical_event_id
                    FROM tweets t
                    LEFT JOIN canonical_events ce ON t.id = ce.first_tweet_id
                    WHERE t.event_hash = ? AND t.timestamp_utc >= ?
                    ORDER BY t.timestamp_et ASC
                    LIMIT 20
                """, (search_value, str(cutoff_time)))

        elif search_type == 'entity':
            if conn_type == 'postgresql':
                cur.execute("""
                    SELECT DISTINCT t.*, ce.id as canonical_event_id
                    FROM tweets t
                    JOIN entities e ON t.id = e.tweet_id
                    LEFT JOIN canonical_events ce ON t.id = ce.first_tweet_id
                    WHERE LOWER(e.entity_value) = LOWER(%s) AND t.timestamp_utc >= %s
                    ORDER BY t.timestamp_et ASC
                    LIMIT 20
                """, (search_value, cutoff_time))
            else:
                cur.execute("""
                    SELECT DISTINCT t.*, ce.id as canonical_event_id
                    FROM tweets t
                    JOIN entities e ON t.id = e.tweet_id
                    LEFT JOIN canonical_events ce ON t.id = ce.first_tweet_id
                    WHERE LOWER(e.entity_value) = LOWER(?) AND t.timestamp_utc >= ?
                    ORDER BY t.timestamp_et ASC
                    LIMIT 20
                """, (search_value, str(cutoff_time)))

        results = cur.fetchall()
        return [dict_from_row(r) for r in results]


def create_canonical_event(tweet_db_id: int, event_hash: str,
                          claim_summary: str = None) -> int:
    """
    Create new canonical event for original report
    Returns: canonical_event ID
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        # Get tweet details
        if conn_type == 'postgresql':
            cur.execute("SELECT * FROM tweets WHERE id = %s", (tweet_db_id,))
        else:
            cur.execute("SELECT * FROM tweets WHERE id = ?", (tweet_db_id,))

        tweet = dict_from_row(cur.fetchone())

        if conn_type == 'postgresql':
            cur.execute("""
                INSERT INTO canonical_events (
                    event_hash, first_tweet_id, first_timestamp_utc,
                    first_timestamp_et, first_display_time, first_author,
                    claim_summary
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_hash) DO UPDATE SET last_updated = NOW()
                RETURNING id
            """, (
                event_hash,
                tweet_db_id,
                tweet['timestamp_utc'],
                tweet['timestamp_et'],
                tweet['display_time'],
                tweet['author'],
                claim_summary or tweet['text'][:200]
            ))
            event_id = cur.fetchone()['id']
        else:
            cur.execute("""
                INSERT OR IGNORE INTO canonical_events (
                    event_hash, first_tweet_id, first_timestamp_utc,
                    first_timestamp_et, first_display_time, first_author,
                    claim_summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                event_hash,
                tweet_db_id,
                tweet['timestamp_utc'],
                tweet['timestamp_et'],
                tweet['display_time'],
                tweet['author'],
                claim_summary or tweet['text'][:200]
            ))
            event_id = cur.lastrowid

            # If insert was ignored (conflict), get existing ID
            if event_id == 0:
                cur.execute("SELECT id FROM canonical_events WHERE event_hash = ?", (event_hash,))
                event_id = cur.fetchone()['id']

        conn.commit()
        return event_id


def add_repost(canonical_event_id: int, repost_tweet_id: int,
              classification: str, confidence: float,
              time_delta_seconds: int, time_delta_display: str,
              added_new_info: bool, new_info_summary: str = None):
    """Track repost/update to existing event"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        if conn_type == 'postgresql':
            cur.execute("""
                INSERT INTO reposts (
                    canonical_event_id, repost_tweet_id, time_delta_seconds,
                    time_delta_display, confidence_score, classification,
                    added_new_info, new_info_summary
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                canonical_event_id, repost_tweet_id, time_delta_seconds,
                time_delta_display, confidence, classification,
                added_new_info, new_info_summary
            ))

            # Update canonical event counts
            if classification == 'REPOST':
                cur.execute("""
                    UPDATE canonical_events
                    SET repost_count = repost_count + 1, last_updated = NOW()
                    WHERE id = %s
                """, (canonical_event_id,))
            elif classification == 'UPDATE':
                cur.execute("""
                    UPDATE canonical_events
                    SET update_count = update_count + 1, last_updated = NOW()
                    WHERE id = %s
                """, (canonical_event_id,))
        else:
            cur.execute("""
                INSERT INTO reposts (
                    canonical_event_id, repost_tweet_id, time_delta_seconds,
                    time_delta_display, confidence_score, classification,
                    added_new_info, new_info_summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                canonical_event_id, repost_tweet_id, time_delta_seconds,
                time_delta_display, confidence, classification,
                added_new_info, new_info_summary
            ))

            if classification == 'REPOST':
                cur.execute("""
                    UPDATE canonical_events
                    SET repost_count = repost_count + 1, last_updated = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (canonical_event_id,))
            elif classification == 'UPDATE':
                cur.execute("""
                    UPDATE canonical_events
                    SET update_count = update_count + 1, last_updated = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (canonical_event_id,))

        conn.commit()


def update_account_metrics(account: str, classification: str, tier: str = None):
    """Update account reliability and statistics"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        if conn_type == 'postgresql':
            # Upsert account metrics
            cur.execute("""
                INSERT INTO account_metrics (account, tier, total_tweets_tracked)
                VALUES (%s, %s, 1)
                ON CONFLICT (account)
                DO UPDATE SET
                    total_tweets_tracked = account_metrics.total_tweets_tracked + 1,
                    tier = COALESCE(%s, account_metrics.tier),
                    last_updated = NOW()
            """, (account, tier, tier))

            # Update classification-specific counts
            if classification == 'ORIGINAL':
                cur.execute("""
                    UPDATE account_metrics
                    SET total_original_reports = total_original_reports + 1
                    WHERE account = %s
                """, (account,))
            elif classification == 'REPOST':
                cur.execute("""
                    UPDATE account_metrics
                    SET total_reposts = total_reposts + 1
                    WHERE account = %s
                """, (account,))
            elif classification == 'UPDATE':
                cur.execute("""
                    UPDATE account_metrics
                    SET total_updates = total_updates + 1
                    WHERE account = %s
                """, (account,))
        else:
            # SQLite upsert
            cur.execute("""
                INSERT INTO account_metrics (account, tier, total_tweets_tracked)
                VALUES (?, ?, 1)
                ON CONFLICT(account) DO UPDATE SET
                    total_tweets_tracked = total_tweets_tracked + 1,
                    tier = COALESCE(?, tier),
                    last_updated = CURRENT_TIMESTAMP
            """, (account, tier, tier))

            if classification == 'ORIGINAL':
                cur.execute("""
                    UPDATE account_metrics
                    SET total_original_reports = total_original_reports + 1
                    WHERE account = ?
                """, (account,))
            elif classification == 'REPOST':
                cur.execute("""
                    UPDATE account_metrics
                    SET total_reposts = total_reposts + 1
                    WHERE account = ?
                """, (account,))
            elif classification == 'UPDATE':
                cur.execute("""
                    UPDATE account_metrics
                    SET total_updates = total_updates + 1
                    WHERE account = ?
                """, (account,))

        conn.commit()


def get_tracked_accounts() -> List[Dict]:
    """Get all tracked accounts"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        if conn_type == 'postgresql':
            cur.execute("""
                SELECT * FROM tracked_accounts
                WHERE is_active = TRUE
                ORDER BY tier, initial_reliability DESC
            """)
        else:
            cur.execute("""
                SELECT * FROM tracked_accounts
                WHERE is_active = 1
                ORDER BY tier, initial_reliability DESC
            """)

        return [dict_from_row(r) for r in cur.fetchall()]


def get_event_timeline(event_id: int) -> Dict:
    """Retrieve full timeline: original + all reposts/updates"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        # Get canonical event
        if conn_type == 'postgresql':
            cur.execute("""
                SELECT ce.*, t.text as original_text
                FROM canonical_events ce
                JOIN tweets t ON ce.first_tweet_id = t.id
                WHERE ce.id = %s
            """, (event_id,))
        else:
            cur.execute("""
                SELECT ce.*, t.text as original_text
                FROM canonical_events ce
                JOIN tweets t ON ce.first_tweet_id = t.id
                WHERE ce.id = ?
            """, (event_id,))

        event = dict_from_row(cur.fetchone())

        # Get all reposts/updates
        if conn_type == 'postgresql':
            cur.execute("""
                SELECT r.*, t.author, t.display_time, t.text
                FROM reposts r
                JOIN tweets t ON r.repost_tweet_id = t.id
                WHERE r.canonical_event_id = %s
                ORDER BY t.timestamp_et ASC
            """, (event_id,))
        else:
            cur.execute("""
                SELECT r.*, t.author, t.display_time, t.text
                FROM reposts r
                JOIN tweets t ON r.repost_tweet_id = t.id
                WHERE r.canonical_event_id = ?
                ORDER BY t.timestamp_et ASC
            """, (event_id,))

        reposts = [dict_from_row(r) for r in cur.fetchall()]

        return {
            "event": event,
            "reposts": reposts
        }


def get_recent_events(limit: int = 50) -> List[Dict]:
    """Get recent canonical events"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        if conn_type == 'postgresql':
            cur.execute("""
                SELECT ce.*, t.text as original_text
                FROM canonical_events ce
                JOIN tweets t ON ce.first_tweet_id = t.id
                ORDER BY ce.first_timestamp_et DESC
                LIMIT %s
            """, (limit,))
        else:
            cur.execute("""
                SELECT ce.*, t.text as original_text
                FROM canonical_events ce
                JOIN tweets t ON ce.first_tweet_id = t.id
                ORDER BY ce.first_timestamp_et DESC
                LIMIT ?
            """, (limit,))

        return [dict_from_row(r) for r in cur.fetchall()]


def search_events(query: str, limit: int = 50) -> List[Dict]:
    """Search events by text"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        search_term = f"%{query}%"

        if conn_type == 'postgresql':
            cur.execute("""
                SELECT ce.*, t.text as original_text
                FROM canonical_events ce
                JOIN tweets t ON ce.first_tweet_id = t.id
                WHERE t.text ILIKE %s OR ce.claim_summary ILIKE %s
                ORDER BY ce.first_timestamp_et DESC
                LIMIT %s
            """, (search_term, search_term, limit))
        else:
            cur.execute("""
                SELECT ce.*, t.text as original_text
                FROM canonical_events ce
                JOIN tweets t ON ce.first_tweet_id = t.id
                WHERE t.text LIKE ? OR ce.claim_summary LIKE ?
                ORDER BY ce.first_timestamp_et DESC
                LIMIT ?
            """, (search_term, search_term, limit))

        return [dict_from_row(r) for r in cur.fetchall()]
