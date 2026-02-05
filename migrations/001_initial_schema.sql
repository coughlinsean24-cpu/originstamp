-- OriginStamp Initial Database Schema
-- PostgreSQL 15+

-- Main tweet storage
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
    translation_of BIGINT REFERENCES tweets(id),
    quoted_tweet_id VARCHAR(50),
    reply_to_tweet_id VARCHAR(50),
    retweet_of_tweet_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tweets_timestamp_utc ON tweets(timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_tweets_timestamp_et ON tweets(timestamp_et);
CREATE INDEX IF NOT EXISTS idx_tweets_author ON tweets(author);
CREATE INDEX IF NOT EXISTS idx_tweets_text_hash ON tweets(text_hash);
CREATE INDEX IF NOT EXISTS idx_tweets_event_hash ON tweets(event_hash);
CREATE INDEX IF NOT EXISTS idx_tweets_tweet_id ON tweets(tweet_id);

-- Media fingerprints
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
);

CREATE INDEX IF NOT EXISTS idx_media_phash ON media(perceptual_hash);
CREATE INDEX IF NOT EXISTS idx_media_sha256 ON media(sha256_hash);
CREATE INDEX IF NOT EXISTS idx_media_tweet_id ON media(tweet_id);

-- Extracted entities
CREATE TABLE IF NOT EXISTS entities (
    id BIGSERIAL PRIMARY KEY,
    tweet_id BIGINT REFERENCES tweets(id) ON DELETE CASCADE,
    entity_type VARCHAR(50),
    entity_value VARCHAR(255),
    confidence DECIMAL(3,2)
);

CREATE INDEX IF NOT EXISTS idx_entities_tweet_id ON entities(tweet_id);
CREATE INDEX IF NOT EXISTS idx_entities_type_value ON entities(entity_type, entity_value);

-- URLs extracted from tweets
CREATE TABLE IF NOT EXISTS urls (
    id BIGSERIAL PRIMARY KEY,
    tweet_id BIGINT REFERENCES tweets(id) ON DELETE CASCADE,
    url_original TEXT,
    url_expanded TEXT,
    url_canonical TEXT,
    domain VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_urls_canonical ON urls(url_canonical);
CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain);
CREATE INDEX IF NOT EXISTS idx_urls_tweet_id ON urls(tweet_id);

-- Canonical events (first occurrence of each claim)
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
);

CREATE INDEX IF NOT EXISTS idx_events_event_hash ON canonical_events(event_hash);
CREATE INDEX IF NOT EXISTS idx_events_first_timestamp_et ON canonical_events(first_timestamp_et);
CREATE INDEX IF NOT EXISTS idx_events_verification_status ON canonical_events(verification_status);

-- Repost tracking
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
);

CREATE INDEX IF NOT EXISTS idx_reposts_canonical ON reposts(canonical_event_id);
CREATE INDEX IF NOT EXISTS idx_reposts_tweet ON reposts(repost_tweet_id);
CREATE INDEX IF NOT EXISTS idx_reposts_classification ON reposts(classification);

-- Account reliability tracking
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
);

CREATE INDEX IF NOT EXISTS idx_account_metrics_account ON account_metrics(account);
CREATE INDEX IF NOT EXISTS idx_account_metrics_reliability ON account_metrics(reliability_score DESC);
CREATE INDEX IF NOT EXISTS idx_account_metrics_tier ON account_metrics(tier);

-- Account interaction network
CREATE TABLE IF NOT EXISTS account_interactions (
    id BIGSERIAL PRIMARY KEY,
    source_account VARCHAR(100) NOT NULL,
    target_account VARCHAR(100) NOT NULL,
    interaction_type VARCHAR(20),
    frequency INT DEFAULT 1,
    last_interaction TIMESTAMP,
    UNIQUE(source_account, target_account, interaction_type)
);

CREATE INDEX IF NOT EXISTS idx_interactions_source ON account_interactions(source_account);
CREATE INDEX IF NOT EXISTS idx_interactions_target ON account_interactions(target_account);

-- Tracked accounts configuration
CREATE TABLE IF NOT EXISTS tracked_accounts (
    id BIGSERIAL PRIMARY KEY,
    account VARCHAR(100) UNIQUE NOT NULL,
    tier VARCHAR(20) NOT NULL,
    initial_reliability DECIMAL(3,2) DEFAULT 0.85,
    date_added TIMESTAMP DEFAULT NOW(),
    added_by VARCHAR(20) DEFAULT 'manual',
    is_active BOOLEAN DEFAULT TRUE,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_tracked_accounts_account ON tracked_accounts(account);
CREATE INDEX IF NOT EXISTS idx_tracked_accounts_tier ON tracked_accounts(tier);
CREATE INDEX IF NOT EXISTS idx_tracked_accounts_active ON tracked_accounts(is_active);
