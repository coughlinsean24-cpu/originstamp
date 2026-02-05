"""
OriginStamp Configuration
All environment variables and settings
"""
import os
from dotenv import load_dotenv

load_dotenv()

# X API credentials (already working)
X_API_KEY = os.getenv("X_API_KEY")
X_API_SECRET = os.getenv("X_API_SECRET")
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET")
X_CLIENT_ID = os.getenv("X_CLIENT_ID")
X_CLIENT_SECRET = os.getenv("X_CLIENT_SECRET")

# Database (Render managed or local)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/originstamp")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Timezone Settings
DISPLAY_TIMEZONE = "America/New_York"  # Eastern Time

# Similarity Detection Thresholds
SIMILARITY_THRESHOLD_REPOST = 85  # confidence score to classify as REPOST
SIMILARITY_THRESHOLD_UPDATE = 70   # confidence score to classify as UPDATE
SIMILARITY_THRESHOLD_RELATED = 50  # minimum to be considered RELATED

# Search Windows
LOOKBACK_DAYS_PRIMARY = 7          # primary search window
LOOKBACK_DAYS_MAJOR_EVENTS = 365   # extended search for major events
LOOKBACK_MINUTES_INDEPENDENT = 5   # window where reports might be independent

# Rate Limits
API_RATE_LIMIT_PER_HOUR = 100
STREAM_RECONNECT_DELAY = 60  # seconds

# Bot Settings
BOT_USERNAME = os.getenv("BOT_USERNAME", "newstimestamp")
POST_ORIGINAL_REPORTS = False  # Disabled - use digest instead (less botty)
REPLY_TO_REPOSTS = False  # Disabled for now
POST_TIMELINE_THREADS = False  # Disabled for now
MIN_REPOSTS_FOR_THREAD = 5  # minimum reposts before posting timeline thread

# Processing Settings
BATCH_SIZE = 100
MAX_CONCURRENT_REQUESTS = 10

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Account Tiers (for classification priority)
TIER_ORDER = {
    '1A_OSINT': 1,
    '1B_OFFICIAL': 2,
    '1C_WIRE': 3,
    '2_AMPLIFIER': 4,
    '3_SECONDARY': 5,
    '3_VERIFICATION': 5,
}

def get_tier_priority(tier: str) -> int:
    """Get numeric priority for tier (lower = higher priority)"""
    return TIER_ORDER.get(tier, 99)
