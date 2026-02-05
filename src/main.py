"""
Main Entry Point for OriginStamp
Background worker that continuously monitors X stream
"""
import logging
import signal
import sys
import os
import argparse

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import LOG_LEVEL
from src.database import init_database
from src.seed_accounts import seed_tracked_accounts
from src.ingestion import start_stream_monitor, poll_tracked_accounts, TweetProcessor

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    logger.info("Shutdown signal received, stopping...")
    sys.exit(0)


def setup_database():
    """Initialize database and seed accounts"""
    logger.info("Initializing database...")
    init_database()

    logger.info("Seeding tracked accounts...")
    count = seed_tracked_accounts()
    logger.info(f"Seeded {count} tracked accounts")


def run_worker(mode: str = 'poll', interval: int = 60):
    """
    Run the background worker

    Args:
        mode: 'stream' for real-time streaming, 'poll' for periodic polling
        interval: Polling interval in seconds (only for poll mode)
    """
    logger.info(f"Starting OriginStamp worker in {mode} mode...")

    # Setup database
    setup_database()

    # Create processor
    processor = TweetProcessor()
    logger.info(f"Loaded {len(processor.tracked_accounts)} tracked accounts")

    if mode == 'stream':
        # Real-time streaming (requires Elevated API access)
        try:
            start_stream_monitor()
        except Exception as e:
            logger.warning(f"Streaming failed: {e}, falling back to polling")
            poll_tracked_accounts(processor, interval_seconds=interval)
    else:
        # Polling mode
        poll_tracked_accounts(processor, interval_seconds=interval)


def run_api(host: str = '0.0.0.0', port: int = 8000):
    """Run the FastAPI server"""
    import uvicorn
    from src.api import app

    # Initialize database
    setup_database()

    logger.info(f"Starting OriginStamp API on {host}:{port}...")
    uvicorn.run(app, host=host, port=port)


def main():
    """Main entry point with CLI argument parsing"""
    parser = argparse.ArgumentParser(description='OriginStamp - News Timestamp Verification')
    parser.add_argument('command', choices=['worker', 'api', 'setup', 'test'],
                       help='Command to run')
    parser.add_argument('--mode', choices=['stream', 'poll'], default='poll',
                       help='Worker mode (default: poll)')
    parser.add_argument('--interval', type=int, default=60,
                       help='Polling interval in seconds (default: 60)')
    parser.add_argument('--host', default='0.0.0.0',
                       help='API host (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8000,
                       help='API port (default: 8000)')

    args = parser.parse_args()

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        if args.command == 'worker':
            run_worker(mode=args.mode, interval=args.interval)

        elif args.command == 'api':
            run_api(host=args.host, port=args.port)

        elif args.command == 'setup':
            setup_database()
            logger.info("Setup complete!")

        elif args.command == 'test':
            # Quick test
            setup_database()

            processor = TweetProcessor()
            logger.info(f"Loaded {len(processor.tracked_accounts)} accounts")

            # Test fingerprinting
            from src.fingerprinting import create_tweet_fingerprint
            test_text = "Breaking: IDF confirms strike on Hezbollah targets in southern Lebanon"
            fp = create_tweet_fingerprint({'text': test_text})
            logger.info(f"Test fingerprint: {fp['text_hash'][:16]}...")
            logger.info(f"Entities: {fp['entities']}")

            logger.info("Test complete!")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
