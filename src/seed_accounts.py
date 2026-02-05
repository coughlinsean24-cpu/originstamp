"""
Seed Accounts Data
Initial tracked accounts for OriginStamp
"""
import logging
from src.database import get_db_connection, get_connection_type

logger = logging.getLogger(__name__)

# Tier 1A - OSINT (Critical Priority)
# REDUCED TO TOP 10 to conserve API quota
# X Basic tier only allows 10,000 reads/month
TIER_1A_OSINT = [
    ('OSINTdefender', 0.98, 'Very fast, high volume, excellent sourcing'),
    ('sentdefender', 0.97, 'Rapid breaking news'),
    ('Faytuks', 0.96, 'Middle East conflicts, very active'),
    ('Conflicts', 0.96, 'Conflict monitoring'),
    ('IntelDoge', 0.95, 'Breaking news'),
    ('WarMonitors', 0.95, 'War monitoring'),
    ('GeoConfirmed', 0.96, 'Geolocation verification'),
    ('AuroraIntel', 0.93, 'Satellite and geolocation'),
    ('BNONews', 0.90, 'Breaking news aggregator'),
    ('spectatorindex', 0.83, 'News aggregator'),
]

# Other tiers disabled to conserve API quota
# Uncomment if you have X Pro tier ($5k/month)

TIER_1B_OFFICIAL = []
TIER_1C_WIRE = []
TIER_2_AMPLIFIER = []
TIER_3_VERIFICATION = []


def seed_tracked_accounts():
    """Insert all seed accounts into the database"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        all_accounts = [
            ('1A_OSINT', TIER_1A_OSINT),
            ('1B_OFFICIAL', TIER_1B_OFFICIAL),
            ('1C_WIRE', TIER_1C_WIRE),
            ('2_AMPLIFIER', TIER_2_AMPLIFIER),
            ('3_VERIFICATION', TIER_3_VERIFICATION),
        ]

        inserted = 0
        for tier, accounts in all_accounts:
            for account_data in accounts:
                username, reliability, notes = account_data

                try:
                    if conn_type == 'postgresql':
                        cur.execute("""
                            INSERT INTO tracked_accounts (account, tier, initial_reliability, notes)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (account) DO UPDATE SET
                                tier = EXCLUDED.tier,
                                initial_reliability = EXCLUDED.initial_reliability,
                                notes = EXCLUDED.notes
                        """, (username, tier, reliability, notes))
                    else:
                        cur.execute("""
                            INSERT OR REPLACE INTO tracked_accounts (account, tier, initial_reliability, notes)
                            VALUES (?, ?, ?, ?)
                        """, (username, tier, reliability, notes))

                    inserted += 1

                except Exception as e:
                    logger.error(f"Error inserting {username}: {e}")

        conn.commit()
        logger.info(f"Seeded {inserted} tracked accounts")
        return inserted


def get_account_counts() -> dict:
    """Get count of accounts by tier"""
    with get_db_connection() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT tier, COUNT(*) as count
            FROM tracked_accounts
            GROUP BY tier
            ORDER BY tier
        """)

        results = cur.fetchall()
        return {row[0]: row[1] for row in results}


if __name__ == "__main__":
    from src.database import init_database

    logging.basicConfig(level=logging.INFO)

    init_database()
    count = seed_tracked_accounts()
    print(f"Seeded {count} accounts")

    counts = get_account_counts()
    print("\nAccounts by tier:")
    for tier, count in counts.items():
        print(f"  {tier}: {count}")
