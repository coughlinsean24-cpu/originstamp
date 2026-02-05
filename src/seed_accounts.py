"""
Seed Accounts Data
Initial tracked accounts for OriginStamp
"""
import logging
from src.database import get_db_connection, get_connection_type

logger = logging.getLogger(__name__)

# MIDDLE EAST & IRAN FOCUSED + WIRE SERVICES
# Expanded for better first-reporter coverage
# ~45 accounts × 10 tweets × 4 polls/hr × 24h = ~43,000 reads/day
# 15 min polling keeps costs reasonable

# Tier 1A - OSINT (fastest breaking news)
TIER_1A_OSINT = [
    ('OSINTdefender', 0.98, 'Very fast ME coverage'),
    ('sentdefender', 0.97, 'Rapid breaking news'),
    ('Faytuks', 0.96, 'Middle East conflicts specialist'),
    ('IntelDoge', 0.95, 'Breaking news ME focus'),
    ('WarMonitors', 0.95, 'War monitoring'),
    ('OSINT613', 0.97, 'Israel-specific OSINT'),
    ('IsraelRadar_com', 0.95, 'Israeli airspace/radar'),
    ('aurora_intel', 0.94, 'Global conflict tracking'),
    ('ELINTNews', 0.93, 'Electronic intel news'),
    ('NotWoofers', 0.92, 'OSINT aggregator'),
    ('Global_Mil_Info', 0.91, 'Military intel'),
    ('no_itsmyturn', 0.90, 'ME OSINT'),
]

# Tier 1B - Wire Services & Major Outlets
TIER_1B_WIRE = [
    ('Reuters', 0.99, 'Reuters news agency'),
    ('ReutersWorld', 0.99, 'Reuters world news'),
    ('AFP', 0.99, 'Agence France-Presse'),
    ('AP', 0.99, 'Associated Press'),
    ('BBCBreaking', 0.97, 'BBC Breaking News'),
    ('BBCWorld', 0.96, 'BBC World News'),
    ('CNNBreaking', 0.95, 'CNN Breaking News'),
    ('WSJ', 0.96, 'Wall Street Journal'),
]

# Tier 1C - Official/Regional Sources
TIER_1C_OFFICIAL = [
    ('IDF', 0.95, 'Official IDF account'),
    ('IsraelMFA', 0.94, 'Israel Ministry of Foreign Affairs'),
    ('IsraeliPM', 0.94, 'Israeli PM Office'),
    ('AJABreaking', 0.90, 'Al Jazeera breaking'),
    ('AJEnglish', 0.89, 'Al Jazeera English'),
    ('IranIntl', 0.82, 'Iran International'),
    ('IranIntl_En', 0.82, 'Iran International English'),
    ('kann_news', 0.88, 'Israeli public broadcaster'),
    ('TimesofIsrael', 0.90, 'Times of Israel'),
    ('Jerusalem_Post', 0.89, 'Jerusalem Post'),
    ('i24NEWS_EN', 0.88, 'i24 News English'),
    ('haborijiaraborijietaborijiz', 0.85, 'Haaretz newspaper'),
]

# Tier 1D - Key Journalists (ME specialists)
TIER_1D_JOURNALISTS = [
    ('BarakRavid', 0.92, 'Barak Ravid - Axios Israel'),
    ('RichardEngel', 0.94, 'NBC Chief Foreign Correspondent'),
    ('ClarissaWard', 0.94, 'CNN Chief International Correspondent'),
    ('AmichaiStein1', 0.91, 'Amichai Stein - i24NEWS'),
    ('JacobMagid', 0.90, 'Times of Israel reporter'),
    ('AnshelPfeffer', 0.89, 'Haaretz correspondent'),
    ('LizSly', 0.91, 'Washington Post ME bureau chief'),
    ('ragaborijipsoylu', 0.88, 'Turkey/ME journalist'),
]

# Tier 2 - Iran/Hezbollah/Regional Specialists
TIER_2_AMPLIFIER = [
    ('Joyce_Karam', 0.87, 'ME correspondent - Al Monitor'),
    ('IntelCrab', 0.93, 'OSINT with Iran coverage'),
    ('JasonMBrodsky', 0.86, 'Iran policy analyst'),
    ('Ali_Vaez', 0.85, 'ICG Iran analyst'),
]

def seed_tracked_accounts():
    """Insert all seed accounts into the database and remove old ones"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        conn_type = get_connection_type()

        all_accounts = [
            ('1A_OSINT', TIER_1A_OSINT),
            ('1B_WIRE', TIER_1B_WIRE),
            ('1C_OFFICIAL', TIER_1C_OFFICIAL),
            ('1D_JOURNALIST', TIER_1D_JOURNALISTS),
            ('2_AMPLIFIER', TIER_2_AMPLIFIER),
        ]

        # Collect all valid usernames
        valid_usernames = []
        for tier, accounts in all_accounts:
            for account_data in accounts:
                valid_usernames.append(account_data[0].lower())

        # Delete accounts not in current list
        if conn_type == 'postgresql':
            placeholders = ','.join(['%s'] * len(valid_usernames))
            cur.execute(f"""
                DELETE FROM tracked_accounts
                WHERE LOWER(account) NOT IN ({placeholders})
            """, valid_usernames)
        else:
            placeholders = ','.join(['?'] * len(valid_usernames))
            cur.execute(f"""
                DELETE FROM tracked_accounts
                WHERE LOWER(account) NOT IN ({placeholders})
            """, valid_usernames)

        deleted = cur.rowcount
        if deleted > 0:
            logger.info(f"Removed {deleted} old tracked accounts")

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
