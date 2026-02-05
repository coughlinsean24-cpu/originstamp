"""
Seed Accounts Data
Initial tracked accounts for OriginStamp
"""
import logging
from src.database import get_db_connection, get_connection_type

logger = logging.getLogger(__name__)

# Tier 1A - OSINT (Critical Priority)
TIER_1A_OSINT = [
    ('OSINTdefender', 0.98, 'Very fast, high volume, excellent sourcing'),
    ('sentdefender', 0.97, 'Rapid breaking news'),
    ('OSINT613', 0.97, 'Israel-specific, excellent sourcing'),
    ('Faytuks', 0.96, 'Middle East conflicts, very active'),
    ('IsraelRadar_com', 0.95, 'Radar tracking, airspace activity'),
    ('RerumNovarum_mg', 0.94, 'Geopolitical analysis, good sourcing'),
    ('Conflicts', 0.96, 'Conflict monitoring'),
    ('IntelDoge', 0.95, 'Breaking news'),
    ('Archer83Able', 0.94, 'OSINT analysis'),
    ('WarMonitors', 0.95, 'War monitoring'),
    ('Osinttechnical', 0.94, 'Technical OSINT'),
    ('GeoConfirmed', 0.96, 'Geolocation verification'),
    ('TheIntelFrog', 0.93, 'Intelligence analysis'),
    ('IntelCrab', 0.93, 'OSINT updates'),
    ('Nrg8000', 0.92, 'Regional conflicts'),
    ('AuroraIntel', 0.93, 'Satellite and geolocation'),
    ('detresfa_', 0.92, 'Visual verification'),
    ('no_itsmyturn', 0.91, 'Geolocation specialist'),
    ('CalibreObscura', 0.92, 'Weapons identification'),
    ('IntelAir', 0.93, 'Aviation tracking'),
    ('GlobeNetNews', 0.91, 'Global news'),
    ('AirLiveNet', 0.92, 'Aviation incidents'),
    ('AlertChannel', 0.91, 'Breaking alerts'),
    ('Ninja_Warrior09', 0.90, 'OSINT monitoring'),
    ('200_zoka', 0.89, 'Regional updates'),
    ('worldonalert', 0.90, 'Global alerts'),
    ('Sprinter99880', 0.89, 'Breaking news'),
    ('TadeuszGiczan', 0.88, 'OSINT analyst'),
    ('UAWeapons', 0.88, 'Weapons tracking'),
    ('RALee85', 0.87, 'Military analysis'),
    ('michaelh992', 0.87, 'OSINT updates'),
    ('AggregateOsint', 0.88, 'Aggregated OSINT'),
    ('clashreport', 0.87, 'Conflict reporting'),
    ('manniefabian', 0.89, 'Israel defense correspondent'),
    ('EylonALevy', 0.88, 'Israeli spokesperson'),
    ('ynetalerts', 0.90, 'Israeli alerts'),
    ('MiddleEastSpect', 0.87, 'ME specialist'),
    ('ELINT_News', 0.88, 'Electronic intelligence'),
    ('NotWoofers', 0.86, 'OSINT updates'),
    ('CivMilAir', 0.87, 'Civil/military aviation'),
    ('YWNReporter', 0.86, 'Breaking alerts'),
    ('AlertsUkraine', 0.88, 'Regional conflicts'),
    ('visegrad24', 0.85, 'European/ME news'),
    ('KyleJGlen', 0.85, 'OSINT analyst'),
    ('Apex_WW', 0.86, 'Worldwide monitoring'),
]

# Tier 1B - Official/Primary Sources
TIER_1B_OFFICIAL = [
    ('IDF', 0.95, 'Official IDF account'),
    ('IDFSpokesperson', 0.95, 'IDF spokesperson'),
    ('Jerusalem_Post', 0.88, 'Israeli news'),
    ('kann_news', 0.87, 'Israeli public broadcaster'),
    ('GLZRadio', 0.86, 'IDF radio'),
    ('QudsNen', 0.82, 'Quds News Network'),
    ('PalestineChron', 0.80, 'Palestinian news'),
    ('AJABreaking', 0.90, 'Al Jazeera breaking'),
    ('PressTV', 0.75, 'Iranian state media'),
    ('Tasnimnews_EN', 0.76, 'Iranian news agency'),
    ('FarsNews_Agency', 0.74, 'Iranian news'),
    ('LebUpdate', 0.81, 'Lebanon updates'),
    ('Hezb_Press', 0.75, 'Hezbollah region'),
    ('SANA_English', 0.72, 'Syrian state media'),
    ('Dannymakkisyria', 0.78, 'Syrian news'),
]

# Tier 1C - Wire Services
TIER_1C_WIRE = [
    ('Reuters', 0.98, 'Reuters wire'),
    ('AP', 0.98, 'Associated Press'),
    ('AFP', 0.97, 'Agence France-Presse'),
    ('BBCBreaking', 0.96, 'BBC breaking'),
    ('BBCWorld', 0.95, 'BBC World'),
    ('AJENews', 0.92, 'Al Jazeera English'),
    ('i24NEWS_EN', 0.88, 'i24 News'),
]

# Tier 2 - Fast Amplifiers
TIER_2_AMPLIFIER = [
    ('Joyce_Karam', 0.87, 'ME correspondent'),
    ('MiddleEastEye', 0.85, 'ME news outlet'),
    ('hxhassan', 0.86, 'Hassan Hassan analyst'),
    ('Charles_Lister', 0.88, 'Syria expert'),
    ('AbbasiMahdieh', 0.84, 'Iran analyst'),
    ('Elizrael', 0.83, 'Israel/regional analyst'),
    ('NatashaBertrand', 0.86, 'National security'),
    ('JakobERobertson', 0.84, 'Regional analyst'),
    ('BNONews', 0.90, 'Breaking news'),
    ('spectatorindex', 0.83, 'News aggregator'),
    ('disclosetv', 0.82, 'Breaking news'),
    ('Breaking911', 0.81, 'Breaking news'),
    ('MiddleEastMnt', 0.82, 'ME monitor'),
    ('IsraelWarRoom', 0.80, 'Israel updates'),
    ('Gaza_Notifications', 0.78, 'Gaza updates'),
    ('Lebanon_News_', 0.79, 'Lebanon news'),
    ('YemenWar_', 0.77, 'Yemen conflict'),
    ('SyriaCivilWar_', 0.76, 'Syria conflict'),
]

# Tier 3 - Verification/Fact-Check
TIER_3_VERIFICATION = [
    ('bellingcat', 0.95, 'Investigative journalism'),
    ('N_Waters89', 0.93, 'Nick Waters - Bellingcat'),
    ('elintnews', 0.91, 'Electronic intelligence'),
]


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
