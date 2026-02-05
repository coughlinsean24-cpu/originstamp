"""
Content Fingerprinting Module
Handles text normalization, entity extraction, and event hashing
"""
import hashlib
import re
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# Lazy load spacy to avoid import errors if not installed
_nlp = None


def get_nlp():
    """Lazy load spaCy model"""
    global _nlp
    if _nlp is None:
        try:
            import spacy
            _nlp = spacy.load("en_core_web_sm")
        except OSError:
            # Model not downloaded, try to download it
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
            import spacy
            _nlp = spacy.load("en_core_web_sm")
    return _nlp


def normalize_text(text: str) -> str:
    """
    Normalize text for comparison:
    - Lowercase
    - Remove URLs
    - Remove mentions
    - Remove hashtags (keep the word)
    - Remove extra whitespace
    - Remove punctuation
    """
    if not text:
        return ""

    # Remove URLs
    text = re.sub(r'http\S+|www\S+', '', text)
    # Remove mentions
    text = re.sub(r'@\w+', '', text)
    # Remove hashtags (keep the word)
    text = re.sub(r'#(\w+)', r'\1', text)
    # Lowercase
    text = text.lower()
    # Remove punctuation except spaces
    text = re.sub(r'[^\w\s]', '', text)
    # Normalize whitespace
    text = ' '.join(text.split())

    return text.strip()


def hash_text(text: str) -> str:
    """Generate SHA256 hash of normalized text"""
    normalized = normalize_text(text)
    return hashlib.sha256(normalized.encode()).hexdigest()


def extract_entities(text: str) -> List[Dict]:
    """
    Extract named entities using spaCy

    Returns:
        List of dicts: [{'type': 'GPE', 'value': 'Tehran', 'confidence': 0.95}, ...]
    """
    if not text:
        return []

    try:
        nlp = get_nlp()
        doc = nlp(text)
        entities = []

        for ent in doc.ents:
            # Filter for relevant entity types
            if ent.label_ in ['GPE', 'LOC', 'ORG', 'PERSON', 'NORP', 'FAC', 'EVENT']:
                entities.append({
                    'type': ent.label_,
                    'value': ent.text,
                    'confidence': 0.90  # spaCy doesn't provide confidence
                })

        # Custom pattern matching for weapons, military units
        weapon_patterns = [
            r'\b([A-Z]-\d+[A-Z]?)\b',  # F-16, S-300
            r'\b([A-Z]{2,}-\d+)\b',     # IRGC-related
            r'\b(Iron Dome|Arrow|Patriot|THAAD)\b',
            r'\b(Merkava|Abrams|Leopard)\b',
            r'\b(Sejjil|Shahab|Fateh|Emad)\b',
            r'\b(Kornet|Javelin|TOW|Milan)\b',
        ]

        for pattern in weapon_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                entities.append({
                    'type': 'WEAPON',
                    'value': match,
                    'confidence': 0.85
                })

        # Military organizations
        military_orgs = [
            r'\b(IDF|IRGC|Hezbollah|Hamas|PIJ|Houthis?)\b',
            r'\b(Quds Force|Revolutionary Guard)\b',
            r'\b(Mossad|Shin Bet|AMAN)\b',
        ]

        for pattern in military_orgs:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                # Check if already extracted
                if not any(e['value'].lower() == match.lower() for e in entities):
                    entities.append({
                        'type': 'MILITARY_ORG',
                        'value': match,
                        'confidence': 0.95
                    })

        return entities

    except Exception as e:
        print(f"Entity extraction error: {e}")
        return []


def canonicalize_url(url: str) -> str:
    """
    Normalize URL by:
    - Removing tracking parameters
    - Normalizing domain (www vs non-www)
    - Removing fragments
    - Lowercasing
    """
    if not url:
        return ""

    try:
        parsed = urlparse(url)

        # Remove common tracking parameters
        tracking_params = [
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term',
            'utm_content', 'fbclid', 'gclid', 'ref', 'source',
            'mc_cid', 'mc_eid', '_ga', 's', 'share'
        ]

        query_params = parse_qs(parsed.query)
        filtered_params = {k: v for k, v in query_params.items()
                          if k.lower() not in tracking_params}

        # Normalize domain (remove www)
        domain = parsed.netloc.lower().replace('www.', '')

        # Rebuild URL
        canonical = urlunparse((
            parsed.scheme.lower() or 'https',
            domain,
            parsed.path.rstrip('/'),
            '',  # params
            urlencode(filtered_params, doseq=True) if filtered_params else '',
            ''   # fragment
        ))

        return canonical

    except Exception:
        return url


def extract_urls(text: str) -> List[Dict]:
    """
    Extract and normalize URLs from text

    Returns:
        List of dicts: [{'original': '...', 'canonical': '...', 'domain': '...'}, ...]
    """
    if not text:
        return []

    # Find all URLs
    url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
    urls = re.findall(url_pattern, text)

    result = []
    for url in urls:
        # Clean trailing punctuation
        url = url.rstrip('.,;:!?)')

        canonical = canonicalize_url(url)
        try:
            domain = urlparse(url).netloc.lower().replace('www.', '')
        except Exception:
            domain = ''

        result.append({
            'original': url,
            'expanded': url,  # Would need to resolve t.co links
            'canonical': canonical,
            'domain': domain
        })

    return result


def detect_language(text: str) -> str:
    """Detect language of text"""
    if not text:
        return 'unknown'

    try:
        from langdetect import detect
        return detect(text)
    except Exception:
        return 'unknown'


def generate_event_hash(text: str, entities: List[Dict] = None,
                       urls: List[str] = None) -> str:
    """
    Generate composite fingerprint for event matching
    Combines text hash, key entities, and URLs
    """
    # Normalize text
    norm_text = normalize_text(text)

    # Extract key entities (locations, orgs)
    if entities is None:
        entities = extract_entities(text)

    key_entities = sorted([
        e['value'].lower() for e in entities
        if e['type'] in ['GPE', 'LOC', 'ORG', 'WEAPON', 'MILITARY_ORG']
    ])

    # Canonical URLs
    if urls is None:
        url_dicts = extract_urls(text)
        urls = [u['canonical'] for u in url_dicts]

    canonical_urls = sorted(urls)

    # Combine into single string
    fingerprint = f"{norm_text}|{'|'.join(key_entities)}|{'|'.join(canonical_urls)}"

    return hashlib.sha256(fingerprint.encode()).hexdigest()


def extract_hashtags(text: str) -> List[str]:
    """Extract hashtags from text"""
    if not text:
        return []

    return re.findall(r'#(\w+)', text)


def extract_mentions(text: str) -> List[str]:
    """Extract @mentions from text"""
    if not text:
        return []

    return re.findall(r'@(\w+)', text)


def create_tweet_fingerprint(tweet_data: Dict) -> Dict:
    """
    Create complete fingerprint for a tweet

    Args:
        tweet_data: Dict with 'text', optional 'media', etc.

    Returns:
        Dict with all fingerprint components
    """
    text = tweet_data.get('text', '')

    # Normalize text
    text_normalized = normalize_text(text)
    text_hash = hash_text(text)

    # Extract entities
    entities = extract_entities(text)

    # Extract URLs
    urls = extract_urls(text)
    urls_canonical = [u['canonical'] for u in urls]

    # Generate event hash
    event_hash = generate_event_hash(text, entities, urls_canonical)

    # Detect language
    language = detect_language(text)

    # Extract hashtags and mentions
    hashtags = extract_hashtags(text)
    mentions = extract_mentions(text)

    return {
        'text_normalized': text_normalized,
        'text_hash': text_hash,
        'event_hash': event_hash,
        'entities': entities,
        'urls': urls,
        'urls_canonical': urls_canonical,
        'language': language,
        'hashtags': hashtags,
        'mentions': mentions
    }
