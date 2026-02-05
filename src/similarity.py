"""
Similarity Detection Module
Multi-factor scoring for detecting reposts vs originals
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import difflib

from src.utils.timezone import format_time_delta_seconds
from src.config import (
    SIMILARITY_THRESHOLD_REPOST,
    SIMILARITY_THRESHOLD_UPDATE,
    LOOKBACK_MINUTES_INDEPENDENT,
    get_tier_priority
)

# Lazy load sentence transformers
_model = None


def get_sentence_model():
    """Lazy load sentence transformer model"""
    global _model
    if _model is None:
        try:
            from sentence_transformers import SentenceTransformer
            _model = SentenceTransformer('all-MiniLM-L6-v2')
        except ImportError:
            _model = None
    return _model


def calculate_text_similarity(text1: str, text2: str) -> float:
    """
    Calculate text similarity using multiple methods
    Returns score 0-100
    """
    if not text1 or not text2:
        return 0

    text1 = text1.lower().strip()
    text2 = text2.lower().strip()

    # Method 1: Character-level (Levenshtein-based)
    ratio = difflib.SequenceMatcher(None, text1, text2).ratio()
    char_score = ratio * 100

    # Method 2: Token-level (Jaccard similarity)
    tokens1 = set(text1.split())
    tokens2 = set(text2.split())

    if not tokens1 or not tokens2:
        token_score = 0
    else:
        intersection = len(tokens1.intersection(tokens2))
        union = len(tokens1.union(tokens2))
        token_score = (intersection / union) * 100

    # Method 3: Semantic similarity (embeddings) - if available
    semantic_score = 0
    model = get_sentence_model()
    if model is not None:
        try:
            import numpy as np
            embeddings = model.encode([text1, text2])
            cosine_sim = np.dot(embeddings[0], embeddings[1]) / (
                np.linalg.norm(embeddings[0]) * np.linalg.norm(embeddings[1])
            )
            semantic_score = (cosine_sim + 1) / 2 * 100  # Scale from [-1,1] to [0,100]

            # Weighted average with semantic
            final_score = (char_score * 0.25 + token_score * 0.25 + semantic_score * 0.50)
        except Exception:
            # Fall back to non-semantic
            final_score = (char_score * 0.5 + token_score * 0.5)
    else:
        # No semantic model, use character and token only
        final_score = (char_score * 0.5 + token_score * 0.5)

    return final_score


def calculate_entity_overlap(entities1: List[Dict], entities2: List[Dict]) -> float:
    """
    Calculate percentage of overlapping entities
    Returns score 0-100
    """
    if not entities1 or not entities2:
        return 0

    # Extract entity values (lowercase for comparison)
    values1 = set(e['value'].lower() for e in entities1)
    values2 = set(e['value'].lower() for e in entities2)

    intersection = len(values1.intersection(values2))
    union = len(values1.union(values2))

    if union == 0:
        return 0

    return (intersection / union) * 100


def calculate_media_similarity(media_hashes1: List[str], media_hashes2: List[str]) -> float:
    """
    Check if media matches (exact or perceptual hash)
    Returns 100 for match, 0 for no match
    """
    if not media_hashes1 or not media_hashes2:
        return 0

    # Check for any exact matches
    set1 = set(media_hashes1)
    set2 = set(media_hashes2)

    if set1.intersection(set2):
        return 100

    # Could add perceptual hash distance check here for near-matches
    return 0


def calculate_url_similarity(urls1: List[str], urls2: List[str]) -> float:
    """
    Check if URLs match
    Returns 100 for match, 0 for no match
    """
    if not urls1 or not urls2:
        return 0

    set1 = set(urls1)
    set2 = set(urls2)

    if set1.intersection(set2):
        return 100

    return 0


def calculate_similarity_score(tweet_a: Dict, tweet_b: Dict) -> float:
    """
    Multi-factor similarity scoring

    Factors:
    - Text similarity (40% weight)
    - Entity overlap (25% weight)
    - Media match (20% weight)
    - URL match (15% weight)

    Returns: Weighted score 0-100
    """
    text_sim = calculate_text_similarity(
        tweet_a.get('text_normalized', ''),
        tweet_b.get('text_normalized', '')
    )

    entity_overlap = calculate_entity_overlap(
        tweet_a.get('entities', []),
        tweet_b.get('entities', [])
    )

    media_sim = calculate_media_similarity(
        tweet_a.get('media_hashes', []),
        tweet_b.get('media_hashes', [])
    )

    url_sim = calculate_url_similarity(
        tweet_a.get('urls_canonical', []),
        tweet_b.get('urls_canonical', [])
    )

    # Weighted average
    score = (
        text_sim * 0.40 +
        entity_overlap * 0.25 +
        media_sim * 0.20 +
        url_sim * 0.15
    )

    return score


def detect_new_information(tweet_new: Dict, tweet_old: Dict) -> bool:
    """
    Detect if new tweet adds information not in old tweet

    Checks:
    - More entities mentioned
    - Additional media
    - New URLs
    - Longer text (significantly)
    """
    # More entities (at least 2 more)
    new_entities = len(tweet_new.get('entities', []))
    old_entities = len(tweet_old.get('entities', []))
    if new_entities > old_entities + 2:
        return True

    # Additional media
    new_media = len(tweet_new.get('media_hashes', []))
    old_media = len(tweet_old.get('media_hashes', []))
    if new_media > old_media:
        return True

    # New URLs
    old_urls = set(tweet_old.get('urls_canonical', []))
    new_urls = set(tweet_new.get('urls_canonical', []))
    if new_urls - old_urls:  # URLs in new but not old
        return True

    # Significantly longer text (>30% more words)
    old_word_count = len(tweet_old.get('text_normalized', '').split())
    new_word_count = len(tweet_new.get('text_normalized', '').split())
    if old_word_count > 0 and new_word_count > old_word_count * 1.3:
        return True

    return False


def classify_tweet(incoming_tweet: Dict, matches: List[Dict]) -> Dict:
    """
    Classify tweet as ORIGINAL, REPOST, UPDATE, or RELATED

    CRITICAL: Strict false positive prevention
    Better to miss a repost than incorrectly label something

    Args:
        incoming_tweet: The new tweet to classify
        matches: List of similar tweets from database (sorted by timestamp)

    Returns:
        Classification dict with status, confidence, original info
    """
    # No matches = ORIGINAL
    if not matches:
        return {
            "status": "ORIGINAL",
            "confidence": 100.0,
            "canonical_event_id": None,
            "original_tweet_id": None,
            "original_timestamp": None,
            "original_source": None,
            "time_delta_seconds": 0,
            "time_delta_display": "First report",
            "added_new_info": False
        }

    # Get best match (earliest tweet with high similarity)
    best_match = None
    best_similarity = 0

    for match in matches:
        similarity = calculate_similarity_score(incoming_tweet, match)
        match['similarity_score'] = similarity

        # Track best match
        if similarity > best_similarity:
            best_similarity = similarity
            best_match = match

    # No match above minimum threshold
    if best_match is None or best_similarity < SIMILARITY_THRESHOLD_UPDATE:
        return {
            "status": "ORIGINAL",
            "confidence": 100.0 - best_similarity if best_match else 100.0,
            "canonical_event_id": None,
            "original_tweet_id": None,
            "original_timestamp": None,
            "original_source": None,
            "time_delta_seconds": 0,
            "time_delta_display": "First report",
            "added_new_info": False
        }

    # Calculate time delta
    incoming_time = incoming_tweet.get('timestamp_et') or incoming_tweet.get('timestamp_utc')
    original_time = best_match.get('timestamp_et') or best_match.get('timestamp_utc')

    if incoming_time and original_time:
        time_delta = incoming_time - original_time
        time_delta_seconds = int(time_delta.total_seconds())
    else:
        time_delta_seconds = 0

    # Check if new information was added
    has_new_info = detect_new_information(incoming_tweet, best_match)

    # Get tier priorities
    incoming_tier = get_tier_priority(incoming_tweet.get('author_tier', ''))
    original_tier = get_tier_priority(best_match.get('author_tier', ''))

    # CLASSIFICATION LOGIC with strict false positive prevention

    classification = "RELATED"  # Default to safest option
    confidence = best_similarity

    if best_similarity >= SIMILARITY_THRESHOLD_REPOST:
        # High similarity detected

        # Check 1: Too close in time (within 5 minutes)
        # Reports this close might be independent observations
        if time_delta_seconds < LOOKBACK_MINUTES_INDEPENDENT * 60:
            classification = "RELATED"
            confidence = best_similarity * 0.8  # Reduce confidence

        # Check 2: Adds new information = UPDATE
        elif has_new_info:
            classification = "UPDATE"
            confidence = best_similarity

        # Check 3: Lower or same tier posting later = likely REPOST
        elif incoming_tier >= original_tier:
            classification = "REPOST"
            confidence = best_similarity

        # Check 4: Higher tier posting later = might be independent verification
        else:
            classification = "RELATED"
            confidence = best_similarity * 0.85

    elif best_similarity >= SIMILARITY_THRESHOLD_UPDATE:
        # Medium similarity
        if has_new_info:
            classification = "UPDATE"
            confidence = best_similarity
        else:
            classification = "RELATED"
            confidence = best_similarity

    # Format time delta display
    time_delta_display = format_time_delta_seconds(time_delta_seconds)

    return {
        "status": classification,
        "confidence": round(confidence, 2),
        "canonical_event_id": best_match.get('canonical_event_id'),
        "original_tweet_id": best_match.get('tweet_id'),
        "original_timestamp": best_match.get('display_time'),
        "original_source": best_match.get('author'),
        "time_delta_seconds": time_delta_seconds,
        "time_delta_display": time_delta_display,
        "added_new_info": has_new_info,
        "similarity_score": best_similarity
    }


def find_potential_matches(event_hash: str, text_hash: str,
                          entities: List[Dict], db_query_func) -> List[Dict]:
    """
    Find potential matching tweets for comparison

    Uses multiple strategies:
    1. Exact text hash match
    2. Event hash match
    3. Entity overlap search

    Args:
        event_hash: Generated event fingerprint
        text_hash: SHA256 of normalized text
        entities: Extracted entities
        db_query_func: Function to query database

    Returns:
        List of potential matches to compare
    """
    matches = []

    # Strategy 1: Exact text hash (fastest)
    exact_matches = db_query_func('text_hash', text_hash)
    matches.extend(exact_matches)

    # Strategy 2: Event hash match
    event_matches = db_query_func('event_hash', event_hash)
    for m in event_matches:
        if m not in matches:
            matches.append(m)

    # Strategy 3: Entity-based search (if few matches so far)
    if len(matches) < 5 and entities:
        key_entities = [e['value'] for e in entities
                       if e['type'] in ['GPE', 'LOC', 'ORG']][:3]
        for entity in key_entities:
            entity_matches = db_query_func('entity', entity)
            for m in entity_matches:
                if m not in matches:
                    matches.append(m)

    return matches
