"""
FastAPI Web API
REST API for OriginStamp data access
"""
import logging
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from src.database import (
    get_recent_events, get_event_timeline, search_events,
    get_tracked_accounts, init_database
)
from src.reliability import get_reliability_leaderboard, detect_verification_chain
from src.ingestion import process_single_tweet

logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="OriginStamp API",
    description="Geopolitical News Timestamp Verification System",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models for API responses
class EventSummary(BaseModel):
    id: int
    event_hash: str
    first_author: str
    first_display_time: str
    claim_summary: str
    verification_status: str
    repost_count: int
    update_count: int


class EventDetail(BaseModel):
    event: dict
    reposts: List[dict]


class AccountInfo(BaseModel):
    account: str
    tier: str
    reliability_score: Optional[float]
    total_tweets_tracked: int
    total_original_reports: int
    total_reposts: int


class ProcessResult(BaseModel):
    status: str
    tweet_id: Optional[str]
    author: Optional[str]
    classification: Optional[dict]
    display_time: Optional[str]


# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    init_database()
    logger.info("OriginStamp API started")


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


# Events endpoints
@app.get("/api/events", response_model=List[dict])
async def list_events(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0)
):
    """Get recent canonical events"""
    events = get_recent_events(limit=limit + offset)
    return events[offset:offset + limit]


@app.get("/api/events/{event_id}")
async def get_event(event_id: int):
    """Get detailed event timeline"""
    timeline = get_event_timeline(event_id)
    if not timeline or not timeline.get('event'):
        raise HTTPException(status_code=404, detail="Event not found")
    return timeline


@app.get("/api/events/{event_id}/verification")
async def get_event_verification(event_id: int):
    """Get verification chain analysis for an event"""
    verification = detect_verification_chain(event_id)
    if verification.get('status') == 'error':
        raise HTTPException(status_code=404, detail=verification.get('message'))
    return verification


@app.get("/api/search")
async def search(
    q: str = Query(..., min_length=2),
    limit: int = Query(50, ge=1, le=200)
):
    """Search events by text"""
    results = search_events(q, limit=limit)
    return results


# Accounts endpoints
@app.get("/api/accounts")
async def list_accounts():
    """Get all tracked accounts"""
    accounts = get_tracked_accounts()
    return accounts


@app.get("/api/accounts/leaderboard")
async def accounts_leaderboard(
    limit: int = Query(50, ge=1, le=200)
):
    """Get accounts ranked by reliability"""
    leaderboard = get_reliability_leaderboard(limit=limit)
    return leaderboard


# Processing endpoints
@app.post("/api/process/tweet/{tweet_id}", response_model=ProcessResult)
async def process_tweet(tweet_id: str):
    """
    Process a single tweet by ID

    Useful for manual processing or testing
    """
    result = process_single_tweet(tweet_id)
    return result


# Simple HTML pages
@app.get("/", response_class=HTMLResponse)
async def home():
    """Home page with recent events"""
    events = get_recent_events(limit=20)

    events_html = ""
    for e in events:
        events_html += f"""
        <div class="event">
            <div class="event-time">{e.get('first_display_time', 'Unknown')}</div>
            <div class="event-author">@{e.get('first_author', 'unknown')}</div>
            <div class="event-text">{e.get('claim_summary', e.get('original_text', ''))[:200]}</div>
            <div class="event-stats">
                Reposts: {e.get('repost_count', 0)} | Updates: {e.get('update_count', 0)} |
                Status: {e.get('verification_status', 'unverified')}
            </div>
            <a href="/event/{e.get('id')}">View Timeline</a>
        </div>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>OriginStamp - News Timestamp Verification</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                   max-width: 800px; margin: 0 auto; padding: 20px; background: #0d1117; color: #c9d1d9; }}
            h1 {{ color: #58a6ff; }}
            .event {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px;
                     padding: 16px; margin: 16px 0; }}
            .event-time {{ color: #8b949e; font-size: 0.9em; }}
            .event-author {{ color: #58a6ff; font-weight: bold; margin: 8px 0; }}
            .event-text {{ color: #c9d1d9; margin: 8px 0; }}
            .event-stats {{ color: #8b949e; font-size: 0.85em; margin: 8px 0; }}
            a {{ color: #58a6ff; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
            .header {{ border-bottom: 1px solid #30363d; padding-bottom: 16px; margin-bottom: 24px; }}
            .nav {{ margin-top: 16px; }}
            .nav a {{ margin-right: 16px; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>OriginStamp</h1>
            <p>Geopolitical News Timestamp Verification</p>
            <div class="nav">
                <a href="/">Live Feed</a>
                <a href="/search">Search</a>
                <a href="/accounts">Accounts</a>
                <a href="/docs">API Docs</a>
            </div>
        </div>
        <h2>Recent Original Reports</h2>
        {events_html if events_html else '<p>No events yet. Start monitoring to see reports.</p>'}
    </body>
    </html>
    """


@app.get("/event/{event_id}", response_class=HTMLResponse)
async def event_page(event_id: int):
    """Event detail page with timeline"""
    timeline = get_event_timeline(event_id)
    if not timeline or not timeline.get('event'):
        raise HTTPException(status_code=404, detail="Event not found")

    event = timeline['event']
    reposts = timeline.get('reposts', [])

    reposts_html = ""
    for r in reposts:
        emoji = "+" if r.get('classification') == 'UPDATE' else "↻"
        reposts_html += f"""
        <div class="repost">
            <span class="repost-time">{r.get('display_time', 'Unknown')}</span>
            <span class="repost-author">@{r.get('author', 'unknown')}</span>
            <span class="repost-type">{emoji} {r.get('classification', 'REPOST')}</span>
            <span class="repost-delta">{r.get('time_delta_display', '')}</span>
        </div>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Event Timeline - OriginStamp</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                   max-width: 800px; margin: 0 auto; padding: 20px; background: #0d1117; color: #c9d1d9; }}
            h1 {{ color: #58a6ff; }}
            .original {{ background: #1f6feb22; border: 2px solid #1f6feb; border-radius: 6px;
                        padding: 16px; margin: 16px 0; }}
            .original-label {{ color: #3fb950; font-weight: bold; font-size: 0.9em; }}
            .event-text {{ color: #c9d1d9; margin: 12px 0; font-size: 1.1em; }}
            .event-meta {{ color: #8b949e; font-size: 0.9em; }}
            .timeline {{ margin-top: 24px; }}
            .repost {{ background: #161b22; border-left: 3px solid #30363d; padding: 12px;
                      margin: 8px 0; display: flex; gap: 16px; align-items: center; }}
            .repost-time {{ color: #8b949e; min-width: 140px; }}
            .repost-author {{ color: #58a6ff; min-width: 120px; }}
            .repost-type {{ color: #8b949e; }}
            .repost-delta {{ color: #8b949e; margin-left: auto; }}
            a {{ color: #58a6ff; text-decoration: none; }}
        </style>
    </head>
    <body>
        <p><a href="/">← Back to Feed</a></p>

        <div class="original">
            <div class="original-label">ORIGINAL REPORT</div>
            <div class="event-text">{event.get('original_text', event.get('claim_summary', ''))}</div>
            <div class="event-meta">
                <strong>First reported:</strong> {event.get('first_display_time', 'Unknown')}<br>
                <strong>Source:</strong> @{event.get('first_author', 'unknown')}<br>
                <strong>Status:</strong> {event.get('verification_status', 'unverified')}<br>
                <strong>Reposts:</strong> {event.get('repost_count', 0)} |
                <strong>Updates:</strong> {event.get('update_count', 0)}
            </div>
        </div>

        <div class="timeline">
            <h2>Timeline ({len(reposts)} reports)</h2>
            {reposts_html if reposts_html else '<p>No reposts yet.</p>'}
        </div>
    </body>
    </html>
    """


@app.get("/search", response_class=HTMLResponse)
async def search_page(q: str = None):
    """Search page"""
    results_html = ""
    if q:
        results = search_events(q, limit=50)
        for e in results:
            results_html += f"""
            <div class="result">
                <div class="result-time">{e.get('first_display_time', 'Unknown')}</div>
                <div class="result-author">@{e.get('first_author', 'unknown')}</div>
                <div class="result-text">{e.get('claim_summary', e.get('original_text', ''))[:200]}</div>
                <a href="/event/{e.get('id')}">View Timeline</a>
            </div>
            """

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Search - OriginStamp</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                   max-width: 800px; margin: 0 auto; padding: 20px; background: #0d1117; color: #c9d1d9; }}
            h1 {{ color: #58a6ff; }}
            input {{ background: #0d1117; border: 1px solid #30363d; color: #c9d1d9;
                    padding: 10px; font-size: 16px; width: 300px; border-radius: 6px; }}
            button {{ background: #238636; color: white; border: none; padding: 10px 20px;
                     font-size: 16px; border-radius: 6px; cursor: pointer; }}
            .result {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px;
                      padding: 16px; margin: 16px 0; }}
            .result-time {{ color: #8b949e; font-size: 0.9em; }}
            .result-author {{ color: #58a6ff; font-weight: bold; margin: 8px 0; }}
            .result-text {{ color: #c9d1d9; margin: 8px 0; }}
            a {{ color: #58a6ff; text-decoration: none; }}
        </style>
    </head>
    <body>
        <p><a href="/">← Back to Feed</a></p>
        <h1>Search Events</h1>

        <form method="get">
            <input type="text" name="q" placeholder="Search..." value="{q or ''}">
            <button type="submit">Search</button>
        </form>

        <div class="results">
            {results_html if results_html else ('<p>Enter a search term above.</p>' if not q else '<p>No results found.</p>')}
        </div>
    </body>
    </html>
    """


@app.get("/accounts", response_class=HTMLResponse)
async def accounts_page():
    """Accounts leaderboard page"""
    accounts = get_reliability_leaderboard(limit=50)

    accounts_html = ""
    for i, a in enumerate(accounts, 1):
        score = a.get('reliability_score', 0) or 0
        stars = "★" * int(score * 5) + "☆" * (5 - int(score * 5))

        accounts_html += f"""
        <tr>
            <td>{i}</td>
            <td>@{a.get('account', 'unknown')}</td>
            <td>{a.get('tier', 'unknown')}</td>
            <td>{stars} ({score:.2f})</td>
            <td>{a.get('total_original_reports', 0)}</td>
            <td>{a.get('total_reposts', 0)}</td>
            <td>{a.get('total_tweets_tracked', 0)}</td>
        </tr>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Account Reliability - OriginStamp</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                   max-width: 1000px; margin: 0 auto; padding: 20px; background: #0d1117; color: #c9d1d9; }}
            h1 {{ color: #58a6ff; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #30363d; }}
            th {{ background: #161b22; color: #8b949e; }}
            tr:hover {{ background: #161b22; }}
            a {{ color: #58a6ff; text-decoration: none; }}
        </style>
    </head>
    <body>
        <p><a href="/">← Back to Feed</a></p>
        <h1>Account Reliability Leaderboard</h1>

        <table>
            <tr>
                <th>#</th>
                <th>Account</th>
                <th>Tier</th>
                <th>Reliability</th>
                <th>Originals</th>
                <th>Reposts</th>
                <th>Total</th>
            </tr>
            {accounts_html if accounts_html else '<tr><td colspan="7">No data yet.</td></tr>'}
        </table>
    </body>
    </html>
    """
