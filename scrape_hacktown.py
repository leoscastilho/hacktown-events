#!/usr/bin/env python3
"""
Hacktown 2025 Event Scraper - Async Version
Scrapes all events from the Hacktown 2025 API with concurrent requests
"""

import asyncio
import aiohttp
import json
import os
import random
from datetime import datetime
import time
from typing import List, Dict, Any, Optional
from zoneinfo import ZoneInfo
import logging

# Configuration
BASE_URL = "https://hacktown-2025-ss-v2.api.yazo.com.br/public/schedules"
OUTPUT_DIR = "events"

# Detect if running in CI
IS_CI = os.environ.get('CI', 'false').lower() == 'true' or os.environ.get('GITHUB_ACTIONS', 'false').lower() == 'true'

# Adjust settings for CI environment
if IS_CI:
    MAX_CONCURRENT_REQUESTS = 1  # More conservative in CI
    RETRY_DELAY = 10  # Longer initial delay in CI
    print("Running in CI environment - using conservative settings")
else:
    MAX_CONCURRENT_REQUESTS = 2  # Normal setting for local
    RETRY_DELAY = 5  # Normal delay for local

MAX_RETRIES = 5
REQUEST_TIMEOUT = 30  # seconds

# Event dates
EVENT_DATES = [
    "2025-07-30",
    "2025-07-31",
    "2025-08-01",
    "2025-08-02",
    "2025-08-03"
]

# Headers (minimal required headers)
HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'origin': 'https://hacktown2025.yazo.app.br',
    'product-identifier': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'accept-language': 'en-US,en;q=0.9',
    'referer': 'https://hacktown2025.yazo.app.br/',
}

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Location normalization cache
location_cache = {}


def normalize_location(place: str) -> str:
    """
    Normalize location names for filtering with caching
    """
    if not place:
        return "Other"

    # Check cache first
    if place in location_cache:
        return location_cache[place]

    # Define location mappings
    place_upper = place.upper()
    result = "Other"

    if "INATEL" in place_upper:
        result = "INATEL"
    elif "ETE" in place_upper:
        result = "ETE"
    elif "LOJA MAÇONICA" in place_upper or "LOJA MAÇÔNICA" in place_upper:
        result = "Loja Maçônica"
    elif "REAL PALACE" in place_upper:
        result = "Real Palace"
    elif "BRASEIRO" in place_upper:
        result = "Braseiro"
    elif "BOTECO DO TIO" in place_upper:
        result = "Boteco do Tio João"
    elif "ASSOCIAÇÃO" in place_upper:
        result = "Associação José do Patrocínio"
    elif "BAR E RESTAURANTE" in place_upper:
        result = "Bar e Restaurante do Dimas II"
    elif "ESCOLA S" in place_upper:
        result = "Escola Sanico Teles"
    elif "CASA DINAMARCA" in place_upper:
        result = "Casa Dinamarca"
    elif "CASA MDM" in place_upper:
        result = "Casa MFM"
    elif "PALCO UNDERSTREAM" in place_upper:
        result = "Palco UNDERSTREAM"
    else:
        # Return the original place for unmapped locations
        result = place

    # Cache the result
    location_cache[place] = result
    return result


async def fetch_page(session: aiohttp.ClientSession, date: str, page: int) -> Optional[Dict[str, Any]]:
    """
    Fetch a single page of events for a given date with retry logic
    """
    params = {
        'category_id': '42',
        'tag_ids': '[]',
        'day[]': [date, '00:00:00.000Z'],
        'page': str(page),
        'search': '',
        'product_ids': '[2]'
    }

    for attempt in range(MAX_RETRIES):
        try:
            # Add random delay before request to appear more human-like
            if IS_CI:
                await asyncio.sleep(random.uniform(3, 7))  # Longer delay in CI
            else:
                await asyncio.sleep(random.uniform(0.5, 1.5))
            
            async with session.get(
                    BASE_URL,
                    headers=HEADERS,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 403 and attempt < MAX_RETRIES - 1:
                    logger.warning(f"403 error for {date} page {page}, attempt {attempt + 1}, retrying...")
                    # Exponential backoff with jitter, capped between 5 and 30 seconds
                    base_delay = RETRY_DELAY * (2 ** attempt) + random.uniform(0, 5)
                    retry_delay = max(5, min(base_delay, 30))  # Clamp between 5 and 30 seconds
                    logger.info(f"Waiting {retry_delay:.1f} seconds before retry...")
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"HTTP {response.status} for {date} page {page}")
                    return None
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching page {page} for {date}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
                continue
            return None
        except Exception as e:
            logger.error(f"Error fetching page {page} for {date}: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
                continue
            return None
    return None


async def fetch_all_pages_for_date(session: aiohttp.ClientSession, date: str, semaphore: asyncio.Semaphore) -> List[
    Dict[str, Any]]:
    """
    Fetch all pages for a given date concurrently
    """
    all_events = []

    # First, fetch page 1 to get total pages
    async with semaphore:
        logger.info(f"Fetching page 1 for {date} to get total pages...")
        first_page_data = await fetch_page(session, date, 1)

    if not first_page_data:
        return []

    events = first_page_data.get('data', [])
    all_events.extend(events)

    meta = first_page_data.get('meta', {})
    last_page = meta.get('last_page', 1)

    if last_page > 1:
        # Fetch remaining pages concurrently
        tasks = []
        for page in range(2, last_page + 1):
            async def fetch_with_semaphore(p):
                async with semaphore:
                    logger.info(f"Fetching page {p} for {date}...")
                    return await fetch_page(session, date, p)

            tasks.append(fetch_with_semaphore(page))

        # Wait for all pages to complete
        results = await asyncio.gather(*tasks)

        for result in results:
            if result:
                events = result.get('data', [])
                all_events.extend(events)

    logger.info(f"Total events for {date}: {len(all_events)}")
    return all_events


def process_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process events to add filterLocation field
    """
    for event in events:
        place = event.get('place', '')
        event['filterLocation'] = normalize_location(place)

    return events


def save_events_to_file(date: str, events: List[Dict[str, Any]]):
    """
    Save events to a JSON file organized by date
    """
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Process events to add filterLocation
    processed_events = process_events(events)

    # Format filename
    filename = f"hacktown_events_{date}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)

    # Get current time in BRT (Brasília Time)
    utc_now = datetime.now(ZoneInfo('UTC'))
    brt_now = utc_now.astimezone(ZoneInfo('America/Sao_Paulo'))

    # Prepare data structure
    output_data = {
        "date": date,
        "total_events": len(processed_events),
        "scraped_at": brt_now.isoformat(),
        "events": processed_events
    }

    # Save to file
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(processed_events)} events to {filepath}")


async def fetch_all_dates(dates: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch events for all dates concurrently
    """
    all_results = {}

    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    # Create session with connection pooling - more conservative settings for CI
    if IS_CI:
        connector = aiohttp.TCPConnector(
            limit=5,  # Much lower limit in CI
            limit_per_host=2,  # Very conservative per-host limit
            ttl_dns_cache=300,  # DNS cache timeout
            force_close=True  # Force close connections after each request
        )
    else:
        connector = aiohttp.TCPConnector(
            limit=20,  # Reduced from 100
            limit_per_host=10,  # Reduced from 30
            ttl_dns_cache=300  # DNS cache timeout
        )

    # Create session with cookie jar to maintain session state
    async with aiohttp.ClientSession(
        connector=connector,
        cookie_jar=aiohttp.CookieJar()
    ) as session:
        tasks = []
        for date in dates:
            task = fetch_all_pages_for_date(session, date, semaphore)
            tasks.append(task)

        # Fetch all dates concurrently
        results = await asyncio.gather(*tasks)

        # Map results to dates
        for date, events in zip(dates, results):
            all_results[date] = events

    return all_results


async def main():
    """
    Main function to orchestrate the scraping process
    """
    logger.info("Starting Hacktown 2025 Event Scraper (Async Version)")
    logger.info("=" * 50)

    start_time = time.time()

    # Load existing summary if it exists
    summary_file = os.path.join(OUTPUT_DIR, "summary.json")
    existing_summary = {}
    if os.path.exists(summary_file):
        try:
            with open(summary_file, 'r', encoding='utf-8') as f:
                existing_summary = json.load(f)
            logger.info(f"Loaded existing summary with {existing_summary.get('total_events', 0)} events")
        except Exception as e:
            logger.warning(f"Could not load existing summary: {e}")

    # Fetch all events concurrently
    all_events = await fetch_all_dates(EVENT_DATES)

    # Track statistics
    total_events = 0
    fetch_successful = False

    # Save results
    for date, events in all_events.items():
        if events:
            save_events_to_file(date, events)
            total_events += len(events)
            fetch_successful = True
        else:
            logger.warning(f"No events found for {date}")

    elapsed_time = time.time() - start_time

    logger.info("\n" + "=" * 50)
    logger.info(f"Scraping complete! Total events scraped: {total_events}")
    logger.info(f"Time taken: {elapsed_time:.2f} seconds")
    logger.info(f"Files saved in: {os.path.abspath(OUTPUT_DIR)}")

    # Get current time in BRT
    utc_now = datetime.now(ZoneInfo('UTC'))
    brt_now = utc_now.astimezone(ZoneInfo('America/Sao_Paulo'))

    # Prepare summary data
    if fetch_successful:
        # Update with new data if fetch was successful
        summary_data = {
            "scraping_completed": brt_now.isoformat(),
            "total_events": total_events,
            "dates_processed": EVENT_DATES,
            "files_created": [f"hacktown_events_{date}.json" for date in EVENT_DATES],
            "scraping_time_seconds": round(elapsed_time, 2)
        }
        logger.info("Fetch successful - updating summary with new data")
    else:
        # Preserve old values if fetch failed
        summary_data = {
            "scraping_completed": existing_summary.get("scraping_completed", brt_now.isoformat()),
            "total_events": existing_summary.get("total_events", 0),
            "dates_processed": EVENT_DATES,
            "files_created": existing_summary.get("files_created", []),
            "scraping_time_seconds": round(elapsed_time, 2),
            "last_failed_attempt": brt_now.isoformat()
        }
        logger.warning("Fetch failed - preserving existing summary values")

    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2)

    logger.info(f"Summary saved to: {summary_file}")
    logger.info(f"Location cache efficiency: {len(location_cache)} unique locations cached")


if __name__ == "__main__":
    asyncio.run(main())