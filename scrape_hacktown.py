#!/usr/bin/env python3
"""
Hacktown 2025 Event Scraper - Async Version
Scrapes all events from the Hacktown 2025 API with concurrent requests
"""

import asyncio
import aiohttp
import json
import os
from datetime import datetime
import time
from typing import List, Dict, Any, Optional
from zoneinfo import ZoneInfo
import logging

# Configuration
BASE_URL = "https://hacktown-2025-ss-v2.api.yazo.com.br/public/schedules"
OUTPUT_DIR = "events"
MAX_CONCURRENT_REQUESTS = 5  # Limit concurrent requests
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds between retries
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
    'accept-language': 'en-US',
    'origin': 'https://hacktown2025.yazo.app.br',
    'product-identifier': '1',
    'referer': 'https://hacktown2025.yazo.app.br/',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
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
    elif "BOTECO" in place_upper:
        result = "Boteco"
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
                    await asyncio.sleep(RETRY_DELAY)
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

    # Create session with connection pooling
    connector = aiohttp.TCPConnector(
        limit=100,  # Total connection pool limit
        limit_per_host=30,  # Per-host connection limit
        ttl_dns_cache=300  # DNS cache timeout
    )

    async with aiohttp.ClientSession(connector=connector) as session:
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

    # Fetch all events concurrently
    all_events = await fetch_all_dates(EVENT_DATES)

    # Track statistics
    total_events = 0

    # Save results
    for date, events in all_events.items():
        if events:
            save_events_to_file(date, events)
            total_events += len(events)
        else:
            logger.warning(f"No events found for {date}")

    elapsed_time = time.time() - start_time

    logger.info("\n" + "=" * 50)
    logger.info(f"Scraping complete! Total events scraped: {total_events}")
    logger.info(f"Time taken: {elapsed_time:.2f} seconds")
    logger.info(f"Files saved in: {os.path.abspath(OUTPUT_DIR)}")

    # Create a summary file with BRT timestamp
    summary_file = os.path.join(OUTPUT_DIR, "summary.json")

    # Get current time in BRT
    utc_now = datetime.now(ZoneInfo('UTC'))
    brt_now = utc_now.astimezone(ZoneInfo('America/Sao_Paulo'))

    summary_data = {
        "scraping_completed": brt_now.isoformat(),
        "total_events": total_events,
        "dates_processed": EVENT_DATES,
        "files_created": [f"hacktown_events_{date}.json" for date in EVENT_DATES],
        "scraping_time_seconds": round(elapsed_time, 2)
    }

    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2)

    logger.info(f"Summary saved to: {summary_file}")
    logger.info(f"Location cache efficiency: {len(location_cache)} unique locations cached")


if __name__ == "__main__":
    asyncio.run(main())