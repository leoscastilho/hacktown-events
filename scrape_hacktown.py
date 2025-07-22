#!/usr/bin/env python3
"""
Hacktown 2025 Event Scraper
Scrapes all events from the Hacktown 2025 API and saves them by day
"""

import requests
import json
import os
from datetime import datetime
import time
from typing import List, Dict, Any

# Configuration
BASE_URL = "https://hacktown-2025-ss-v2.api.yazo.com.br/public/schedules"
OUTPUT_DIR = "output"
DELAY_BETWEEN_REQUESTS = 0.5  # seconds

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


def fetch_page(date: str, page: int) -> Dict[str, Any]:
    """
    Fetch a single page of events for a given date
    """
    params = {
        'category_id': '42',
        'tag_ids': '[]',
        'day[]': [date, '00:00:00.000Z'],
        'page': str(page),
        'search': '',
        'product_ids': '[2]'
    }
    
    try:
        response = requests.get(BASE_URL, headers=HEADERS, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page} for {date}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON for page {page} on {date}: {e}")
        return None


def fetch_all_events_for_date(date: str) -> List[Dict[str, Any]]:
    """
    Fetch all events for a given date by paginating through all pages
    """
    all_events = []
    current_page = 1
    
    print(f"Fetching events for {date}...")
    
    while True:
        print(f"  Fetching page {current_page}...")
        
        data = fetch_page(date, current_page)
        if not data:
            break
        
        # Extract events from this page
        events = data.get('data', [])
        all_events.extend(events)
        
        # Check if we need to continue to the next page
        meta = data.get('meta', {})
        last_page = meta.get('last_page', 1)
        
        print(f"  Got {len(events)} events (total so far: {len(all_events)})")
        
        if current_page >= last_page:
            break
        
        current_page += 1
        time.sleep(DELAY_BETWEEN_REQUESTS)  # Be nice to the server
    
    print(f"  Total events for {date}: {len(all_events)}\n")
    return all_events


def normalize_location(place: str) -> str:
    """
    Normalize location names for filtering
    """
    if not place:
        return "Other"
    
    # Define location mappings
    place_upper = place.upper()
    
    if "INATEL" in place_upper:
        return "INATEL"
    elif "ETE" in place_upper:
        return "ETE"
    elif "LOJA MAÇONICA" in place_upper or "LOJA MAÇÔNICA" in place_upper:
        return "Loja Maçônica"
    elif "REAL PALACE" in place_upper:
        return "Real Palace"
    elif "BRASEIRO" in place_upper:
        return "Braseiro"
    elif "BOTECO" in place_upper:
        return "Boteco"
    else:
        # Return the original place for unmapped locations
        return place


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
    
    # Prepare data structure
    output_data = {
        "date": date,
        "total_events": len(processed_events),
        "scraped_at": datetime.now().isoformat(),
        "events": processed_events
    }
    
    # Save to file
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"Saved {len(processed_events)} events to {filepath}")


def main():
    """
    Main function to orchestrate the scraping process
    """
    print("Starting Hacktown 2025 Event Scraper")
    print("=" * 50)
    
    # Track statistics
    total_events = 0
    
    # Process each date
    for date in EVENT_DATES:
        events = fetch_all_events_for_date(date)
        
        if events:
            save_events_to_file(date, events)
            total_events += len(events)
        else:
            print(f"No events found for {date}")
    
    print("\n" + "=" * 50)
    print(f"Scraping complete! Total events scraped: {total_events}")
    print(f"Files saved in: {os.path.abspath(OUTPUT_DIR)}")
    
    # Create a summary file
    summary_file = os.path.join(OUTPUT_DIR, "summary.json")
    summary_data = {
        "scraping_completed": datetime.now().isoformat(),
        "total_events": total_events,
        "dates_processed": EVENT_DATES,
        "files_created": [f"hacktown_events_{date}.json" for date in EVENT_DATES]
    }
    
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2)
    
    print(f"Summary saved to: {summary_file}")


if __name__ == "__main__":
    main()