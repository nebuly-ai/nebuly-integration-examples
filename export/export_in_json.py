import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Union

import aiohttp

"""
Export interactions from the Nebuly API to a JSON file.

The script fetches interactions from the Nebuly get-interactions endpoint
and saves them to a JSON file. It automatically retrieves all data using
pagination with an internal batch size of 100.

Usage:
    python export_in_json.py <api_key> [options]

Options:
    --endpoint-url: Custom endpoint URL (default: https://backend.nebuly.com/api/external/get-interactions)
    --start-date: Start date in ISO format (default: 30 days ago)
    --end-date: End date in ISO format (default: now)
    --output: Output JSON file path (default: interactions.json)
    --power-bi-format: Save as Power BI compatible format (array of objects)

Requirements (pip install/poetry add/...):
- aiohttp
"""

# Default endpoint URL - can be modified for self-hosted solutions
DEFAULT_ENDPOINT_URL = "https://backend.nebuly.com/api/external/get-interactions"

# Internal batch size for pagination (not exposed to users)
DEFAULT_BATCH_SIZE = 100

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_default_time_range() -> tuple[str, str]:
    """Get default time range (last 30 days)."""
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=30)
    return start_date.isoformat(), end_date.isoformat()


async def fetch_interactions(
    session: aiohttp.ClientSession,
    endpoint_url: str,
    api_key: str,
    time_range: dict[str, str],
    filters: Union[list[dict[str, Any]], None] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """
    Fetch interactions from the Nebuly API.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload: dict[str, Any] = {
        "time_range": time_range,
        "limit": limit,
        "offset": offset,
        "filters": []
    }

    if filters:
        payload["filters"] = filters

    try:
        async with session.post(endpoint_url, json=payload, headers=headers) as response:
            text = await response.text()
            if response.status != 200:
                logger.error(f"HTTP Error {response.status}: {text}")
                raise Exception(f"HTTP Error {response.status}: {text}")
            return await response.json()
    except aiohttp.ClientResponseError as e:
        logger.error(f"HTTP Error for {endpoint_url}: {e.status} - {e.message}")
        raise
    except aiohttp.ClientError as e:
        logger.error(f"Network or Client Error for {endpoint_url}: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred for {endpoint_url}: {e}")
        raise


async def fetch_all_interactions(
    endpoint_url: str,
    api_key: str,
    time_range: dict[str, str],
    filters: Union[list[dict[str, Any]], None] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[dict[str, Any]]:
    """
    Fetch all interactions with automatic pagination.
    Continues retrieving data until all interactions have been fetched.
    """
    all_interactions: list[dict[str, Any]] = []
    offset = 0

    async with aiohttp.ClientSession() as session:
        while True:
            logger.info(f"Fetching interactions (offset: {offset}, batch size: {batch_size})")
            response = await fetch_interactions(
                session, endpoint_url, api_key, time_range, filters, batch_size, offset
            )

            interactions = response.get("data", [])
            all_interactions.extend(interactions)

            total = response.get("total", 0)
            logger.info(f"Retrieved {len(interactions)} interactions (total: {total}, fetched so far: {len(all_interactions)})")

            # Check if we've fetched all interactions
            if offset + len(interactions) >= total or len(interactions) == 0:
                break

            offset += batch_size

    return all_interactions


async def main():
    parser = argparse.ArgumentParser(
        description="Export interactions from Nebuly API to JSON file"
    )
    parser.add_argument(
        "api_key",
        help="Nebuly API key (Bearer token)",
    )
    parser.add_argument(
        "--endpoint-url",
        default=DEFAULT_ENDPOINT_URL,
        help=f"Custom endpoint URL (default: {DEFAULT_ENDPOINT_URL})",
    )
    parser.add_argument(
        "--start-date",
        help="Start date in ISO format (e.g., 2024-01-01T00:00:00Z). Default: 30 days ago",
    )
    parser.add_argument(
        "--end-date",
        help="End date in ISO format (e.g., 2024-01-31T23:59:59Z). Default: now",
    )
    parser.add_argument(
        "--output",
        default="interactions.json",
        help="Output JSON file path (default: interactions.json)",
    )
    parser.add_argument(
        "--power-bi-format",
        action="store_true",
        help="Save as Power BI compatible format (array of objects instead of nested structure)",
    )

    args = parser.parse_args()

    # Set up time range
    if args.start_date and args.end_date:
        time_range = {"start": args.start_date, "end": args.end_date}
    else:
        start, end = get_default_time_range()
        time_range = {"start": start, "end": end}
        if args.start_date:
            time_range["start"] = args.start_date
        if args.end_date:
            time_range["end"] = args.end_date

    logger.info(f"Time range: {time_range['start']} to {time_range['end']}")

    try:
        # Fetch all interactions with automatic pagination (no filters)
        interactions = await fetch_all_interactions(
            args.endpoint_url,
            args.api_key,
            time_range,
            None,  # No filters
            DEFAULT_BATCH_SIZE,
        )

        # Save to JSON file
        if args.power_bi_format:
            # Power BI prefers a direct array of objects
            output_data = interactions
        else:
            # Default format with metadata
            output_data = {
                "total": len(interactions),
                "time_range": time_range,
                "data": interactions,
            }

        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Successfully exported {len(interactions)} interactions to {args.output}")

    except Exception as e:
        logger.error(f"Error exporting interactions: {e}")
        sys.exit(1)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

