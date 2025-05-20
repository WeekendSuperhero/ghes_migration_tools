#!/usr/bin/env python3
"""
Dump users (active and deactivated) from a Slack workspace to a CSV file.
Requires: slack_sdk (pip install slack_sdk)
Env var: SLACK_BOT_TOKEN
"""
import os
import csv
import time
import logging
import argparse
import shutil
import json
from typing import Dict, List, Generator, Optional
from pathlib import Path
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Constants
DEFAULT_BATCH_SIZE = 200
DEFAULT_OUTPUT = "slack_users.csv"
AVAILABLE_FIELDS = [
    "id", "name", "real_name", "email", "deleted", "is_bot",
    "team_id", "tz", "title", "phone", "skype", "first_name", "last_name",
    "is_app_user", "is_owner", "is_admin", "is_primary_owner",
]

def setup_logging(verbose: bool) -> None:
    """Configure logging with timestamp, level, and function name based on verbosity."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    # Ensure Slack SDK logger does not use DEBUG level even if verbose is enabled
    logging.getLogger("slack_sdk").setLevel(logging.WARNING)

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Dump Slack workspace users to a CSV file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-o", "--output",
        default=DEFAULT_OUTPUT,
        help="Output CSV file path"
    )
    parser.add_argument(
        "-b", "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of users to fetch per API call"
    )
    parser.add_argument(
        "-f", "--fields",
        nargs="*",
        default=["id", "name", "first_name", "last_name", "email", "deleted", "is_bot", "is_app_user"],
        choices=AVAILABLE_FIELDS,
        help=f"Fields to include in output (available: {', '.join(AVAILABLE_FIELDS)})"
    )
    parser.add_argument(
        "--include-bots",
        action="store_true",
        help="Include bot users in output"
    )
    parser.add_argument(
        "--include-deleted",
        action="store_true",
        help="Include deleted users in output"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug-level logging for script (excludes Slack API client)"
    )
    return parser.parse_args()

def fetch_all_users(client: WebClient, batch_size: int) -> Generator[Dict, None, None]:
    """Generator yielding every user object in the workspace."""
    cursor: Optional[str] = None
    while True:
        try:
            resp = client.users_list(limit=batch_size, cursor=cursor)
            logging.info(f"Fetched {len(resp['members'])} users")
            logging.debug(f"Response: {json.dumps(resp.data, indent=2)}")
            for member in resp["members"]:
                yield member
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
        except SlackApiError as e:
            if e.response["error"] == "ratelimited":
                delay = int(e.response.headers.get("Retry-After", 1))
                logging.warning(f"Rate-limited, retrying after {delay}s")
                time.sleep(delay)
                continue
            logging.error(f"Slack API error: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error fetching users: {str(e)}")
            raise

def backup_existing_file(filepath: str) -> None:
    """Create a backup of existing output file if it exists."""
    path = Path(filepath)
    if path.exists():
        backup_path = path.with_suffix(f".backup_{time.strftime('%Y%m%d_%H%M%S')}.csv")
        try:
            shutil.copy2(path, backup_path)
            logging.info(f"Backed up existing file to {backup_path}")
        except IOError as e:
            logging.warning(f"Failed to create backup: {str(e)}")

def main() -> None:
    """Main function to dump Slack users to CSV."""
    args = parse_args()
    setup_logging(args.verbose)

    token = os.getenv("SLACK_BOT_TOKEN")
    if not token:
        logging.error("SLACK_BOT_TOKEN environment variable not set")
        raise SystemExit(1)

    try:
        client = WebClient(token=token)
    except Exception as e:
        logging.error(f"Failed to initialize Slack client: {str(e)}")
        raise SystemExit(1)

    active = deactivated = bot = app = 0
    backup_existing_file(args.output)

    try:
        with open(args.output, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=args.fields)
            writer.writeheader()

            for user in fetch_all_users(client, args.batch_size):
                if not args.include_bots and (user.get("is_bot", False) or user.get("is_app_user", False)):
                    continue
                if not args.include_deleted and user.get("deleted", False):
                    continue

                row = {}
                for field in args.fields:
                    if field in ["id", "name", "deleted", "is_bot", "is_app_user"]:
                        row[field] = user.get(field, "")
                    elif field == "real_name":
                        row[field] = user.get("real_name", "")
                    else:
                        row[field] = user.get("profile", {}).get(field, "")

                writer.writerow(row)
                if user.get("deleted", False):
                    deactivated += 1
                else:
                    active += 1
                if user.get("is_bot", True):
                    bot += 1
                if user.get("is_app_user", True):
                    app += 1

    except IOError as e:
        logging.error(f"Failed to write to {args.output}: {str(e)}")
        raise SystemExit(1)

    total = active + deactivated
    logging.info(
        f"Saved {total} users to {args.output} "
        f"({active} active, {deactivated} deactivated, {bot} bot, {app} app users)"
    )

if __name__ == "__main__":
    main()