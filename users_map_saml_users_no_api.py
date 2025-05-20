#!/usr/bin/env python3
"""
map_saml_github.py

Purpose:
  Inventory GitHub Enterprise users by merging SAML mappings and user listings.
  Supports two modes:
    1. Remote: SSH into GHE server and run
         • ghe-saml-mapping-csv -d -o
         • ghe-user-csv        -d -o
       capturing their CSV output directly.
    2. Local: Use existing CSV files for SAML and user data.

Usage Examples:
  Remote mode:
    ./map_saml_github.py --remote-host ghe.company.com --remote-user deploy --prefix inventory

  Local mode:
    ./map_saml_github.py --saml saml.csv --users users.csv --prefix inventory

Requirements:
  • Python 3 with pandas installed (`pip install pandas`).
  • SSH keys configured for passwordless login to the GitHub Enterprise server when using remote mode.
  • The commands `ghe-saml-mapping-csv` and `ghe-user-csv` must be available on the remote host.

Why this script exists:
  GitHub Enterprise’s SCIM and SAML APIs can occasionally omit users. 
  To ensure a complete inventory, this script pulls the raw CSV exports directly,
  merges them by `login`, and reports any unmatched entries.
"""

import argparse
import subprocess
import os
import sys
import pandas as pd

def run_remote_commands(host, user, port=122):
    """
    SSH into host and run the CSV generators, capturing stdout to local files.
    """
    saml_local = "saml.csv"
    users_local = "users.csv"

    print("→ Fetching SAML CSV via SSH...")
    with open(saml_local, "w") as f:
        subprocess.run(
            ["ssh", f"-p {port}", f"{user}@{host}", "ghe-saml-mapping-csv -d -o"],
            check=True, stdout=f
        )

    print("→ Fetching Users CSV via SSH...")
    with open(users_local, "w") as f:
        subprocess.run(
            ["ssh", f"-p {port}", f"{user}@{host}", "ghe-user-csv -d -o"],
            check=True, stdout=f
        )

    return saml_local, users_local

def load_and_normalize(saml_path, users_path):
    """
    Load the SAML and users CSVs, normalize the 'login' key and 'email' fields.
    """
    saml_df = pd.read_csv(saml_path)
    users_df = pd.read_csv(users_path, header=0)

    if 'login' not in saml_df.columns:
        raise ValueError("SAML CSV must have a 'login' column")

    # Normalize GitHub CSV: first column = login, second = email
    if 'login' not in users_df.columns:
        cols = list(users_df.columns)
        users_df = users_df.rename(columns={cols[0]: 'login', cols[1]: 'github_email'})
    else:
        users_df = users_df.rename(columns={users_df.columns[1]: 'github_email'})

    # Lowercase & strip whitespace
    saml_df['login']         = saml_df['login'].astype(str).str.strip().str.lower()
    users_df['login']        = users_df['login'].astype(str).str.strip().str.lower()
    users_df['github_email'] = users_df['github_email'].astype(str).str.strip().str.lower()

    return saml_df, users_df

def map_and_output(saml_df, users_df, prefix):
    """
    Merge on 'login', write merged & unmatched files, skipping any unmatched_saml
    if there are no missing entries.
    """
    merged = pd.merge(
        saml_df, users_df,
        how='left', on='login',
        suffixes=('_saml', '_github')
    )

    unmatched_saml   = merged[ merged['github_email'].isna() ]
    unmatched_github = users_df[ ~users_df['login'].isin(saml_df['login']) ]

    # Always write merged
    merged_path = f"{prefix}_merged.csv"
    merged.to_csv(merged_path, index=False)
    print(f"✅ Wrote merged file: {merged_path} ({len(merged)} rows)")

    # Unmatched SAML: only write if nonempty
    if not unmatched_saml.empty:
        path = f"{prefix}_unmatched_saml.csv"
        unmatched_saml.to_csv(path, index=False)
        print(f"⚠️  Wrote unmatched SAML: {path} ({len(unmatched_saml)} rows)")
    else:
        print("ℹ️  No unmatched SAML entries—skipping unmatched_saml.csv")

    # Unmatched GitHub: write if nonempty
    if not unmatched_github.empty:
        path = f"{prefix}_unmatched_github.csv"
        unmatched_github.to_csv(path, index=False)
        print(f"⚠️  Wrote unmatched GitHub: {path} ({len(unmatched_github)} rows)")
    else:
        print("ℹ️  No unmatched GitHub entries—skipping unmatched_github.csv")

def main():
    parser = argparse.ArgumentParser(
        description="Map SAML ↔ GitHub Enterprise users by 'login'",
        epilog=(
            "Ensure SSH keys are set up for --remote-host mode, and that\n"
            "'ghe-saml-mapping-csv' & 'ghe-user-csv' are installed on the server.\n"
            "This script helps catch users missing from SCIM/SAML APIs by using\n"
            "the raw CSV exports."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--remote-host",
        help="SSH host to run 'ghe-saml-mapping-csv' and 'ghe-user-csv' remotely"
    )
    parser.add_argument(
        "--remote-user",
        help="SSH user (must have key-based auth configured)",
        default=os.getlogin()
    )
    parser.add_argument(
        "--remote-port",
        help="optional SSH port (default: 122)",
        default=122
    )
    parser.add_argument(
        "--saml",
        help="Local SAML CSV path (if not using --remote-host)"
    )
    parser.add_argument(
        "--users",
        help="Local users CSV path (if not using --remote-host)"
    )
    parser.add_argument(
        "--prefix",
        help="Filename prefix for outputs",
        default="mapping"
    )
    args = parser.parse_args()

    # Determine mode
    if args.remote_host:
        saml_path, users_path = run_remote_commands(args.remote_host, args.remote_user, args.remote_port)
    else:
        if not args.saml or not args.users:
            parser.error("Either --remote-host or both --saml and --users must be provided.")
        saml_path, users_path = args.saml, args.users

    # Load, normalize, map, output
    saml_df, users_df = load_and_normalize(saml_path, users_path)
    map_and_output(saml_df, users_df, args.prefix)

if __name__ == "__main__":
    main()