"""
Timeout Worker - Monitors and fails jobs stuck in 'running' status for too long.
This should run as a scheduled task (e.g., every hour) to prevent zombie jobs.

Usage:
  python timeout_worker.py

Or as a Heroku scheduled task:
  heroku addons:create scheduler:standard
  heroku addons:open scheduler
  Then add: python timeout_worker.py (to run hourly)
"""

import os
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import sentry_sdk

# Initialize Sentry for error tracking
sentry_sdk.init(dsn=os.environ.get('SENTRY_DSN'))

# Fix postgres:// URL to postgresql://
db_url = os.environ.get('DATABASE_URL', '').replace('postgres://', 'postgresql://', 1)
if not db_url:
    print("ERROR: DATABASE_URL environment variable not set")
    exit(1)

engine = create_engine(db_url)

# Check what environment we're in
env = os.environ.get('ENVIRONMENT', 'production')
table = 'export_dev' if env == 'dev' else 'export'

# Jobs stuck in "running" for more than 2 hours are considered timed out
# Adjust this threshold based on your typical job duration
TIMEOUT_HOURS = 2
timeout_cutoff = datetime.utcnow() - timedelta(hours=TIMEOUT_HOURS)

print(f"Checking for jobs stuck in 'running' for more than {TIMEOUT_HOURS} hours...")

with engine.begin() as conn:
    # Find timed out jobs
    result = conn.execute(text(f"""
        SELECT id, format, submitted, progress_updated
        FROM {table}
        WHERE status = 'running'
        AND progress_updated < :cutoff
    """), {"cutoff": timeout_cutoff})

    timed_out_jobs = list(result)

    if timed_out_jobs:
        print(f"\nFound {len(timed_out_jobs)} timed out jobs:")
        for job in timed_out_jobs[:10]:
            print(f"  - {job[0]} ({job[1]}) - stuck since {job[3]}")

        if len(timed_out_jobs) > 10:
            print(f"  ... and {len(timed_out_jobs) - 10} more")

        # Mark them as failed
        result = conn.execute(text(f"""
            UPDATE {table}
            SET status = 'failed',
                progress_updated = NOW()
            WHERE status = 'running'
            AND progress_updated < :cutoff
        """), {"cutoff": timeout_cutoff})

        print(f"\n✓ Marked {len(timed_out_jobs)} jobs as 'failed'")
    else:
        print("✓ No timed out jobs found. All running jobs are progressing normally.")

print("\nTimeout check complete.")