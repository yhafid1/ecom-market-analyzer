import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apscheduler.schedulers.blocking import BlockingScheduler
from etl.pipeline import run_pipeline
from dotenv import load_dotenv

load_dotenv()

scheduler = BlockingScheduler()

@scheduler.scheduled_job(
    "cron",
    hour=int(os.getenv("PIPELINE_SCHEDULE_HOUR", 6)),
    minute=0,
    id="daily_pipeline",
)
def scheduled_run():
    run_pipeline()

if __name__ == "__main__":
    print(f"Scheduler started — pipeline runs daily at {os.getenv('PIPELINE_SCHEDULE_HOUR', 6)}:00 UTC")
    scheduler.start()
