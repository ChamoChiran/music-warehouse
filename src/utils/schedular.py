from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
from src.clients.lastfm_client import LastfmClient

def daily_job():
    """
    Job to run daily for fetching Last.fm data.
    """
    print(f"Running Last.fm data fetch job.")
    client = LastfmClient(limit=50, delay=1.5)
    client.run()
    print(f"Job completed.")

def run_scheduler():
    """
    Sets up and starts the scheduler to run the daily job.
    """

    schedular = BlockingScheduler()
    # Run every 24 hours
    schedular.add_job(daily_job, "interval", hours=24)
    print(f"Scheduler started will fetch data every 24 hours.")
    schedular.start()

if __name__ == "__main__":
    run_scheduler()