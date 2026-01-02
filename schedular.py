import schedule
import time
from datetime import datetime
from scripts import run_stock_job  # make sure run_stock_job is defined in scripts.py

def monday_job():
    print("Job started at:", datetime.now())
    run_stock_job()   # call your actual API-to-Snowflake job
    print("Job finished at:", datetime.now())

# Schedule the job every Monday at 08:00 AM
schedule.every().monday.at("08:00").do(monday_job)

# Run the job immediately when the script starts
print("Running the job immediately...")
monday_job()

print("Scheduler started. Waiting for Monday 08:00 AM...")

try:
    while True:
        schedule.run_pending()  # check if the job should run
        time.sleep(1)           # sleep to prevent high CPU usage
except KeyboardInterrupt:
    print("Scheduler stopped by user")