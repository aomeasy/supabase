# schedule_daily_summary.py
import schedule
import time

def send_daily_summary():
    users = get_all_active_users()
    summary = generate_market_summary()
    
    for user in users:
        send_message(user.user_id, summary)

schedule.every().day.at("09:00").do(send_daily_summary)

while True:
    schedule.run_pending()
    time.sleep(60)
