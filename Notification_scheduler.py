import os
import asyncio
import schedule
import time
from datetime import datetime, timedelta
from telegram import Bot
from supabase import create_client, Client

# --- Configuration ---
TELEGRAM_BOT_TOKEN = "8473805508:AAE2w9F1n3Va5TO53rhdqs7ZbOr2VM8IwMA"
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
bot = Bot(token=TELEGRAM_BOT_TOKEN)


# === 1. PRICE ALERTS ===
async def check_price_alerts():
    """ตรวจสอบและส่งแจ้งเตือนราคา"""
    print(f"[{datetime.now()}] Checking price alerts...")
    
    try:
        # ดึง alerts ที่ยังใช้งานอยู่
        alerts = supabase.table("price_alerts")\
            .select("*")\
            .eq("is_active", True)\
            .execute()
        
        if not alerts.data:
            return
        
        for alert in alerts.data:
            symbol = alert['symbol']
            target_price = alert['target_price']
            condition = alert['condition']
            user_id = alert['user_id']
            
            # ดึงราคาปัจจุบัน
            snapshot = supabase.table("stock_snapshots")\
                .select("price, change_pct")\
                .eq("symbol", symbol)\
                .order("recorded_at", desc=True)\
                .limit(1)\
                .execute()
            
            if not snapshot.data:
                continue
            
            current_price = snapshot.data[0]['price']
            change_pct = snapshot.data[0]['change_pct']
            
            # ตรวจสอบเงื่อนไข
            triggered = False
            if condition == 'above' and current_price >= target_price:
                triggered = True
            elif condition == 'below' and current_price <= target_price:
                triggered = True
            
            if triggered:
                # ส่งแจ้งเตือน
                emoji = "🟢" if change_pct > 0 else "🔴"
                message = (
                    f"🔔 **แจ้งเตือนราคา**\n\n"
                    f"**{symbol}** ถึงราคาเป้าหมายแล้ว!\n"
                    f"💰 ราคาปัจจุบัน: ${current_price:.2f}\n"
                    f"🎯 ราคาเป้าหมาย: ${target_price:.2f}\n"
                    f"{emoji} เปลี่ยนแปลง: {change_pct:+.2f}%\n\n"
                    f"พิมพ์ /stock_{symbol} เพื่อดูรายละเอียด"
                )
                
                await bot.send_message(
                    chat_id=user_id,
                    text=message,
                    parse_mode='Markdown'
                )
                
                # อัพเดทสถานะเป็น triggered
                supabase.table("price_alerts")\
                    .update({
                        "is_active": False,
                        "triggered_at": datetime.now().isoformat()
                    })\
                    .eq("id", alert['id'])\
                    .execute()
                
                # บันทึก log
                supabase.table("notification_log").insert({
                    "user_id": user_id,
                    "notification_type": "price_alert",
                    "symbol": symbol,
                    "message": message,
                    "sent_at": datetime.now().isoformat()
                }).execute()
                
                print(f"✅ Sent price alert to user {user_id} for {symbol}")
    
    except Exception as e:
        print(f"❌ Error in check_price_alerts: {e}")


# === 2. RSI ALERTS ===
async def check_rsi_alerts():
    """ตรวจสอบและส่งแจ้งเตือน RSI"""
    print(f"[{datetime.now()}] Checking RSI alerts...")
    
    try:
        # ดึงผู้ใช้ที่เปิดใช้ RSI alerts
        users = supabase.table("user_settings")\
            .select("user_id")\
            .eq("enable_rsi_alerts", True)\
            .execute()
        
        if not users.data:
            return
        
        # ดึง snapshots ล่าสุด
        snapshots = supabase.table("stock_snapshots")\
            .select("*")\
            .not_.is_("rsi", "null")\
            .order("recorded_at", desc=True)\
            .limit(100)\
            .execute()
        
        # จัดกลุ่มตาม symbol (เอาล่าสุด)
        latest_stocks = {}
        for snap in snapshots.data:
            symbol = snap['symbol']
            if symbol not in latest_stocks:
                latest_stocks[symbol] = snap
        
        for user_data in users.data:
            user_id = user_data['user_id']
            
            # ดึง watchlist ของผู้ใช้
            watchlist = supabase.table("user_watchlist")\
                .select("symbol")\
                .eq("user_id", user_id)\
                .execute()
            
            if not watchlist.data:
                continue
            
            watched_symbols = [w['symbol'] for w in watchlist.data]
            
            # ตรวจสอบแต่ละหุ้นใน watchlist
            for symbol in watched_symbols:
                if symbol not in latest_stocks:
                    continue
                
                stock = latest_stocks[symbol]
                rsi = stock.get('rsi')
                price = stock.get('price')
                
                if not rsi:
                    continue
                
                # ตรวจสอบว่าเคยส่งแจ้งเตือนไปแล้วหรือยัง (ใน 24 ชม.)
                last_24h = datetime.now() - timedelta(hours=24)
                recent_notif = supabase.table("notification_log")\
                    .select("id")\
                    .eq("user_id", user_id)\
                    .eq("notification_type", "rsi_alert")\
                    .eq("symbol", symbol)\
                    .gte("sent_at", last_24h.isoformat())\
                    .execute()
                
                if recent_notif.data:
                    continue  # ข้ามถ้าเคยส่งไปแล้ว
                
                # ส่งแจ้งเตือน RSI
                if rsi < 30:
                    message = (
                        f"📊 **แจ้งเตือน RSI Oversold!**\n\n"
                        f"**{symbol}** มี RSI ต่ำมาก - โอกาสซื้อ!\n"
                        f"📉 RSI: {rsi:.1f} (Oversold)\n"
                        f"💵 ราคา: ${price:.2f}\n"
                        f"🎯 แนะนำ: พิจารณาเข้าซื้อ\n\n"
                        f"พิมพ์ /stock_{symbol} เพื่อดูรายละเอียด"
                    )
                    
                    await bot.send_message(
                        chat_id=user_id,
                        text=message,
                        parse_mode='Markdown'
                    )
                    
                    supabase.table("notification_log").insert({
                        "user_id": user_id,
                        "notification_type": "rsi_alert",
                        "symbol": symbol,
                        "message": message
                    }).execute()
                    
                    print(f"✅ Sent RSI oversold alert to user {user_id} for {symbol}")
                
                elif rsi > 70:
                    message = (
                        f"📊 **แจ้งเตือน RSI Overbought!**\n\n"
                        f"**{symbol}** มี RSI สูงมาก - โอกาสขาย!\n"
                        f"📈 RSI: {rsi:.1f} (Overbought)\n"
                        f"💵 ราคา: ${price:.2f}\n"
                        f"🎯 แนะนำ: พิจารณาขายหรือทำกำไร\n\n"
                        f"พิมพ์ /stock_{symbol} เพื่อดูรายละเอียด"
                    )
                    
                    await bot.send_message(
                        chat_id=user_id,
                        text=message,
                        parse_mode='Markdown'
                    )
                    
                    supabase.table("notification_log").insert({
                        "user_id": user_id,
                        "notification_type": "rsi_alert",
                        "symbol": symbol,
                        "message": message
                    }).execute()
                    
                    print(f"✅ Sent RSI overbought alert to user {user_id} for {symbol}")
    
    except Exception as e:
        print(f"❌ Error in check_rsi_alerts: {e}")


# === 3. AI SCORE ALERTS ===
async def check_ai_score_alerts():
    """ตรวจสอบและส่งแจ้งเตือน AI Score สูง"""
    print(f"[{datetime.now()}] Checking AI score alerts...")
    
    try:
        # ดึงผู้ใช้ที่เปิดใช้ AI alerts
        users = supabase.table("user_settings")\
            .select("user_id")\
            .eq("enable_ai_alerts", True)\
            .execute()
        
        if not users.data:
            return
        
        # ดึง predictions ที่เพิ่งสร้าง (ใน 1 ชม.ล่าสุด)
        one_hour_ago = datetime.now() - timedelta(hours=1)
        predictions = supabase.table("ai_predictions")\
            .select("*")\
            .gte("overall_score", 75)\
            .gte("created_at", one_hour_ago.isoformat())\
            .execute()
        
        if not predictions.data:
            return
        
        for user_data in users.data:
            user_id = user_data['user_id']
            
            for pred in predictions.data:
                symbol = pred['symbol']
                score = pred['overall_score']
                rec = pred['recommendation']
                price = pred['price_at_prediction']
                
                # ตรวจสอบว่าเคยส่งแจ้งเตือนไปแล้วหรือยัง
                recent_notif = supabase.table("notification_log")\
                    .select("id")\
                    .eq("user_id", user_id)\
                    .eq("notification_type", "ai_score_alert")\
                    .eq("symbol", symbol)\
                    .gte("sent_at", one_hour_ago.isoformat())\
                    .execute()
                
                if recent_notif.data:
                    continue
                
                # ดึงข้อมูลล่าสุด
                snapshot = supabase.table("stock_snapshots")\
                    .select("upside_pct")\
                    .eq("symbol", symbol)\
                    .order("recorded_at", desc=True)\
                    .limit(1)\
                    .execute()
                
                upside_pct = snapshot.data[0].get('upside_pct') if snapshot.data else None
                
                message = (
                    f"🤖 **แจ้งเตือน AI Score สูง!**\n\n"
                    f"**{symbol}** ได้คะแนน AI สูงมาก!\n"
                    f"⭐ Score: {score}/100\n"
                    f"🟢 Recommendation: {rec}\n"
                    f"💰 ราคา: ${price:.2f}\n"
                )
                
                if upside_pct:
                    message += f"📈 Upside: {upside_pct:+.1f}%\n"
                
                message += f"\nพิมพ์ /stock_{symbol} เพื่อดูรายละเอียด"
                
                await bot.send_message(
                    chat_id=user_id,
                    text=message,
                    parse_mode='Markdown'
                )
                
                supabase.table("notification_log").insert({
                    "user_id": user_id,
                    "notification_type": "ai_score_alert",
                    "symbol": symbol,
                    "message": message
                }).execute()
                
                print(f"✅ Sent AI score alert to user {user_id} for {symbol}")
    
    except Exception as e:
        print(f"❌ Error in check_ai_score_alerts: {e}")


# === 4. NEWS ALERTS ===
async def check_news_alerts():
    """ตรวจสอบและส่งแจ้งเตือนข่าวใหม่"""
    print(f"[{datetime.now()}] Checking news alerts...")
    
    try:
        # ดึงผู้ใช้ที่เปิดใช้ news alerts
        users = supabase.table("user_settings")\
            .select("user_id")\
            .eq("enable_news_alerts", True)\
            .execute()
        
        if not users.data:
            return
        
        # ดึงข่าวที่เพิ่งเผยแพร่ (15 นาทีล่าสุด)
        fifteen_min_ago = datetime.now() - timedelta(minutes=15)
        news = supabase.table("stock_news")\
            .select("*")\
            .gte("published_at", fifteen_min_ago.isoformat())\
            .execute()
        
        if not news.data:
            return
        
        # กรองเฉพาะข่าวที่มี sentiment แรง (> 0.5 หรือ < -0.5)
        strong_news = [
            n for n in news.data 
            if n.get('sentiment_score') is not None 
            and abs(n['sentiment_score']) > 0.5
        ]
        
        for user_data in users.data:
            user_id = user_data['user_id']
            
            # ดึง watchlist
            watchlist = supabase.table("user_watchlist")\
                .select("symbol")\
                .eq("user_id", user_id)\
                .execute()
            
            if not watchlist.data:
                continue
            
            watched_symbols = [w['symbol'] for w in watchlist.data]
            
            for article in strong_news:
                symbol = article['symbol']
                
                if symbol not in watched_symbols:
                    continue
                
                # ตรวจสอบว่าเคยส่งแจ้งเตือนไปแล้วหรือยัง
                recent_notif = supabase.table("notification_log")\
                    .select("id")\
                    .eq("user_id", user_id)\
                    .eq("notification_type", "news_alert")\
                    .eq("symbol", symbol)\
                    .gte("sent_at", fifteen_min_ago.isoformat())\
                    .execute()
                
                if recent_notif.data:
                    continue
                
                title = article['title_th'][:100] + "..." if len(article['title_th']) > 100 else article['title_th']
                sentiment = article['sentiment_score']
                sent_emoji = "🟢" if sentiment > 0 else "🔴"
                sent_text = "เชิงบวก" if sentiment > 0 else "เชิงลบ"
                
                message = (
                    f"📰 **ข่าวด่วน!**\n\n"
                    f"**{symbol}** - ข่าว{sent_text} {sent_emoji}\n"
                    f"📌 {title}\n\n"
                    f"😊 Sentiment: {sentiment:+.2f}\n"
                )
                
                if article.get('url'):
                    message += f"🔗 [อ่านเพิ่มเติม]({article['url']})\n"
                
                message += f"\nพิมพ์ /stock_{symbol} เพื่อดูรายละเอียด"
                
                await bot.send_message(
                    chat_id=user_id,
                    text=message,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
                
                supabase.table("notification_log").insert({
                    "user_id": user_id,
                    "notification_type": "news_alert",
                    "symbol": symbol,
                    "message": message
                }).execute()
                
                print(f"✅ Sent news alert to user {user_id} for {symbol}")
    
    except Exception as e:
        print(f"❌ Error in check_news_alerts: {e}")


# === 5. DAILY MARKET SUMMARY ===
async def send_daily_summary():
    """ส่งสรุปตลาดรายวัน"""
    print(f"[{datetime.now()}] Sending daily market summary...")
    
    try:
        # ดึงผู้ใช้ที่เปิดใช้ daily summary
        users = supabase.table("user_settings")\
            .select("user_id")\
            .eq("enable_daily_summary", True)\
            .execute()
        
        if not users.data:
            return
        
        # ดึงข้อมูลสรุป
        snapshots = supabase.table("stock_snapshots")\
            .select("*")\
            .order("recorded_at", desc=True)\
            .limit(100)\
            .execute()
        
        # จัดกลุ่มตาม symbol
        latest_stocks = {}
        for snap in snapshots.data:
            symbol = snap['symbol']
            if symbol not in latest_stocks:
                latest_stocks[symbol] = snap
        
        stocks_list = list(latest_stocks.values())
        
        # Top Gainers
        top_gainers = sorted(
            [s for s in stocks_list if s.get('change_pct')], 
            key=lambda x: x['change_pct'], 
            reverse=True
        )[:3]
        
        # Top Losers
        top_losers = sorted(
            [s for s in stocks_list if s.get('change_pct')], 
            key=lambda x: x['change_pct']
        )[:3]
        
        # AI Top Picks
        ai_preds = supabase.table("ai_predictions")\
            .select("symbol, overall_score, recommendation")\
            .gte("overall_score", 75)\
            .order("overall_score", desc=True)\
            .limit(3)\
            .execute()
        
        # คำนวณ % หุ้นที่ขึ้น
        up_stocks = len([s for s in stocks_list if s.get('change_pct', 0) > 0])
        total_stocks = len(stocks_list)
        up_pct = (up_stocks / total_stocks * 100) if total_stocks > 0 else 0
        
        # สร้างข้อความ
        today = datetime.now().strftime("%d %b %Y")
        message = f"⏰ **สรุปตลาดวันที่ {today}**\n\n"
        
        message += "📊 **ภาพรวม**\n"
        message += f"• จำนวนหุ้นขึ้น: {up_pct:.0f}%\n\n"
        
        message += "🔥 **Top Gainers**\n"
        for i, stock in enumerate(top_gainers, 1):
            message += f"{i}. {stock['symbol']}: {stock['change_pct']:+.2f}%\n"
        
        message += "\n❄️ **Top Losers**\n"
        for i, stock in enumerate(top_losers, 1):
            message += f"{i}. {stock['symbol']}: {stock['change_pct']:+.2f}%\n"
        
        if ai_preds.data:
            message += "\n🤖 **AI Picks วันนี้**\n"
            for pred in ai_preds.data:
                message += f"• {pred['symbol']} ({pred['overall_score']}/100) - {pred['recommendation']}\n"
        
        message += "\n💡 มีคำถาม? พิมพ์ /start"
        
        # ส่งให้ผู้ใช้ทั้งหมด
        for user_data in users.data:
            user_id = user_data['user_id']
            
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=message,
                    parse_mode='Markdown'
                )
                
                supabase.table("notification_log").insert({
                    "user_id": user_id,
                    "notification_type": "daily_summary",
                    "symbol": None,
                    "message": message
                }).execute()
                
                print(f"✅ Sent daily summary to user {user_id}")
                
            except Exception as e:
                print(f"⚠️ Failed to send to user {user_id}: {e}")
        
        print(f"✅ Daily summary sent to {len(users.data)} users")
    
    except Exception as e:
        print(f"❌ Error in send_daily_summary: {e}")


# === SCHEDULER ===
def schedule_jobs():
    """กำหนดตารางเวลาทำงาน"""
    
    # Price Alerts - ทุก 5 นาที
    schedule.every(5).minutes.do(lambda: asyncio.run(check_price_alerts()))
    
    # RSI Alerts - ทุก 30 นาที
    schedule.every(30).minutes.do(lambda: asyncio.run(check_rsi_alerts()))
    
    # AI Score Alerts - ทุก 1 ชั่วโมง
    schedule.every(1).hours.do(lambda: asyncio.run(check_ai_score_alerts()))
    
    # News Alerts - ทุก 15 นาที
    schedule.every(15).minutes.do(lambda: asyncio.run(check_news_alerts()))
    
    # Daily Summary - ทุกวันเวลา 09:00 น.
    schedule.every().day.at("09:00").do(lambda: asyncio.run(send_daily_summary()))
    
    print("✅ Scheduler initialized!")
    print("📅 Jobs scheduled:")
    print("   - Price Alerts: Every 5 minutes")
    print("   - RSI Alerts: Every 30 minutes")
    print("   - AI Score Alerts: Every 1 hour")
    print("   - News Alerts: Every 15 minutes")
    print("   - Daily Summary: Every day at 09:00")


def main():
    """เริ่มต้น Scheduler"""
    print("🚀 Starting notification scheduler...")
    
    schedule_jobs()
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # ตรวจสอบทุก 1 นาที


if __name__ == "__main__":
    main()
