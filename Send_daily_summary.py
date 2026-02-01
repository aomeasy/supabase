import os
import asyncio
from datetime import datetime
from supabase import create_client, Client
from telegram import Bot

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # เพิ่ม Chat ID ของคุณ
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("❌ Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_emoji_by_score(score):
    """คืนค่า emoji ตาม score"""
    if score >= 80:
        return "🌟"
    elif score >= 70:
        return "✅"
    elif score >= 60:
        return "👍"
    elif score >= 50:
        return "😐"
    elif score >= 40:
        return "⚠️"
    else:
        return "❌"


async def send_daily_summary():
    """ส่งสรุปหุ้นแนะนำรายวัน"""
    
    # ดึงหุ้นที่ score >= 70
    predictions = supabase.table("ai_predictions")\
        .select("symbol, overall_score, recommendation, price_at_prediction")\
        .gte("overall_score", 70)\
        .order("overall_score", desc=True)\
        .limit(10)\
        .execute()
    
    if not predictions.data:
        message = "📭 วันนี้ยังไม่มีหุ้นที่แนะนำ (Score >= 70)"
    else:
        message = f"⭐ <b>หุ้นแนะนำประจำวันที่ {datetime.now().strftime('%d/%m/%Y')}</b>\n\n"
        
        for idx, pred in enumerate(predictions.data, 1):
            symbol = pred['symbol']
            score = pred['overall_score']
            rec = pred['recommendation']
            price = pred['price_at_prediction']
            emoji = get_emoji_by_score(score)
            
            message += f"{idx}. {emoji} <b>{symbol}</b>\n"
            message += f"   Score: <b>{score}/100</b> | {rec}\n"
            message += f"   Price: <b>${price:.2f}</b>\n\n"
        
        message += f"\n<i>อัพเดท: {datetime.now().strftime('%d/%m/%Y %H:%M')}</i>\n"
        message += "\n💡 พิมพ์ /start เพื่อดูรายละเอียดเพิ่มเติม"
    
    # ส่งข้อความ
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=message,
        parse_mode='HTML'
    )
    
    print(f"✅ Daily summary sent to Telegram at {datetime.now()}")


if __name__ == "__main__":
    asyncio.run(send_daily_summary())
