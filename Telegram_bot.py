import os
import asyncio
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, 
    CommandHandler, 
    CallbackQueryHandler, 
    ContextTypes,
    MessageHandler,
    filters
)
from supabase import create_client, Client

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not TELEGRAM_BOT_TOKEN:
    raise ValueError("❌ Missing TELEGRAM_BOT_TOKEN in environment variables")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Helper Functions ---

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


def get_emoji_by_recommendation(rec):
    """คืนค่า emoji ตามคำแนะนำ"""
    rec_lower = rec.lower()
    if "strong buy" in rec_lower:
        return "🚀"
    elif "buy" in rec_lower:
        return "✅"
    elif "hold" in rec_lower:
        return "⏸️"
    elif "sell" in rec_lower:
        return "⬇️"
    else:
        return "📊"


def format_stock_detail(symbol, snapshot, prediction):
    """จัดรูปแบบข้อมูลหุ้นแบบละเอียด"""
    
    score = prediction.get('overall_score', 0)
    rec = prediction.get('recommendation', 'N/A')
    
    price = snapshot.get('price', 0)
    change_pct = snapshot.get('change_pct', 0)
    upside_pct = snapshot.get('upside_pct', 0)
    
    rsi = snapshot.get('rsi')
    macd = snapshot.get('macd')
    ema_20 = snapshot.get('ema_20')
    
    # สร้างข้อความ
    emoji_score = get_emoji_by_score(score)
    emoji_rec = get_emoji_by_recommendation(rec)
    change_emoji = "📈" if change_pct > 0 else "📉"
    
    text = f"""
{emoji_score} <b>{symbol}</b> {emoji_rec}

━━━━━━━━━━━━━━━━━━━━
<b>📊 AI Analysis</b>
• Overall Score: <b>{score}/100</b>
• Recommendation: <b>{rec}</b>

<b>💰 Price Info</b>
• Current: <b>${price:.2f}</b>
• Change: {change_emoji} <b>{change_pct:+.2f}%</b>
• Upside Potential: <b>{upside_pct:.2f}%</b>

<b>📈 Technical Indicators</b>
• RSI: {f"<b>{rsi:.2f}</b>" if rsi else "N/A"}
• MACD: {f"<b>{macd:.4f}</b>" if macd else "N/A"}
• EMA 20: {f"<b>${ema_20:.2f}</b>" if ema_20 else "N/A"}

<i>อัพเดทล่าสุด: {datetime.now().strftime('%d/%m/%Y %H:%M')}</i>
━━━━━━━━━━━━━━━━━━━━
"""
    return text


def format_news_item(news):
    """จัดรูปแบบข่าว"""
    title_th = news.get('title_th') or news.get('title', '')
    sentiment = news.get('sentiment_score', 0)
    published = news.get('published_at', '')
    source = news.get('source', 'Unknown')
    url = news.get('url', '')
    
    # Emoji ตาม sentiment
    if sentiment > 0.3:
        emoji = "😊"
    elif sentiment < -0.3:
        emoji = "😔"
    else:
        emoji = "😐"
    
    # แปลงวันที่
    try:
        pub_date = datetime.fromisoformat(published.replace('Z', '+00:00'))
        date_str = pub_date.strftime('%d/%m %H:%M')
    except:
        date_str = "N/A"
    
    text = f"""
{emoji} <b>{title_th[:80]}...</b>

📅 {date_str} | 📰 {source}
💭 Sentiment: <b>{sentiment:+.2f}</b>
🔗 <a href="{url}">อ่านเพิ่มเติม</a>
━━━━━━━━━━━━━━━━━━━━
"""
    return text


# --- Command Handlers ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """คำสั่ง /start - แสดงเมนูหลัก"""
    keyboard = [
        [
            InlineKeyboardButton("📈 ดูหุ้นทั้งหมด", callback_data="view_all_stocks"),
            InlineKeyboardButton("⭐ หุ้นแนะนำวันนี้", callback_data="recommended_stocks")
        ],
        [
            InlineKeyboardButton("📰 ข่าวล่าสุด", callback_data="latest_news"),
            InlineKeyboardButton("🔔 ตั้งค่าแจ้งเตือน", callback_data="notifications")
        ],
        [
            InlineKeyboardButton("ℹ️ วิธีใช้งาน", callback_data="help")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    welcome_text = """
🤖 <b>ยินดีต้อนรับสู่ Stock Analysis Bot!</b>

ระบบวิเคราะห์หุ้นอัจฉริยะที่ใช้ AI ช่วยตัดสินใจลงทุน

<b>✨ ฟีเจอร์หลัก:</b>
• วิเคราะห์เทคนิคแบบ Real-time
• AI Recommendation Score
• ข่าวล่าสุดพร้อม Sentiment Analysis
• การแจ้งเตือนอัตโนมัติ

เลือกเมนูด้านล่างเพื่อเริ่มต้น 👇
"""
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=reply_markup,
        parse_mode='HTML'
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """จัดการปุ่มที่ถูกกด"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    # === 1. ดูหุ้นทั้งหมด ===
    if data == "view_all_stocks":
        keyboard = [
            [InlineKeyboardButton("🌟 Core Stocks", callback_data="category_Core")],
            [InlineKeyboardButton("🚀 Growth Stocks", callback_data="category_Growth")],
            [InlineKeyboardButton("📦 ETF", callback_data="category_ETF")],
            [InlineKeyboardButton("🔙 กลับเมนูหลัก", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "📊 <b>เลือกหมวดหมู่หุ้น:</b>",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    # === 2. ดูหุ้นตาม Category ===
    elif data.startswith("category_"):
        category = data.split("_")[1]
        
        # ดึงหุ้นจาก stock_master
        stocks = supabase.table("stock_master")\
            .select("symbol")\
            .eq("category", category)\
            .eq("is_active", True)\
            .execute()
        
        if not stocks.data:
            await query.edit_message_text(
                f"❌ ไม่พบหุ้นในหมวด {category}",
                parse_mode='HTML'
            )
            return
        
        # สร้างปุ่ม
        keyboard = []
        for stock in stocks.data:
            symbol = stock['symbol']
            keyboard.append([InlineKeyboardButton(
                f"📊 {symbol}", 
                callback_data=f"stock_{symbol}"
            )])
        
        keyboard.append([InlineKeyboardButton("🔙 กลับ", callback_data="view_all_stocks")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f"📈 <b>หุ้นในหมวด {category}:</b>\n\nเลือกหุ้นที่ต้องการดูรายละเอียด 👇",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    # === 3. ดูรายละเอียดหุ้น ===
    elif data.startswith("stock_"):
        symbol = data.split("_")[1]
        
        # ดึงข้อมูลล่าสุด
        snapshot = supabase.table("stock_snapshots")\
            .select("*")\
            .eq("symbol", symbol)\
            .order("recorded_at", desc=True)\
            .limit(1)\
            .execute()
        
        prediction = supabase.table("ai_predictions")\
            .select("*")\
            .eq("symbol", symbol)\
            .order("created_at", desc=True)\
            .limit(1)\
            .execute()
        
        if not snapshot.data or not prediction.data:
            await query.edit_message_text(
                f"❌ ไม่พบข้อมูลสำหรับ {symbol}",
                parse_mode='HTML'
            )
            return
        
        # จัดรูปแบบและแสดงผล
        text = format_stock_detail(symbol, snapshot.data[0], prediction.data[0])
        
        keyboard = [
            [InlineKeyboardButton("📰 ดูข่าวล่าสุด", callback_data=f"news_{symbol}")],
            [InlineKeyboardButton("🔙 กลับ", callback_data="view_all_stocks")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
    
    # === 4. หุ้นแนะนำวันนี้ ===
    elif data == "recommended_stocks":
        # ดึงหุ้นที่ score >= 70
        predictions = supabase.table("ai_predictions")\
            .select("symbol, overall_score, recommendation")\
            .gte("overall_score", 70)\
            .order("overall_score", desc=True)\
            .limit(10)\
            .execute()
        
        if not predictions.data:
            await query.edit_message_text(
                "📭 วันนี้ยังไม่มีหุ้นที่แนะนำ (Score >= 70)",
                parse_mode='HTML'
            )
            return
        
        text = "⭐ <b>หุ้นแนะนำวันนี้</b>\n\n"
        
        for pred in predictions.data:
            symbol = pred['symbol']
            score = pred['overall_score']
            rec = pred['recommendation']
            emoji = get_emoji_by_score(score)
            
            text += f"{emoji} <b>{symbol}</b> - {score}/100 ({rec})\n"
        
        text += f"\n<i>อัพเดท: {datetime.now().strftime('%d/%m/%Y %H:%M')}</i>"
        
        keyboard = [[InlineKeyboardButton("🔙 กลับเมนูหลัก", callback_data="back_to_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    # === 5. ข่าวล่าสุด ===
    elif data == "latest_news":
        keyboard = [
            [InlineKeyboardButton("🔥 ข่าวทั้งหมด", callback_data="news_all")],
            [InlineKeyboardButton("😊 ข่าวบวก", callback_data="news_positive")],
            [InlineKeyboardButton("😔 ข่าวลบ", callback_data="news_negative")],
            [InlineKeyboardButton("🔙 กลับเมนูหลัก", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "📰 <b>เลือกประเภทข่าว:</b>",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    # === 6. ข่าวทั้งหมด/บวก/ลบ ===
    elif data.startswith("news_"):
        news_type = data.split("_")[1]
        
        query_builder = supabase.table("stock_news")\
            .select("*")\
            .order("published_at", desc=True)
        
        if news_type == "positive":
            query_builder = query_builder.gte("sentiment_score", 0.3)
        elif news_type == "negative":
            query_builder = query_builder.lte("sentiment_score", -0.3)
        
        news_list = query_builder.limit(5).execute()
        
        if not news_list.data:
            await query.edit_message_text(
                "📭 ไม่พบข่าว",
                parse_mode='HTML'
            )
            return
        
        text = "📰 <b>ข่าวล่าสุด</b>\n\n"
        
        for news in news_list.data:
            text += format_news_item(news)
        
        keyboard = [[InlineKeyboardButton("🔙 กลับ", callback_data="latest_news")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
    
    # === 7. ตั้งค่าการแจ้งเตือน ===
    elif data == "notifications":
        text = """
🔔 <b>ตั้งค่าการแจ้งเตือน</b>

<b>ฟีเจอร์ที่จะมีในอนาคต:</b>
• แจ้งเตือนรายวัน (8:00 AM)
• แจ้งข่าวด่วน (Sentiment < -0.5)
• แจ้ง Score เปลี่ยนแปลง > 10 คะแนน
• ติดตามหุ้นที่สนใจ

<i>🚧 กำลังพัฒนา...</i>
"""
        keyboard = [[InlineKeyboardButton("🔙 กลับเมนูหลัก", callback_data="back_to_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    # === 8. วิธีใช้งาน ===
    elif data == "help":
        text = """
ℹ️ <b>วิธีใช้งาน Stock Analysis Bot</b>

<b>📊 ความหมายของ Score:</b>
• 80-100: Strong Buy 🌟
• 70-79: Buy ✅
• 60-69: Moderate Buy 👍
• 50-59: Hold 😐
• 40-49: Caution ⚠️
• 0-39: Avoid ❌

<b>📈 Technical Indicators:</b>
• RSI: วัดความแข็งแกร่งของแนวโน้ม
• MACD: สัญญาณซื้อ/ขาย
• EMA: แนวโน้มราคาเฉลี่ย

<b>💡 Tips:</b>
• Score >= 70 = น่าสนใจลงทุน
• Sentiment > 0.3 = ข่าวดี
• Upside > 10% = มี Potential

<b>📞 ติดต่อ:</b>
/start - เมนูหลัก
/recommended - หุ้นแนะนำ
"""
        keyboard = [[InlineKeyboardButton("🔙 กลับเมนูหลัก", callback_data="back_to_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    # === 9. กลับเมนูหลัก ===
    elif data == "back_to_main":
        keyboard = [
            [
                InlineKeyboardButton("📈 ดูหุ้นทั้งหมด", callback_data="view_all_stocks"),
                InlineKeyboardButton("⭐ หุ้นแนะนำวันนี้", callback_data="recommended_stocks")
            ],
            [
                InlineKeyboardButton("📰 ข่าวล่าสุด", callback_data="latest_news"),
                InlineKeyboardButton("🔔 ตั้งค่าแจ้งเตือน", callback_data="notifications")
            ],
            [
                InlineKeyboardButton("ℹ️ วิธีใช้งาน", callback_data="help")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "🤖 <b>Stock Analysis Bot</b>\n\nเลือกเมนูด้านล่าง 👇",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )


# === คำสั่งพิเศษ ===

async def recommended_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """คำสั่ง /recommended - แสดงหุ้นแนะนำโดยตรง"""
    predictions = supabase.table("ai_predictions")\
        .select("symbol, overall_score, recommendation")\
        .gte("overall_score", 70)\
        .order("overall_score", desc=True)\
        .limit(10)\
        .execute()
    
    if not predictions.data:
        await update.message.reply_text(
            "📭 วันนี้ยังไม่มีหุ้นที่แนะนำ (Score >= 70)"
        )
        return
    
    text = "⭐ <b>หุ้นแนะนำวันนี้</b>\n\n"
    
    for pred in predictions.data:
        symbol = pred['symbol']
        score = pred['overall_score']
        rec = pred['recommendation']
        emoji = get_emoji_by_score(score)
        
        text += f"{emoji} <b>{symbol}</b> - {score}/100 ({rec})\n"
    
    text += f"\n<i>อัพเดท: {datetime.now().strftime('%d/%m/%Y %H:%M')}</i>"
    
    await update.message.reply_text(text, parse_mode='HTML')


# === Main Function ===

def main():
    """เริ่มต้น Bot"""
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # เพิ่ม handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("recommended", recommended_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    print("🤖 Bot started successfully!")
    print("📱 Bot username: @stock_newss_2bot")
    print("🔗 Link: https://t.me/stock_newss_2bot")
    
    # รัน bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
