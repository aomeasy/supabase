import os
import asyncio
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, 
    CommandHandler, 
    CallbackQueryHandler, 
    ContextTypes,
    ConversationHandler
)
from supabase import create_client, Client

# --- Configuration ---
TELEGRAM_BOT_TOKEN = "8473805508:AAE2w9F1n3Va5TO53rhdqs7ZbOr2VM8IwMA"
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Conversation states
SELECTING_STOCK = 1


# === MAIN MENU ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """แสดงเมนูหลักเมื่อเริ่มต้น"""
    keyboard = [
        [
            InlineKeyboardButton("📊 ดูภาพรวมตลาด", callback_data="market_overview"),
            InlineKeyboardButton("⭐ หุ้นแนะนำ", callback_data="top_picks")
        ],
        [
            InlineKeyboardButton("🔍 ค้นหาหุ้น", callback_data="search_stock"),
            InlineKeyboardButton("📈 หุ้นที่ติดตาม", callback_data="watchlist")
        ],
        [
            InlineKeyboardButton("📰 ข่าวล่าสุด", callback_data="latest_news"),
            InlineKeyboardButton("🤖 AI Predictions", callback_data="ai_predictions")
        ],
        [
            InlineKeyboardButton("⚙️ ตั้งค่าการแจ้งเตือน", callback_data="settings")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    welcome_text = (
        "🎯 **ยินดีต้อนรับสู่ Stock Analysis Bot**\n\n"
        "ระบบวิเคราะห์หุ้นด้วย AI ที่จะช่วยคุณ:\n"
        "• ติดตามราคาหุ้นแบบ Real-time\n"
        "• วิเคราะห์ทางเทคนิค (RSI, MACD, EMA)\n"
        "• รับคำแนะนำจาก AI Score\n"
        "• อ่านข่าวภาษาไทย\n\n"
        "เลือกเมนูด้านล่างเพื่อเริ่มต้น 👇"
    )
    
    if update.message:
        await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.callback_query.message.edit_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')


# === 1. MARKET OVERVIEW ===
async def market_overview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """แสดงภาพรวมตลาด - Top Gainers, Losers, AI Scores"""
    query = update.callback_query
    await query.answer()
    
    try:
        # ดึงข้อมูลล่าสุดจาก stock_snapshots
        snapshots = supabase.table("stock_snapshots")\
            .select("*")\
            .order("recorded_at", desc=True)\
            .limit(100)\
            .execute()
        
        if not snapshots.data:
            await query.message.edit_text("❌ ไม่พบข้อมูลหุ้น")
            return
        
        # จัดกลุ่มตาม symbol และเอาล่าสุด
        latest_stocks = {}
        for snap in snapshots.data:
            symbol = snap['symbol']
            if symbol not in latest_stocks:
                latest_stocks[symbol] = snap
        
        stocks_list = list(latest_stocks.values())
        
        # Top Gainers (เรียงตาม change_pct)
        top_gainers = sorted(
            [s for s in stocks_list if s.get('change_pct')], 
            key=lambda x: x['change_pct'], 
            reverse=True
        )[:5]
        
        # Top Losers
        top_losers = sorted(
            [s for s in stocks_list if s.get('change_pct')], 
            key=lambda x: x['change_pct']
        )[:5]
        
        # Top AI Scores (ดึงจาก ai_predictions)
        ai_preds = supabase.table("ai_predictions")\
            .select("symbol, overall_score, recommendation")\
            .order("created_at", desc=True)\
            .limit(50)\
            .execute()
        
        top_ai_scores = sorted(
            ai_preds.data if ai_preds.data else [],
            key=lambda x: x.get('overall_score', 0),
            reverse=True
        )[:5]
        
        # สร้างข้อความ
        message = "📊 **ภาพรวมตลาดวันนี้**\n\n"
        
        message += "🔥 **Top Gainers**\n"
        for stock in top_gainers:
            emoji = "🚀" if stock['change_pct'] > 5 else "📈"
            message += f"{emoji} {stock['symbol']}: ${stock['price']:.2f} (+{stock['change_pct']:.2f}%)\n"
        
        message += "\n❄️ **Top Losers**\n"
        for stock in top_losers:
            emoji = "💔" if stock['change_pct'] < -5 else "📉"
            message += f"{emoji} {stock['symbol']}: ${stock['price']:.2f} ({stock['change_pct']:.2f}%)\n"
        
        if top_ai_scores:
            message += "\n🤖 **AI Top Picks**\n"
            for pred in top_ai_scores:
                message += f"⭐ {pred['symbol']}: {pred['overall_score']}/100 ({pred['recommendation']})\n"
        
        # ปุ่มกลับเมนูหลัก
        keyboard = [[InlineKeyboardButton("🏠 กลับเมนูหลัก", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        await query.message.edit_text(f"❌ เกิดข้อผิดพลาด: {e}")


# === 2. TOP PICKS (AI Recommendations) ===
async def top_picks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """แสดงหุ้นที่ AI แนะนำ (Score >= 70)"""
    query = update.callback_query
    await query.answer()
    
    try:
        # ดึง predictions ที่มี score >= 70
        predictions = supabase.table("ai_predictions")\
            .select("symbol, overall_score, recommendation, price_at_prediction, created_at")\
            .gte("overall_score", 70)\
            .order("overall_score", desc=True)\
            .limit(10)\
            .execute()
        
        if not predictions.data:
            message = "📭 ยังไม่มีหุ้นที่ AI แนะนำในขณะนี้"
        else:
            message = "⭐ **หุ้นที่ AI แนะนำ (Score >= 70/100)**\n\n"
            
            for idx, pred in enumerate(predictions.data, 1):
                symbol = pred['symbol']
                score = pred['overall_score']
                rec = pred['recommendation']
                price = pred['price_at_prediction']
                
                # ดึงข้อมูลล่าสุด
                latest = supabase.table("stock_snapshots")\
                    .select("price, change_pct, rsi, upside_pct")\
                    .eq("symbol", symbol)\
                    .order("recorded_at", desc=True)\
                    .limit(1)\
                    .execute()
                
                if latest.data:
                    current = latest.data[0]
                    change = ((current['price'] - price) / price * 100) if price else 0
                    
                    message += f"{idx}. **{symbol}** - Score: {score}/100\n"
                    message += f"   📊 ${current['price']:.2f} ({current['change_pct']:.2f}%)\n"
                    message += f"   🎯 {rec}\n"
                    if current.get('rsi'):
                        message += f"   📈 RSI: {current['rsi']:.1f}"
                    if current.get('upside_pct'):
                        message += f" | Upside: {current['upside_pct']:.1f}%"
                    message += f"\n   💰 เปลี่ยนแปลง: {change:+.2f}%\n\n"
            
            message += "💡 _กดที่ชื่อหุ้นเพื่อดูรายละเอียดเพิ่มเติม_"
        
        # สร้างปุ่มเลือกหุ้น
        keyboard = []
        if predictions.data:
            # แบ่งเป็น 3 หุ้นต่อแถว
            for i in range(0, len(predictions.data), 3):
                row = []
                for pred in predictions.data[i:i+3]:
                    row.append(InlineKeyboardButton(
                        pred['symbol'], 
                        callback_data=f"stock_detail:{pred['symbol']}"
                    ))
                keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("🏠 กลับเมนูหลัก", callback_data="main_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        await query.message.edit_text(f"❌ เกิดข้อผิดพลาด: {e}")


# === 3. SEARCH STOCK (แสดงหุ้นทั้งหมดให้เลือก) ===
async def search_stock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """แสดงหุ้นทั้งหมดแยกตาม category"""
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [
            InlineKeyboardButton("📊 Core Stocks", callback_data="category:Core"),
            InlineKeyboardButton("🚀 Growth Stocks", callback_data="category:Growth")
        ],
        [
            InlineKeyboardButton("💰 Value Stocks", callback_data="category:Value"),
            InlineKeyboardButton("📈 Dividend Stocks", callback_data="category:Dividend")
        ],
        [
            InlineKeyboardButton("🛡️ ETFs", callback_data="category:ETF")
        ],
        [
            InlineKeyboardButton("🏠 กลับเมนูหลัก", callback_data="main_menu")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message = (
        "🔍 **ค้นหาหุ้น**\n\n"
        "เลือกหมวดหมู่ที่ต้องการดู:\n\n"
        "📊 **Core** - หุ้นหลักที่แนะนำ\n"
        "🚀 **Growth** - หุ้นเติบโตสูง\n"
        "💰 **Value** - หุ้นมูลค่าต่ำกว่าจริง\n"
        "📈 **Dividend** - หุ้นจ่ายเงินปันผล\n"
        "🛡️ **ETF** - กองทุน Index Fund"
    )
    
    await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='Markdown')


async def show_category_stocks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """แสดงหุ้นใน category ที่เลือก"""
    query = update.callback_query
    await query.answer()
    
    category = query.data.split(":")[1]
    
    try:
        # ดึงหุ้นตาม category
        stocks = supabase.table("stock_master")\
            .select("symbol, name")\
            .eq("category", category)\
            .eq("is_active", True)\
            .execute()
        
        if not stocks.data:
            await query.message.edit_text(f"❌ ไม่พบหุ้นในหมวด {category}")
            return
        
        message = f"📊 **หุ้นในหมวด {category}**\n\n"
        message += "เลือกหุ้นที่ต้องการดูรายละเอียด:\n\n"
        
        # สร้างปุ่ม (4 หุ้นต่อแถว)
        keyboard = []
        for i in range(0, len(stocks.data), 4):
            row = []
            for stock in stocks.data[i:i+4]:
                row.append(InlineKeyboardButton(
                    stock['symbol'], 
                    callback_data=f"stock_detail:{stock['symbol']}"
                ))
            keyboard.append(row)
        
        keyboard.append([
            InlineKeyboardButton("🔙 กลับเลือกหมวด", callback_data="search_stock"),
            InlineKeyboardButton("🏠 เมนูหลัก", callback_data="main_menu")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        await query.message.edit_text(f"❌ เกิดข้อผิดพลาด: {e}")


# === 4. STOCK DETAIL (แสดงรายละเอียดหุ้น) ===
async def stock_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """แสดงรายละเอียดหุ้นแบบเต็ม"""
    query = update.callback_query
    await query.answer()
    
    symbol = query.data.split(":")[1]
    
    try:
        # 1. ดึงข้อมูล snapshot ล่าสุด
        snapshot = supabase.table("stock_snapshots")\
            .select("*")\
            .eq("symbol", symbol)\
            .order("recorded_at", desc=True)\
            .limit(1)\
            .execute()
        
        if not snapshot.data:
            await query.message.edit_text(f"❌ ไม่พบข้อมูลของ {symbol}")
            return
        
        stock = snapshot.data[0]
        
        # 2. ดึง AI Prediction ล่าสุด
        prediction = supabase.table("ai_predictions")\
            .select("*")\
            .eq("symbol", symbol)\
            .order("created_at", desc=True)\
            .limit(1)\
            .execute()
        
        ai_data = prediction.data[0] if prediction.data else None
        
        # 3. ดึงข่าวล่าสุด 3 ข่าว
        news = supabase.table("stock_news")\
            .select("title_th, sentiment_score, published_at, url")\
            .eq("symbol", symbol)\
            .order("published_at", desc=True)\
            .limit(3)\
            .execute()
        
        # สร้างข้อความ
        message = f"📊 **{symbol}** - รายละเอียดหุ้น\n\n"
        
        # ราคาและการเปลี่ยนแปลง
        change_emoji = "🟢" if stock['change_pct'] > 0 else "🔴"
        message += f"💵 **ราคา**: ${stock['price']:.2f}\n"
        message += f"{change_emoji} **เปลี่ยนแปลง**: {stock['change_pct']:+.2f}%\n\n"
        
        # Technical Indicators
        message += "📈 **ตัวชี้วัดทางเทคนิค**\n"
        if stock.get('rsi'):
            rsi_status = "🟢 Oversold" if stock['rsi'] < 30 else "🔴 Overbought" if stock['rsi'] > 70 else "⚪ Neutral"
            message += f"• RSI (14): {stock['rsi']:.1f} {rsi_status}\n"
        
        if stock.get('macd') and stock.get('macd_signal'):
            macd_signal = "🟢 Bullish" if stock['macd'] > stock['macd_signal'] else "🔴 Bearish"
            message += f"• MACD: {macd_signal}\n"
        
        if stock.get('ema_20') and stock.get('ema_50'):
            trend = "🟢 Uptrend" if stock['price'] > stock['ema_20'] > stock['ema_50'] else "🔴 Downtrend"
            message += f"• Trend: {trend}\n"
        
        if stock.get('upside_pct'):
            message += f"• Upside Potential: {stock['upside_pct']:+.1f}%\n"
        
        message += "\n"
        
        # AI Recommendation
        if ai_data:
            rec_emoji = "🟢" if "Buy" in ai_data['recommendation'] else "🔴" if "Sell" in ai_data['recommendation'] else "🟡"
            message += f"🤖 **AI Analysis**\n"
            message += f"• Score: {ai_data['overall_score']}/100\n"
            message += f"• {rec_emoji} Recommendation: **{ai_data['recommendation']}**\n\n"
        
        # Sentiment
        if stock.get('sentiment_score') is not None:
            sentiment = stock['sentiment_score']
            sent_emoji = "😊" if sentiment > 0.3 else "😐" if sentiment >= -0.3 else "😟"
            message += f"{sent_emoji} **Sentiment**: {sentiment:.2f}\n\n"
        
        # ข่าวล่าสุด
        if news.data:
            message += "📰 **ข่าวล่าสุด**\n"
            for idx, article in enumerate(news.data, 1):
                title = article['title_th'][:60] + "..." if len(article['title_th']) > 60 else article['title_th']
                sent = article.get('sentiment_score', 0)
                sent_emoji = "🟢" if sent > 0 else "🔴" if sent < 0 else "⚪"
                message += f"{idx}. {sent_emoji} {title}\n"
        
        # ปุ่มเพิ่มเติม
        keyboard = [
            [
                InlineKeyboardButton("📰 ข่าวทั้งหมด", callback_data=f"stock_news:{symbol}"),
                InlineKeyboardButton("⭐ เพิ่ม Watchlist", callback_data=f"add_watchlist:{symbol}")
            ],
            [
                InlineKeyboardButton("🔔 ตั้งแจ้งเตือน", callback_data=f"alert_menu:{symbol}")
            ],
            [
                InlineKeyboardButton("🔙 กลับค้นหา", callback_data="search_stock"),
                InlineKeyboardButton("🏠 เมนูหลัก", callback_data="main_menu")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        await query.message.edit_text(f"❌ เกิดข้อผิดพลาด: {e}")


# === 5. LATEST NEWS ===
async def latest_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """แสดงข่าวล่าสุดทั้งหมด"""
    query = update.callback_query
    await query.answer()
    
    try:
        news = supabase.table("stock_news")\
            .select("symbol, title_th, sentiment_score, published_at, url")\
            .order("published_at", desc=True)\
            .limit(15)\
            .execute()
        
        if not news.data:
            await query.message.edit_text("📭 ยังไม่มีข่าวในขณะนี้")
            return
        
        message = "📰 **ข่าวหุ้นล่าสุด**\n\n"
        
        for idx, article in enumerate(news.data, 1):
            symbol = article['symbol']
            title = article['title_th'][:70] + "..." if len(article['title_th']) > 70 else article['title_th']
            sent = article.get('sentiment_score', 0)
            sent_emoji = "🟢" if sent > 0 else "🔴" if sent < 0 else "⚪"
            
            message += f"{idx}. **{symbol}** {sent_emoji}\n"
            message += f"   {title}\n\n"
        
        keyboard = [[InlineKeyboardButton("🏠 กลับเมนูหลัก", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        await query.message.edit_text(f"❌ เกิดข้อผิดพลาด: {e}")


# === 6. STOCK NEWS (ข่าวเฉพาะหุ้น) ===
async def stock_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """แสดงข่าวทั้งหมดของหุ้นที่เลือก"""
    query = update.callback_query
    await query.answer()
    
    symbol = query.data.split(":")[1]
    
    try:
        news = supabase.table("stock_news")\
            .select("*")\
            .eq("symbol", symbol)\
            .order("published_at", desc=True)\
            .limit(10)\
            .execute()
        
        if not news.data:
            message = f"📭 ไม่พบข่าวของ {symbol}"
        else:
            message = f"📰 **ข่าว {symbol}** (10 ข่าวล่าสุด)\n\n"
            
            for idx, article in enumerate(news.data, 1):
                title = article['title_th'][:80] + "..." if len(article['title_th']) > 80 else article['title_th']
                sent = article.get('sentiment_score', 0)
                sent_emoji = "🟢" if sent > 0 else "🔴" if sent < 0 else "⚪"
                source = article.get('source', 'Unknown')
                
                message += f"{idx}. {sent_emoji} **[{source}]**\n"
                message += f"   {title}\n"
                if article.get('url'):
                    message += f"   🔗 [อ่านเพิ่มเติม]({article['url']})\n"
                message += "\n"
        
        keyboard = [
            [
                InlineKeyboardButton("🔙 กลับดูหุ้น", callback_data=f"stock_detail:{symbol}"),
                InlineKeyboardButton("🏠 เมนูหลัก", callback_data="main_menu")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='Markdown', disable_web_page_preview=True)
        
    except Exception as e:
        await query.message.edit_text(f"❌ เกิดข้อผิดพลาด: {e}")


# === 7. AI PREDICTIONS ===
async def ai_predictions_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """แสดง AI predictions ทั้งหมด"""
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [
            InlineKeyboardButton("🟢 Strong Buy", callback_data="ai_filter:Strong Buy"),
            InlineKeyboardButton("📈 Buy", callback_data="ai_filter:Buy")
        ],
        [
            InlineKeyboardButton("⚪ Hold", callback_data="ai_filter:Hold"),
            InlineKeyboardButton("📉 Sell", callback_data="ai_filter:Sell")
        ],
        [
            InlineKeyboardButton("🔴 Strong Sell", callback_data="ai_filter:Strong Sell")
        ],
        [
            InlineKeyboardButton("📊 ทั้งหมด", callback_data="ai_filter:All")
        ],
        [
            InlineKeyboardButton("🏠 กลับเมนูหลัก", callback_data="main_menu")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message = (
        "🤖 **AI Stock Predictions**\n\n"
        "เลือกดูคำแนะนำตามระดับ:\n\n"
        "🟢 **Strong Buy** - แนะนำซื้อเข้มแข็ง\n"
        "📈 **Buy** - แนะนำซื้อ\n"
        "⚪ **Hold** - แนะนำถือ\n"
        "📉 **Sell** - แนะนำขาย\n"
        "🔴 **Strong Sell** - แนะนำขายทันที"
    )
    
    await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='Markdown')


async def show_ai_predictions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """แสดง predictions ตามที่กรอง"""
    query = update.callback_query
    await query.answer()
    
    filter_type = query.data.split(":")[1]
    
    try:
        # ดึงข้อมูล
        if filter_type == "All":
            predictions = supabase.table("ai_predictions")\
                .select("*")\
                .order("overall_score", desc=True)\
                .limit(15)\
                .execute()
        else:
            predictions = supabase.table("ai_predictions")\
                .select("*")\
                .eq("recommendation", filter_type)\
                .order("overall_score", desc=True)\
                .limit(15)\
                .execute()
        
        if not predictions.data:
            message = f"📭 ไม่พบหุ้นที่มีคำแนะนำ '{filter_type}'"
        else:
            filter_emoji = {
                "Strong Buy": "🟢",
                "Buy": "📈",
                "Hold": "⚪",
                "Sell": "📉",
                "Strong Sell": "🔴",
                "All": "📊"
            }
            
            message = f"{filter_emoji.get(filter_type, '📊')} **{filter_type} Recommendations**\n\n"
            
            for idx, pred in enumerate(predictions.data, 1):
                symbol = pred['symbol']
                score = pred['overall_score']
                rec = pred['recommendation']
                
                message += f"{idx}. **{symbol}** - {score}/100\n"
                message += f"   {filter_emoji.get(rec, '⚪')} {rec}\n\n"
        
        keyboard = [
            [
                InlineKeyboardButton("🔙 กลับเลือกกรอง", callback_data="ai_predictions"),
                InlineKeyboardButton("🏠 เมนูหลัก", callback_data="main_menu")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        await query.message.edit_text(f"❌ เกิดข้อผิดพลาด: {e}")


# === 8. SETTINGS (ตั้งค่าการแจ้งเตือน) ===
async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """เมนูตั้งค่า"""
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [
            InlineKeyboardButton("🔔 แจ้งเตือนราคา", callback_data="alert_type:price"),
            InlineKeyboardButton("📊 แจ้งเตือน RSI", callback_data="alert_type:rsi")
        ],
        [
            InlineKeyboardButton("🤖 แจ้งเตือน AI Score", callback_data="alert_type:ai_score"),
            InlineKeyboardButton("📰 แจ้งเตือนข่าว", callback_data="alert_type:news")
        ],
        [
            InlineKeyboardButton("⏰ สรุปตลาดรายวัน", callback_data="daily_summary")
        ],
        [
            InlineKeyboardButton("🏠 กลับเมนูหลัก", callback_data="main_menu")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message = (
        "⚙️ **ตั้งค่าการแจ้งเตือน**\n\n"
        "คุณสามารถตั้งค่าการแจ้งเตือนได้หลายประเภท:\n\n"
        "🔔 **แจ้งเตือนราคา** - เมื่อราคาถึงเป้าหมาย\n"
        "📊 **แจ้งเตือน RSI** - เมื่อ RSI oversold/overbought\n"
        "🤖 **แจ้งเตือน AI Score** - เมื่อ AI ให้คะแนนสูง\n"
        "📰 **แจ้งเตือนข่าว** - มีข่าวสำคัญของหุ้นที่ติดตาม\n"
        "⏰ **สรุปตลาดรายวัน** - ส่งสรุปทุกวัน 9:00 น."
    )
    
    await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='Markdown')


# === CALLBACK HANDLERS ===
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """จัดการกดปุ่มทั้งหมด"""
    query = update.callback_query
    
    if query.data == "main_menu":
        await start(update, context)
    elif query.data == "market_overview":
        await market_overview(update, context)
    elif query.data == "top_picks":
        await top_picks(update, context)
    elif query.data == "search_stock":
        await search_stock(update, context)
    elif query.data.startswith("category:"):
        await show_category_stocks(update, context)
    elif query.data.startswith("stock_detail:"):
        await stock_detail(update, context)
    elif query.data == "latest_news":
        await latest_news(update, context)
    elif query.data.startswith("stock_news:"):
        await stock_news(update, context)
    elif query.data == "ai_predictions":
        await ai_predictions_menu(update, context)
    elif query.data.startswith("ai_filter:"):
        await show_ai_predictions(update, context)
    elif query.data == "settings":
        await settings(update, context)
    else:
        await query.answer("🚧 ฟีเจอร์นี้กำลังพัฒนา...")


# === MAIN ===
def main():
    """เริ่มต้น Bot"""
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    print("✅ Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
