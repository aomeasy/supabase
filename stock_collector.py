import os
import asyncio
import yfinance as yf
import pandas as pd
import talib
from supabase import create_client, Client
import requests 
from datetime import datetime, timedelta


# --- Configuration ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TWELVE_DATA_KEY = os.getenv("TWELVE_DATA_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_news_data(symbol):
    """ดึงข่าวล่าสุดจาก yfinance และคำนวณ sentiment"""
    try:
        stock = yf.Ticker(symbol)
        news = stock.news
        
        if not news or len(news) == 0:
            print(f"📭 No news for {symbol}")
            return []
        

        positive_keywords = [
            # Price Movement (ขึ้น/ดี)
            'surge', 'soar', 'jump', 'gain', 'rise', 'rally', 'climb', 'spike', 
            'advance', 'boost', 'pop', 'breakout', 'breakthrough', 'skyrocket',
            
            # Trend & Market (แนวโน้มดี)
            'bull', 'bullish', 'uptrend', 'momentum', 'strength', 'resilient',
            
            # Performance (ผลงานดี)
            'beat', 'exceed', 'outperform', 'top', 'best', 'leading', 'dominance',
            'strong', 'robust', 'solid', 'impressive', 'stellar', 'outstanding',
            
            # Growth & Expansion (เติบโต)
            'growth', 'expand', 'expansion', 'increase', 'accelerate', 'boom',
            'thriving', 'flourish', 'prosper',
            
            # Records & Achievements (สถิติ/ความสำเร็จ)
            'record', 'high', 'peak', 'all-time', 'milestone', 'historic',
            'breakthrough', 'achievement',
            
            # Upgrades & Ratings (อัพเกรด)
            'upgrade', 'upgraded', 'raised', 'lift', 'improve', 'improved',
            'positive', 'optimistic', 'confidence', 'bullish',
            
            # Profits & Revenue (กำไร)
            'profit', 'profitable', 'revenue', 'earnings', 'income', 'dividend',
            
            # Success & Winners (ชนะ/สำเร็จ)
            'win', 'winner', 'winning', 'success', 'successful', 'triumph',
            
            # Sentiment (บวก)
            'optimism', 'hope', 'excited', 'enthusiasm', 'promising', 'favorable',
            'opportunity', 'potential', 'bright', 'positive',
            
            # Guidance & Outlook (สำคัญมากสำหรับหุ้น Growth)
            'raise guidance', 'raised outlook', 'upward revision', 'beat-and-raise', 'favorable outlook',
            
            # Tech & AI Specific (สำหรับ NVDA / Tech)
            'ai demand', 'gpu demand', 'data center growth', 'next-gen', 'backlog', 'production ramp',
            'market share gain', 'technological lead', 'innovation',
            
            # Subscription & User Base (สำหรับ NFLX)
            'subscriber growth', 'low churn', 'content hit', 'ad-tier success', 'average revenue per user',
            
            # Options & Technical Signals
            'short squeeze', 'gamma squeeze', 'consolidation breakout', 'accumulation', 'high volume rally',
            
            # Valuation & GARP
            'undervalued', 'attractive valuation', 'reasonable price', 'strong cash flow', 'buyback', 'share repurchase'
        ]
        
        negative_keywords = [
            # Price Movement (ลง/แย่)
            'fall', 'drop', 'plunge', 'crash', 'tumble', 'sink', 'slide', 'slump',
            'decline', 'decrease', 'dive', 'plummet', 'collapse', 'tank', 'nosedive',
            
            # Trend & Market (แนวโน้มแย่)
            'bear', 'bearish', 'downtrend', 'downturn', 'recession', 'correction',
            
            # Performance (ผลงานแย่)
            'miss', 'missed', 'underperform', 'disappoint', 'disappointing',
            'weak', 'weaken', 'poor', 'worst', 'struggle', 'struggling',
            'fail', 'failure', 'failed', 'underwhelm',
            
            # Loss & Damage (ขาดทุน/เสียหาย)
            'loss', 'losses', 'losing', 'deficit', 'debt', 'bankrupt', 'bankruptcy',
            'insolvent', 'write-down', 'impairment',
            
            # Risk & Concern (ความเสี่ยง/กังวล)
            'concern', 'concerned', 'worry', 'worried', 'fear', 'fearful', 'anxiety',
            'risk', 'risky', 'danger', 'threat', 'threaten', 'warning', 'alert',
            'uncertain', 'uncertainty', 'doubt', 'skeptical', 'cautious',
            
            # Downgrades & Negative Ratings (ลดระดับ)
            'downgrade', 'downgraded', 'cut', 'lower', 'lowered', 'reduce', 'reduced',
            'negative', 'pessimistic',
            
            # Crisis & Problems (วิกฤต/ปัญหา)
            'crisis', 'problem', 'issue', 'trouble', 'challenge', 'difficulty',
            'setback', 'hurdle', 'obstacle',
            
            # Records & Extremes (สถิติแย่)
            'low', 'bottom', 'trough', 'lowest', 'worst', 'record-low',
            
            # Legal & Regulatory (กฎหมาย/ควบคุม)
            'lawsuit', 'sue', 'sued', 'investigation', 'probe', 'fine', 'penalty',
            'violation', 'fraud', 'scandal',
            
            # Layoffs & Cuts (ลดพนักงาน/ตัด)
            'layoff', 'layoffs', 'fire', 'fired', 'cut', 'cuts', 'cutting',
            'eliminate', 'restructure', 'downsize',
            
            # Sentiment (ลบ)
            'pessimism', 'gloomy', 'bleak', 'dire', 'dismal', 'disappointing',

            # Guidance & Outlook (ตัวทำลายราคาหุ้น Tech)
            'lowered guidance', 'guidance cut', 'weak outlook', 'downward revision', 'cautious guidance',
            'shortfall', 'missed estimates',
            
            # Tech & AI Specific
            'supply constraints', 'chip ban', 'export restriction', 'inventory glut', 'component shortage',
            'obsolescence', 'stiff competition',
            
            # Subscription & User Base (สำหรับ NFLX)
            'subscriber loss', 'high churn', 'content fatigue', 'account sharing crackdown impact',
            
            # Macro & Regulatory (กลุ่ม Tech โดนบ่อย)
            'antitrust', 'regulation', 'investigation', 'probe', 'monopoly concerns', 'interest rate hike',
            
            # Options & Technical Signals
            'overbought', 'valuation bubble', 'profit taking', 'distribution', 'dead cat bounce',
            
            # Valuation & Financials
            'overvalued', 'expensive', 'stretched valuation', 'cash burn', 'margin compression'
        ]
       
        
        news_records = []
        
        for article in news[:10]:  # เก็บแค่ 10 ข่าวล่าสุด
            title = article.get('title', '')
            title_lower = title.lower()
            
            # คำนวณ sentiment
            pos_count = sum(1 for word in positive_keywords if word in title_lower)
            neg_count = sum(1 for word in negative_keywords if word in title_lower)
            
            if pos_count > 0 or neg_count > 0:
                sentiment = round((pos_count - neg_count) / max(pos_count + neg_count, 1), 2)
            else:
                sentiment = 0.0
            
            # แปลง timestamp
            pub_time = article.get('providerPublishTime')
            if pub_time:
                published_at = datetime.fromtimestamp(pub_time).isoformat()
            else:
                published_at = datetime.now().isoformat()
            
            news_records.append({
                "symbol": symbol,
                "title": title,
                "summary": article.get('summary', '')[:500],  # จำกัดความยาว
                "url": article.get('link', ''),
                "published_at": published_at,
                "source": article.get('publisher', 'Unknown'),
                "sentiment_score": sentiment
            })
        
        return news_records
        
    except Exception as e:
        print(f"⚠️ Cannot fetch news for {symbol}: {e}")
        return []
    
def fetch_fundamental_data(symbol):
    """ดึงข้อมูล Fundamental สำหรับกลยุทธ์ GARP"""
    try:
        stock = yf.Ticker(symbol)
        info = stock.info
        
        return {
            "pe_ratio": info.get('forwardPE') or info.get('trailingPE'),
            "peg_ratio": info.get('pegRatio'),
            "eps_growth_pct": info.get('earningsGrowth', 0) * 100 if info.get('earningsGrowth') else None,
            "market_cap": info.get('marketCap')
        }
    except Exception as e:
        print(f"⚠️ Cannot fetch fundamental data for {symbol}: {e}")
        return {}

 
def calculate_technical_indicators(df):
    """คำนวณค่าเทคนิคด้วย TA-Lib"""
    try:
        if len(df) < 200:  # ต้องมีข้อมูลอย่างน้อย 200 แท่ง
            return None
        
        # แปลงเป็น numpy arrays
        close = df['Close'].values
        high = df['High'].values
        low = df['Low'].values
        
        # คำนวณด้วย talib
        rsi = talib.RSI(close, timeperiod=14)
        macd, macd_signal, macd_hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        ema_20 = talib.EMA(close, timeperiod=20)
        ema_50 = talib.EMA(close, timeperiod=50)
        ema_200 = talib.EMA(close, timeperiod=200)
        bb_upper, bb_middle, bb_lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
        
        # ดึงค่าล่าสุด
        return {
            "price": float(close[-1]),
            "rsi": float(rsi[-1]) if not pd.isna(rsi[-1]) else None,
            "macd": float(macd[-1]) if not pd.isna(macd[-1]) else None,
            "macd_signal": float(macd_signal[-1]) if not pd.isna(macd_signal[-1]) else None,
            "ema_20": float(ema_20[-1]) if not pd.isna(ema_20[-1]) else None,
            "ema_50": float(ema_50[-1]) if not pd.isna(ema_50[-1]) else None,
            "ema_200": float(ema_200[-1]) if not pd.isna(ema_200[-1]) else None,
            "bb_upper": float(bb_upper[-1]) if not pd.isna(bb_upper[-1]) else None,
            "bb_lower": float(bb_lower[-1]) if not pd.isna(bb_lower[-1]) else None
        }
    except Exception as e:
        print(f"❌ Error calculating indicators: {e}")
        return None


def calculate_upside_pct(current_price, ema_200, ema_50=None):
    """คำนวณ upside potential - ใช้ EMA 200 หรือ EMA 50 แทนถ้าไม่มี"""
    if not current_price:
        return None
    
    # ลองใช้ EMA 200 ก่อน
    if ema_200 and ema_200 > 0:
        return round(((ema_200 - current_price) / current_price) * 100, 2)
    
    # ถ้าไม่มี EMA 200 ให้ใช้ EMA 50 แทน
    if ema_50 and ema_50 > 0:
        return round(((ema_50 - current_price) / current_price) * 100, 2)
    
    return None


def fetch_analyst_data(symbol):
    """ดึงข้อมูล Analyst Recommendations จาก yfinance"""
    try:
        stock = yf.Ticker(symbol)
        recommendations = stock.recommendations
        
        if recommendations is not None and not recommendations.empty:
            recent = recommendations.tail(10)
            buy_grades = ['buy', 'strong buy', 'outperform', 'overweight']
            buy_count = 0
            
            for _, row in recent.iterrows():
                grade = str(row.get('To Grade', '')).lower()
                if any(buy_word in grade for buy_word in buy_grades):
                    buy_count += 1
            
            total = len(recent)
            return round((buy_count / total) * 100, 2) if total > 0 else None
            
    except Exception as e:
        print(f"⚠️ Cannot fetch analyst data for {symbol}: {e}")
    
    return None


def fetch_sentiment_score(symbol):
    """คำนวณ Sentiment Score จากข่าวของ yfinance"""
    try:
        stock = yf.Ticker(symbol)
        news = stock.news
        
        if not news or len(news) == 0:
            return None
        
        positive_keywords = [
            'surge', 'soar', 'jump', 'gain', 'rise', 'rally', 'bull', 
            'upgrade', 'beat', 'strong', 'growth', 'record', 'high'
        ]
        negative_keywords = [
            'fall', 'drop', 'plunge', 'crash', 'bear', 'downgrade', 
            'miss', 'weak', 'loss', 'decline', 'low', 'concern'
        ]
        
        score = 0
        analyzed_count = 0
        
        for article in news[:20]:
            title = article.get('title', '').lower()
            pos_count = sum(1 for word in positive_keywords if word in title)
            neg_count = sum(1 for word in negative_keywords if word in title)
            
            if pos_count > 0 or neg_count > 0:
                score += pos_count - neg_count
                analyzed_count += 1
        
        if analyzed_count == 0:
            return None
        
        normalized_score = score / analyzed_count
        return round(max(-1, min(1, normalized_score)), 2)
        
    except Exception as e:
        print(f"⚠️ Cannot fetch sentiment for {symbol}: {e}")
    
    return None


async def fetch_data_waterfall(symbol):
    """กลยุทธ์ดึงข้อมูลแบบน้ำตก: yfinance -> Twelve Data"""
    print(f"🔍 Fetching data for {symbol}...")
    
    # --- Source 1: yfinance (Primary) ---
    try:
        stock = yf.Ticker(symbol)
        df = stock.history(period="2y")
        
        if not df.empty and len(df) >= 2:
            tech_data = calculate_technical_indicators(df)
            
            # ถ้าคำนวณไม่ได้ (ETF หรือข้อมูลน้อย) ใช้ข้อมูลพื้นฐาน
            if not tech_data:
                prev_close = df['Close'].iloc[-2]
                current_price = float(df['Close'].iloc[-1])
                change_pct = ((current_price - prev_close) / prev_close) * 100
                
                print(f"⚠️ Using basic data only for {symbol}")
                return {
                    "price": current_price,
                    "change_pct": round(change_pct, 2),
                    "source": "yfinance_basic",
                    "rsi": None,
                    "macd": None,
                    "macd_signal": None,
                    "ema_20": None,
                    "ema_50": None,
                    "ema_200": None,
                    "bb_upper": None,
                    "bb_lower": None
                }
            
            prev_close = df['Close'].iloc[-2]
            current_price = tech_data['price']
            change_pct = ((current_price - prev_close) / prev_close) * 100
            
            tech_data['change_pct'] = round(change_pct, 2)
            tech_data['source'] = 'yfinance'
            return tech_data
        else:
            print(f"⚠️ Insufficient data from yfinance for {symbol}")
            
    except Exception as e:
        print(f"⚠️ yfinance failed for {symbol}: {e}")

    # --- Source 2: Twelve Data (Fallback) ---
    if TWELVE_DATA_KEY:
        try:
            print(f"🔄 Falling back to Twelve Data for {symbol}...")
            url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TWELVE_DATA_KEY}"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            
            data = resp.json()
            
            if "close" in data and "percent_change" in data:
                return {
                    "price": float(data['close']),
                    "change_pct": float(data['percent_change']),
                    "source": "twelvedata",
                    "rsi": None,
                    "macd": None,
                    "macd_signal": None,
                    "ema_20": None,
                    "ema_50": None,
                    "ema_200": None,
                    "bb_upper": None,
                    "bb_lower": None
                }
            else:
                print(f"⚠️ Invalid response from Twelve Data: {data}")
                
        except Exception as e:
            print(f"❌ Twelve Data fallback failed for {symbol}: {e}")

    print(f"❌ All sources failed for {symbol}")
    return None

async def main():
    global supabase
    
    # ดึงทั้ง symbol และ category
    res = supabase.table("stock_master").select("symbol, category").eq("is_active", True).execute()
    stocks = res.data
    
    if not stocks:
        print("📭 No active symbols found in stock_master.")
        return

    print(f"\n🚀 Starting technical analysis for {len(stocks)} symbols\n")
    
    for idx, stock_data in enumerate(stocks, 1):
        symbol = stock_data['symbol']
        category = stock_data.get('category', 'Core')
        
        print(f"\n{'='*60}")
        print(f"[{idx}/{len(stocks)}] Processing: {symbol} ({category})")
        print(f"{'='*60}")
        
        data = await fetch_data_waterfall(symbol)
        
        if not data:
            print(f"❌ Failed: {symbol}")
            await asyncio.sleep(5)
            continue
        
        if not data.get("ema_200"):
            print(f"⚠️ {symbol}: No EMA 200 data")
        
        print(f"📊 Calculating metrics for {symbol}...")
        
        upside_pct = calculate_upside_pct(
            data.get("price"), 
            data.get("ema_200"),
            data.get("ema_50")
        )
        
        # ข้าม analyst/sentiment สำหรับ ETF
        analyst_pct = None if category == 'ETF' else fetch_analyst_data(symbol)
        sentiment = None if category == 'ETF' else fetch_sentiment_score(symbol)
        
        # สร้าง snapshot payload
        snapshot_payload = {
            "symbol": symbol,
            "price": data.get("price"),
            "change_pct": data.get("change_pct"),
            "rsi": data.get("rsi"),
            "macd": data.get("macd"),
            "macd_signal": data.get("macd_signal"),
            "ema_20": data.get("ema_20"),
            "ema_50": data.get("ema_50"),
            "ema_200": data.get("ema_200"),
            "bb_upper": data.get("bb_upper"),
            "bb_lower": data.get("bb_lower"),
            "upside_pct": upside_pct,
            "analyst_buy_pct": analyst_pct,
            "sentiment_score": sentiment,
            "recorded_at": datetime.now().isoformat()
        }
        
        # บันทึก snapshot
        max_db_retries = 3
        for db_attempt in range(max_db_retries):
            try:
                supabase.table("stock_snapshots").insert(snapshot_payload).execute()
                print(f"✅ Snapshot saved: {symbol}")
                print(f"   Price: ${data.get('price'):.2f} | Change: {data.get('change_pct'):.2f}%")
                if data.get('rsi'):
                    print(f"   RSI: {data.get('rsi'):.2f} | Upside: {upside_pct}%")
                break
            except Exception as db_error:
                print(f"⚠️ Database error (attempt {db_attempt + 1}/{max_db_retries}): {db_error}")
                if db_attempt < max_db_retries - 1:
                    await asyncio.sleep(2)
                    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                else:
                    print(f"❌ Failed to save snapshot for {symbol}")
                    break
        
        # === เพิ่มส่วนดึงและบันทึกข่าว ===
        if category != 'ETF':
            print(f"📰 Fetching news for {symbol}...")
            news_records = fetch_news_data(symbol)
            
            if news_records:
                try:
                    # บันทึกข่าวทีละรายการเพื่อข้าม duplicate
                    saved_count = 0
                    for news in news_records:
                        try:
                            supabase.table("stock_news").insert(news).execute()
                            saved_count += 1
                        except Exception as dup_error:
                            # ข้าม error ถ้าข่าวซ้ำ (unique constraint)
                            if "duplicate" not in str(dup_error).lower():
                                print(f"⚠️ News error: {dup_error}")
                    
                    print(f"✅ Saved {saved_count}/{len(news_records)} news for {symbol}")
                    
                except Exception as news_error:
                    print(f"⚠️ Failed to save news for {symbol}: {news_error}")
            else:
                print(f"📭 No news saved for {symbol}")
        # === จบส่วนที่เพิ่ม ===
        
        await asyncio.sleep(3)
    
    print(f"\n{'='*60}")
    print("✅ Technical data collection completed!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
