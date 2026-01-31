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
FINNHUB_KEY = os.getenv("FINNHUB_KEY") 

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def calculate_overall_score(symbol, tech_data, fundamental_data, news_sentiment):
    """
    คำนวณ Overall Score จากข้อมูลต่างๆ โดยไม่ใช้ AI API
    Score: 0-100 (integer)
    """
    
    score = 50  # เริ่มที่ 50 (กลางๆ)
    
    # === 1. Technical Score (40 คะแนน) ===
    technical_score = 0
    
    # RSI (10 คะแนน)
    rsi = tech_data.get('rsi')
    if rsi:
        if 30 <= rsi <= 70:
            technical_score += 10
        elif 20 <= rsi < 30 or 70 < rsi <= 80:
            technical_score += 5
    
    # MACD (10 คะแนน)
    macd = tech_data.get('macd')
    macd_signal = tech_data.get('macd_signal')
    if macd and macd_signal:
        if macd > macd_signal:
            technical_score += 10
        elif macd > macd_signal * 0.9:
            technical_score += 5
    
    # EMA Trend (10 คะแนน)
    price = tech_data.get('price')
    ema_20 = tech_data.get('ema_20')
    ema_50 = tech_data.get('ema_50')
    ema_200 = tech_data.get('ema_200')
    
    if price and ema_20 and ema_50:
        if price > ema_20 > ema_50:
            technical_score += 10
        elif price > ema_20:
            technical_score += 5
    
    # Upside Potential (10 คะแนน)
    upside_pct = tech_data.get('upside_pct')
    if upside_pct:
        if upside_pct > 20:
            technical_score += 10
        elif upside_pct > 10:
            technical_score += 7
        elif upside_pct > 5:
            technical_score += 4
    
    score += (technical_score / 40) * 40
    
    
    # === 2. Fundamental Score (30 คะแนน) ===
    fundamental_score = 0
    
    if fundamental_data:
        pe_ratio = fundamental_data.get('pe_ratio')
        if pe_ratio:
            if 10 <= pe_ratio <= 25:
                fundamental_score += 10
            elif 5 <= pe_ratio < 10 or 25 < pe_ratio <= 35:
                fundamental_score += 5
        
        peg_ratio = fundamental_data.get('peg_ratio')
        if peg_ratio:
            if peg_ratio < 1:
                fundamental_score += 10
            elif 1 <= peg_ratio <= 1.5:
                fundamental_score += 7
            elif 1.5 < peg_ratio <= 2:
                fundamental_score += 4
        
        eps_growth = fundamental_data.get('eps_growth_pct')
        if eps_growth:
            if eps_growth > 20:
                fundamental_score += 10
            elif eps_growth > 10:
                fundamental_score += 7
            elif eps_growth > 5:
                fundamental_score += 4
    
    score += (fundamental_score / 30) * 30
    
    
    # === 3. Sentiment Score (30 คะแนน) ===
    sentiment_score = 0
    
    if news_sentiment:
        if news_sentiment > 0.5:
            sentiment_score += 15
        elif news_sentiment > 0.2:
            sentiment_score += 10
        elif news_sentiment >= -0.2:
            sentiment_score += 5
    
    analyst_buy_pct = tech_data.get('analyst_buy_pct')
    if analyst_buy_pct:
        if analyst_buy_pct >= 70:
            sentiment_score += 15
        elif analyst_buy_pct >= 50:
            sentiment_score += 10
        elif analyst_buy_pct >= 30:
            sentiment_score += 5
    
    score += (sentiment_score / 30) * 30
    
    # ⬇️ แก้ไขตรงนี้: แปลงเป็น int
    return int(min(100, max(0, round(score))))  # ✅ คืนค่าเป็น integer


def generate_recommendation(overall_score, price, upside_pct):
    """สร้างคำแนะนำจาก Score"""
    
    if overall_score >= 75:
        recommendation = "Strong Buy"
        reason = "Excellent technical and fundamental indicators"
    elif overall_score >= 60:
        recommendation = "Buy"
        reason = "Good growth potential with positive momentum"
    elif overall_score >= 45:
        recommendation = "Hold"
        reason = "Wait for better entry point or confirmation"
    elif overall_score >= 30:
        recommendation = "Sell"
        reason = "Weak signals, consider taking profits"
    else:
        recommendation = "Strong Sell"
        reason = "Poor performance across all metrics"
    
    # เพิ่ม price target
    if upside_pct and upside_pct > 0:
        price_target = round(price * (1 + upside_pct / 100), 2)
    else:
        price_target = None
    
    return recommendation, reason, price_target


def calculate_actual_outcome(symbol, prediction_date):
    """
    คำนวณผลลัพธ์จริงหลังจาก 30 วัน (สำหรับการเรียนรู้ในอนาคต)
    """
    try:
        # ดึงราคาล่าสุด
        current_snapshot = supabase.table("stock_snapshots")\
            .select("price")\
            .eq("symbol", symbol)\
            .order("recorded_at", desc=True)\
            .limit(1)\
            .execute()
        
        if not current_snapshot.data:
            return None
        
        current_price = current_snapshot.data[0]['price']
        
        # ดึงราคาตอนทำนาย
        prediction_snapshot = supabase.table("stock_snapshots")\
            .select("price")\
            .eq("symbol", symbol)\
            .lte("recorded_at", prediction_date)\
            .order("recorded_at", desc=True)\
            .limit(1)\
            .execute()
        
        if not prediction_snapshot.data:
            return None
        
        prediction_price = prediction_snapshot.data[0]['price']
        
        # คำนวณ % เปลี่ยนแปลง
        return round(((current_price - prediction_price) / prediction_price) * 100, 2)
        
    except Exception as e:
        print(f"⚠️ Error calculating actual outcome: {e}")
        return None


def fetch_news_data(symbol):
    """ดึงข่าวล่าสุดจาก Finnhub API และคำนวณ sentiment + แปลภาษาไทย"""
    try:
        if not FINNHUB_KEY or FINNHUB_KEY == "":
            print(f"⚠️ FINNHUB_KEY not configured, skipping news for {symbol}")
            return []
        
        # 1. กำหนดช่วงเวลา: 7 วันล่าสุด
        to_date = datetime.now()
        from_date = to_date - timedelta(days=7)
        
        # 2. เตรียม URL และ Parameters
        url = "https://finnhub.io/api/v1/company-news"
        params = {
            "symbol": symbol,
            "from": from_date.strftime('%Y-%m-%d'),
            "to": to_date.strftime('%Y-%m-%d'),
            "token": FINNHUB_KEY
        }
        
        # 3. ยิง Request ไปที่ API
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # 4. ตรวจสอบข้อมูล
        if not data or not isinstance(data, list):
            print(f"📭 No news available for {symbol}")
            return []
        
        print(f"📰 Found {len(data)} news articles for {symbol}")
        
        # 5. เอาแค่ 10 ข่าวล่าสุด
        news_list = data[:10]
        
        # 6. แปลภาษาไทย
        try:
            translator = GoogleTranslator(source='en', target='th')
            for news in news_list:
                headline = news.get('headline', '')
                summary = news.get('summary', '')
                
                if headline:
                    news['headline_th'] = translator.translate(headline)
                
                if summary:
                    news['summary_th'] = translator.translate(summary[:4500])
        except Exception as trans_error:
            print(f"⚠️ Translation failed for {symbol}: {trans_error}")
            # ถ้าแปลไม่ได้ ใช้ภาษาอังกฤษเดิม
 
 

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
 
        # 8. สร้าง news_records พร้อม sentiment
        news_records = []
        
        for idx, news in enumerate(news_list, 1):
            headline = news.get('headline', '')
            
            if not headline:
                print(f"⚠️ News #{idx}: No headline found, skipping...")
                continue
            
            headline_lower = headline.lower()
            
            # คำนวณ sentiment
            pos_count = sum(1 for word in positive_keywords if word in headline_lower)
            neg_count = sum(1 for word in negative_keywords if word in headline_lower)
            
            if pos_count > 0 or neg_count > 0:
                sentiment = round((pos_count - neg_count) / max(pos_count + neg_count, 1), 2)
            else:
                sentiment = 0.0
            
            # แปลง timestamp (Finnhub ใช้ Unix timestamp)
            pub_timestamp = news.get('datetime')
            if pub_timestamp:
                published_at = datetime.fromtimestamp(pub_timestamp).isoformat()
            else:
                published_at = datetime.now().isoformat()
            
            news_record = {
                "symbol": symbol,
                "title": headline[:500],
                "title_th": news.get('headline_th', '')[:500] if news.get('headline_th') else None,
                "summary": news.get('summary', '')[:500] if news.get('summary') else None,
                "summary_th": news.get('summary_th', '')[:500] if news.get('summary_th') else None,
                "url": news.get('url', ''),
                "published_at": published_at,
                "source": news.get('source', 'Unknown'),
                "sentiment_score": sentiment
            }
            
            news_records.append(news_record)
            
            # Debug: แสดงข้อมูลข่าวแรก
            if idx == 1:
                print(f"   Sample: {headline[:50]}...")
                print(f"   Thai: {news.get('headline_th', '')[:50]}...")
                print(f"   Sentiment: {sentiment} | Source: {news.get('source')}")
        
        return news_records
        
    except Exception as e:
        print(f"⚠️ Cannot fetch news for {symbol}: {e}")
        import traceback
        traceback.print_exc()
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
            
            print(f"📊 Retrieved {len(news_records)} valid news articles")  # เพิ่มบรรทัดนี้
            
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
                print(f"📭 No valid news found for {symbol}")
             
        # === จบส่วนที่เพิ่ม ===


                # === เพิ่มส่วนนี้หลังบันทึกข่าวเสร็จ ===
        
        # คำนวณ AI Prediction
        print(f"🤖 Calculating AI prediction for {symbol}...")
        
        # ดึง fundamental data (ข้ามถ้าเป็น ETF)
        fundamental_data = None if category == 'ETF' else fetch_fundamental_data(symbol)
        
        # คำนวณ overall score
        overall_score = calculate_overall_score(
            symbol=symbol,
            tech_data={
                'price': data.get('price'),
                'rsi': data.get('rsi'),
                'macd': data.get('macd'),
                'macd_signal': data.get('macd_signal'),
                'ema_20': data.get('ema_20'),
                'ema_50': data.get('ema_50'),
                'ema_200': data.get('ema_200'),
                'upside_pct': upside_pct,
                'analyst_buy_pct': analyst_pct
            },
            fundamental_data=fundamental_data,
            news_sentiment=sentiment
        )
        
        # สร้างคำแนะนำ
        recommendation, reason, price_target = generate_recommendation(
            overall_score=overall_score,
            price=data.get('price'),
            upside_pct=upside_pct
        )
        
        # บันทึกใน ai_predictions
        prediction_payload = {
            "symbol": symbol,
            "ai_model": "rule_based_v1",  # ระบุว่าใช้ rule-based
            "overall_score": overall_score,
            "recommendation": recommendation,
            "price_at_prediction": data.get('price'),
            "actual_outcome": None  # จะอัพเดททีหลังหลัง 30 วัน
        }
        
        try:
            supabase.table("ai_predictions").insert(prediction_payload).execute()
            print(f"✅ AI Prediction saved: {symbol}")
            print(f"   Score: {overall_score}/100 | {recommendation}")
            print(f"   Reason: {reason}")
            if price_target:
                print(f"   Target: ${price_target:.2f}")
        except Exception as pred_error:
            print(f"⚠️ Failed to save prediction for {symbol}: {pred_error}")
        
        # === จบส่วนที่เพิ่ม ===
        
        await asyncio.sleep(3)
    
    print(f"\n{'='*60}")
    print("✅ Technical data collection completed!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
