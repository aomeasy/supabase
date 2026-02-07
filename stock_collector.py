import os
import asyncio
import yfinance as yf
import pandas as pd
import talib
from supabase import create_client, Client
import requests 
from datetime import datetime, timedelta
from deep_translator import GoogleTranslator 


# --- Configuration ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TWELVE_DATA_KEY = os.getenv("TWELVE_DATA_KEY")
FINNHUB_KEY = os.getenv("FINNHUB_KEY") 

TELEGRAM_BOT_TOKEN = "8473805508:AAE7FqIeUl_H0vdMuIzfHMld_rIfBSUPpbw"
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # ต้องตั้งค่า Chat ID ใน environment

# Debug
if FINNHUB_KEY:
    print(f"✅ FINNHUB_KEY loaded: {FINNHUB_KEY[:10]}...{FINNHUB_KEY[-4:]}")
else:
    print("❌ FINNHUB_KEY not found")
    
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
 

def calculate_technical_score(tech_data):
    """แยกการคำนวณ Technical Score ออกมา"""
    score = 0
    
    # RSI (10 คะแนน)
    rsi = tech_data.get('rsi')
    if rsi:
        if 30 <= rsi <= 70:
            score += 10
        elif 20 <= rsi < 30 or 70 < rsi <= 80:
            score += 5
    
    # MACD (10 คะแนน)
    macd = tech_data.get('macd')
    macd_signal = tech_data.get('macd_signal')
    if macd and macd_signal:
        if macd > macd_signal:
            score += 10
        elif macd > macd_signal * 0.9:
            score += 5
    
    # EMA Trend (10 คะแนน)
    price = tech_data.get('price')
    ema_20 = tech_data.get('ema_20')
    ema_50 = tech_data.get('ema_50')
    
    if price and ema_20 and ema_50:
        if price > ema_20 > ema_50:
            score += 10
        elif price > ema_20:
            score += 5
    
    # Upside Potential (10 คะแนน)
    upside_pct = tech_data.get('upside_pct')
    if upside_pct:
        if upside_pct > 20:
            score += 10
        elif upside_pct > 10:
            score += 7
        elif upside_pct > 5:
            score += 4
    
    return score  # 0-40


def calculate_fundamental_score(fundamental_data):
    """แยกการคำนวณ Fundamental Score ออกมา"""
    if not fundamental_data:
        return 0
    
    score = 0
    
    # P/E Ratio (10 คะแนน)
    pe_ratio = fundamental_data.get('pe_ratio')
    if pe_ratio:
        if 10 <= pe_ratio <= 25:
            score += 10
        elif 5 <= pe_ratio < 10 or 25 < pe_ratio <= 35:
            score += 5
    
    # PEG Ratio (10 คะแนน)
    peg_ratio = fundamental_data.get('peg_ratio')
    if peg_ratio:
        if peg_ratio < 1:
            score += 10
        elif 1 <= peg_ratio <= 1.5:
            score += 7
        elif 1.5 < peg_ratio <= 2:
            score += 4
    
    # EPS Growth (10 คะแนน)
    eps_growth = fundamental_data.get('eps_growth_pct')
    if eps_growth:
        if eps_growth > 20:
            score += 10
        elif eps_growth > 10:
            score += 7
        elif eps_growth > 5:
            score += 4
    
    return score  # 0-30


def calculate_sentiment_score(news_sentiment, tech_data):
    """แยกการคำนวณ Sentiment Score ออกมา"""
    score = 0
    
    # News Sentiment (15 คะแนน)
    if news_sentiment:
        if news_sentiment > 0.5:
            score += 15
        elif news_sentiment > 0.2:
            score += 10
        elif news_sentiment >= -0.2:
            score += 5
    
    # Analyst Buy % (15 คะแนน)
    analyst_buy_pct = tech_data.get('analyst_buy_pct')
    if analyst_buy_pct:
        if analyst_buy_pct >= 70:
            score += 15
        elif analyst_buy_pct >= 50:
            score += 10
        elif analyst_buy_pct >= 30:
            score += 5
    
    return score  # 0-30


def calculate_risk_score(tech_data, fundamental_data, market_cap):
    """
    คำนวณความเสี่ยง (0-100, ยิ่งสูงยิ่งเสี่ยง)
    
    ใช้ปรับ Overall Score ลง ถ้าเสี่ยงสูง
    """
    
    risk_score = 0
    
    # 1. Volatility Risk (จาก RSI)
    rsi = tech_data.get('rsi')
    if rsi:
        if rsi > 80 or rsi < 20:  # Overbought/Oversold
            risk_score += 30
        elif rsi > 70 or rsi < 30:
            risk_score += 15
    
    # 2. Price vs Bollinger Bands
    price = tech_data.get('price')
    bb_upper = tech_data.get('bb_upper')
    bb_lower = tech_data.get('bb_lower')
    
    if price and bb_upper and bb_lower:
        if price > bb_upper:  # ราคาสูงเกินไป
            risk_score += 20
        elif price < bb_lower:  # ราคาต่ำเกินไป
            risk_score += 15
    
    # 3. Market Cap Risk
    if market_cap:
        if market_cap < 1_000_000_000:  # < $1B
            risk_score += 25  # Small cap เสี่ยงมาก
        elif market_cap < 10_000_000_000:  # < $10B
            risk_score += 10
    
    # 4. Fundamental Risk
    if fundamental_data:
        pe_ratio = fundamental_data.get('pe_ratio')
        if pe_ratio and pe_ratio > 50:  # Overvalued
            risk_score += 20
        
        peg_ratio = fundamental_data.get('peg_ratio')
        if peg_ratio and peg_ratio > 2:  # Expensive growth
            risk_score += 15
    
    return min(100, risk_score)


def adjust_score_by_risk(overall_score, risk_score):
    """
    ปรับ Score ตามความเสี่ยง
    """
    
    if risk_score >= 70:
        # เสี่ยงสูงมาก → ลดคะแนน 30%
        return int(overall_score * 0.7)
    elif risk_score >= 50:
        # เสี่ยงปานกลาง → ลดคะแนน 15%
        return int(overall_score * 0.85)
    elif risk_score >= 30:
        # เสี่ยงเล็กน้อย → ลดคะแนน 5%
        return int(overall_score * 0.95)
    else:
        # เสี่ยงต่ำ → ไม่ลด
        return overall_score


# ตัวอย่างการใช้ใน calculate_overall_score
def calculate_overall_score_with_risk(symbol, tech_data, fundamental_data, news_sentiment, category='Core', market_cap=None):
    
    # คำนวณ Score ปกติ
    base_score = calculate_overall_score(symbol, tech_data, fundamental_data, news_sentiment, category, market_cap)
    
    # คำนวณความเสี่ยง
    risk_score = calculate_risk_score(tech_data, fundamental_data, market_cap)
    
    # ปรับ Score
    final_score = adjust_score_by_risk(base_score, risk_score)
    
    print(f"   Base Score: {base_score} | Risk: {risk_score} | Final: {final_score}")
    
    return final_score 

def generate_recommendation_advanced(overall_score, price, upside_pct, risk_score, category):
    """
    สร้างคำแนะนำแบบละเอียด พิจารณาทั้ง Score + Risk + Upside
    """
    
    # 1. คำนวณ Confidence Level
    if risk_score < 20:
        confidence = "High"
    elif risk_score < 50:
        confidence = "Medium"
    else:
        confidence = "Low"
    
    # 2. กำหนด Recommendation
    if overall_score >= 75 and risk_score < 50:
        recommendation = "Strong Buy"
        reason = f"Excellent signals with {confidence.lower()} risk"
        
    elif overall_score >= 60:
        if risk_score >= 60:
            recommendation = "Hold"  # คะแนนดีแต่เสี่ยงสูง
            reason = f"Good score but high risk ({risk_score}/100)"
        else:
            recommendation = "Buy"
            reason = f"Positive momentum with {confidence.lower()} risk"
    
    elif overall_score >= 45:
        recommendation = "Hold"
        reason = "Wait for clearer signals"
    
    elif overall_score >= 30:
        recommendation = "Sell"
        reason = f"Weak performance, consider reducing position"
    
    else:
        recommendation = "Strong Sell"
        reason = "Poor metrics across the board"
    
    # 3. คำนวณ Price Target
    if upside_pct and upside_pct > 0:
        # ปรับ upside ตามความเสี่ยง
        adjusted_upside = upside_pct * (1 - risk_score / 200)
        price_target = round(price * (1 + adjusted_upside / 100), 2)
    else:
        price_target = None
    
    # 4. เพิ่ม Time Horizon (ระยะเวลาที่แนะนำ)
    if category in ['Growth', 'Momentum']:
        time_horizon = "3-6 months"
    elif category in ['Value', 'Dividend']:
        time_horizon = "6-12 months"
    else:
        time_horizon = "6 months"
    
    return {
        'recommendation': recommendation,
        'reason': reason,
        'confidence': confidence,
        'price_target': price_target,
        'time_horizon': time_horizon,
        'risk_level': 'High' if risk_score >= 60 else 'Medium' if risk_score >= 30 else 'Low'
    }

def get_scoring_weights(symbol, category, market_cap):
    """
    กำหนดน้ำหนักคะแนนตามประเภทหุ้น
    
    Returns: (technical_weight, fundamental_weight, sentiment_weight)
    """
    
    # 1. ถ้าเป็น ETF → ดู Technical อย่างเดียว
    if category == 'ETF':
        return (1.0, 0.0, 0.0)
    
    # 2. แยกตาม Market Cap
    if market_cap:
        # Large Cap (> $200B) → เชื่อถือ Fundamental + Sentiment
        if market_cap > 200_000_000_000:
            return (0.25, 0.40, 0.35)  # เช่น AAPL, MSFT
        
        # Mid Cap ($10B - $200B) → ดูทุกอย่างพอๆ กัน
        elif market_cap > 10_000_000_000:
            return (0.35, 0.35, 0.30)
        
        # Small Cap (< $10B) → เน้น Technical (ผันผวนสูง)
        else:
            return (0.50, 0.30, 0.20)
    
    # 3. แยกตาม Category (ถ้าไม่มี market_cap)
    category_weights = {
        'Growth': (0.30, 0.30, 0.40),      # เช่น NVDA, TSLA
        'Value': (0.20, 0.60, 0.20),       # เช่น BRK.B, JNJ
        'Dividend': (0.25, 0.50, 0.25),    # เช่น T, VZ
        'Momentum': (0.60, 0.20, 0.20),    # เช่น GME, AMC
        'Core': (0.35, 0.35, 0.30)         # Default
    }
    
    return category_weights.get(category, (0.35, 0.35, 0.30))


def calculate_overall_score(symbol, tech_data, fundamental_data, news_sentiment, category='Core', market_cap=None):
    """
    คำนวณ Overall Score แบบ Dynamic Weighting
    """
    
    # คำนวณคะแนนแต่ละส่วน (เหมือนเดิม)
    technical_score = calculate_technical_score(tech_data)      # 0-40
    fundamental_score = calculate_fundamental_score(fundamental_data)  # 0-30
    sentiment_score = calculate_sentiment_score(news_sentiment, tech_data)  # 0-30
    
    # 🔥 ใช้น้ำหนักแบบ Dynamic
    tech_w, fund_w, sent_w = get_scoring_weights(symbol, category, market_cap)
    
    final_score = (
        (technical_score / 40) * 100 * tech_w +
        (fundamental_score / 30) * 100 * fund_w +
        (sentiment_score / 30) * 100 * sent_w
    )
    
    return int(min(100, max(0, round(final_score))))
 

def calculate_news_sentiment_advanced(headline, summary=''):
    """
    Sentiment Analysis แบบละเอียดขึ้น
    
    Returns: sentiment_score (-1 to 1)
    """
    
    text = f"{headline} {summary}".lower()
    
    # 1. ตรวจสอบ Negation (ปฏิเสธ)
    negation_words = ['not', 'no', 'never', 'neither', 'nobody', 'nothing', 
                      'fails to', 'unable to', 'without']
    
    # 2. คำที่มีน้ำหนักมาก (Strong Signal)
    strong_positive = {
        'surge': 2, 'soar': 2, 'skyrocket': 2, 'breakout': 2,
        'beat': 1.5, 'exceed': 1.5, 'strong': 1.5, 'rally': 1.5
    }
    
    strong_negative = {
        'plunge': -2, 'crash': -2, 'collapse': -2, 'tank': -2,
        'miss': -1.5, 'disappoint': -1.5, 'weak': -1.5, 'slump': -1.5
    }
    
    # 3. คำทั่วไป (Moderate Signal)
    moderate_positive = {
        'gain': 1, 'rise': 1, 'growth': 1, 'increase': 1,
        'upgrade': 1, 'positive': 1, 'bullish': 1
    }
    
    moderate_negative = {
        'fall': -1, 'drop': -1, 'decline': -1, 'concern': -1,
        'downgrade': -1, 'negative': -1, 'bearish': -1
    }
    
    # 4. คำนวณ Sentiment
    sentiment = 0
    words = text.split()
    
    for i, word in enumerate(words):
        # เช็ค Negation (3 คำก่อนหน้า)
        is_negated = False
        if i > 0:
            prev_words = ' '.join(words[max(0, i-3):i])
            if any(neg in prev_words for neg in negation_words):
                is_negated = True
        
        # คำนวณคะแนน
        score = 0
        if word in strong_positive:
            score = strong_positive[word]
        elif word in strong_negative:
            score = strong_negative[word]
        elif word in moderate_positive:
            score = moderate_positive[word]
        elif word in moderate_negative:
            score = moderate_negative[word]
        
        # ถ้ามี Negation → กลับเครื่องหมาย
        if is_negated and score != 0:
            score = -score * 0.8  # ลดน้ำหนักเล็กน้อย
        
        sentiment += score
    
    # 5. Normalize (-1 to 1)
    max_possible = len(words) * 2  # สมมติทุกคำเป็น strong signal
    normalized = sentiment / max(max_possible, 1)
    
    return round(max(-1, min(1, normalized)), 2)

async def send_telegram_message(message):
    """ส่งข้อความไปที่ Telegram"""
    if not TELEGRAM_CHAT_ID:
        print("⚠️ TELEGRAM_CHAT_ID not configured, skipping notification")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        
        print("✅ Telegram notification sent")
        return True
        
    except Exception as e:
        print(f"⚠️ Failed to send Telegram: {e}")
        return False

async def send_normal_dca(top_stocks):
    """ส่งคำแนะนำ DCA แบบปกติ (มีหุ้น Score สูง)"""
    
    message = f"🎯 <b>DCA Today</b>: {datetime.now().strftime('%d %b %Y')}\n\n"
    
    # Top Pick
    if top_stocks:
        top = top_stocks[0]
        risk_level = "ต่ำ" if top.get('risk_score', 50) < 30 else "ปานกลาง" if top.get('risk_score', 50) < 60 else "สูง"
        
        message += f"💰 <b>TOP PICK: {top['symbol']}</b>\n"
        message += f"   ราคา: ${top['price_at_prediction']:.2f} | Score: {top['overall_score']}/100\n"
        message += f"   Risk: {risk_level} | Confidence: {top.get('confidence', 'N/A')}\n\n"
    
    # Runner-up
    if len(top_stocks) > 1:
        runner = top_stocks[1]
        risk_level = "ต่ำ" if runner.get('risk_score', 50) < 30 else "ปานกลาง"
        
        message += f"💎 <b>RUNNER-UP: {runner['symbol']}</b>\n"
        message += f"   ราคา: ${runner['price_at_prediction']:.2f} | Score: {runner['overall_score']}/100\n"
        message += f"   Risk: {risk_level}\n\n"
    
    message += "💡 <i>กลยุทธ์: DCA ทุกสัปดาห์/เดือน</i>"
    
    await send_telegram_message(message)

async def send_no_opportunity_message():
    """ส่งข้อความเมื่อไม่มีโอกาสเลย"""
    
    message = f"⏸️ <b>DCA Alert</b>: {datetime.now().strftime('%d %b %Y')}\n\n"
    message += "ไม่พบโอกาสที่ดีในการลงทุนวันนี้\n\n"
    message += "<b>สาเหตุ:</b>\n"
    message += "   • ไม่มีหุ้น Score สูง (≥70)\n"
    message += "   • ไม่มีหุ้น Oversold ที่น่าสนใจ\n\n"
    message += "💡 <b>แนะนำ:</b>\n"
    message += "   • รอดูสถานการณ์ตลาด\n"
    message += "   • เก็บเงินไว้รอโอกาสที่ดีกว่า\n\n"
    message += "💰 <i>\"Cash is also a position\"</i>"
    
    await send_telegram_message(message)


async def send_daily_dca_recommendation():
    """🎯 Main Function: ส่งคำแนะนำ DCA ประจำวัน"""
    
    print("\n" + "="*60)
    print(f"🚀 Starting Daily DCA Analysis: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")
    
    today = datetime.now().date().isoformat()
    
    # === STEP 1: หาหุ้นที่ Score สูง (≥70) ===
    print("🔍 Searching for high-score stocks (Score ≥ 70)...")
    
    top_stocks = supabase.table("ai_predictions")\
        .select("symbol, overall_score, recommendation, confidence, risk_score, price_at_prediction, created_at")\
        .gte("created_at", today)\
        .gte("overall_score", 70)\
        .in_("recommendation", ["Strong Buy", "Buy"])\
        .order("overall_score", desc=True)\
        .limit(3)\
        .execute()
    
    # === STEP 2: ถ้ามีหุ้น Score สูง → ส่งแบบปกติ ===
    if top_stocks.data:
        print(f"✅ Found {len(top_stocks.data)} high-score stocks")
        for stock in top_stocks.data:
            print(f"   - {stock['symbol']}: Score {stock['overall_score']}/100")
        
        await send_normal_dca(top_stocks.data)
        return
    
    # === STEP 3: ไม่มีหุ้น Score สูง → หา Buy the Dip ===
    print("⚠️ No high-score stocks found")
    print("🔄 Switching to 'Buy the Dip' mode...\n")
    
    await send_dip_opportunities()
    
    print("\n" + "="*60)
    print("✅ Daily DCA analysis completed!")
    print("="*60 + "\n")





async def send_dip_opportunities():
    """🆕 หาหุ้นที่ Oversold + ราคาถูก (Buy the Dip Mode)"""
    
    print("🔄 Switching to 'Buy the Dip' mode...")
    
    today = datetime.now().date().isoformat()
    
    # 1. ดึงหุ้นที่ RSI < 35 (Oversold)
    snapshots = supabase.table("stock_snapshots")\
        .select("symbol, price, rsi, ema_20, ema_50")\
        .gte("recorded_at", today)\
        .lt("rsi", 40)\
        .execute()
    
    if not snapshots.data:
        print("⚠️ No oversold stocks found")
        # 🆕 ใช้ฟังก์ชันใหม่
        await send_enhanced_no_opportunity_message()
        return
    
    # 2. คำนวณหุ้นที่ต่ำกว่า MA20 มาก
    dip_stocks = []  # ✅ สร้างตัวแปรก่อน!
    
    for stock in snapshots.data:
        if not stock.get('ema_20'):
            continue
        
        # คำนวณ % ต่ำกว่า MA20
        below_ma20 = ((stock['ema_20'] - stock['price']) / stock['ema_20']) * 100
        
        # เงื่อนไข: ต่ำกว่า MA20 อย่างน้อย 5%
        if below_ma20 >= 5:
            # ดึงข้อมูล AI prediction
            pred = supabase.table("ai_predictions")\
                .select("overall_score, risk_score, price_target")\
                .eq("symbol", stock['symbol'])\
                .gte("created_at", today)\
                .order("created_at", desc=True)\
                .limit(1)\
                .execute()
            
            if pred.data:
                score = pred.data[0]['overall_score']
                risk = pred.data[0].get('risk_score', 50)
                target = pred.data[0].get('price_target')
                
                # คำนวณ upside
                upside_pct = ((target - stock['price']) / stock['price'] * 100) if target else None
                
                dip_stocks.append({
                    'symbol': stock['symbol'],
                    'price': stock['price'],
                    'rsi': stock['rsi'],
                    'below_ma20': below_ma20,
                    'score': score,
                    'risk': risk,
                    'target': target,
                    'upside_pct': upside_pct
                })
    
    # ✅ ตอนนี้เช็คได้แล้ว เพราะ dip_stocks มีอยู่จริง
    if not dip_stocks:
        print("⚠️ No dip opportunities found")
        await send_enhanced_no_opportunity_message()
        return
    
    # 3. เรียงตาม RSI (ยิ่งต่ำยิ่งดี = โอกาสกลับตัวสูง)
    dip_stocks.sort(key=lambda x: x['rsi'])
    
    # 4. สร้างข้อความ
    message = f"🔴 <b>Market Dip Alert</b>: {datetime.now().strftime('%d %b %Y')}\n\n"
    message += "⚠️ ไม่มีหุ้น Score ≥ 70 วันนี้\n"
    message += "💡 แต่พบ<b>โอกาสซื้อราคาถูก</b> (Buy the Dip)\n\n"
    
    # แสดงหุ้นที่น่าสนใจ 3 ตัวแรก
    for i, stock in enumerate(dip_stocks[:3], 1):
        # ระดับความเสี่ยง
        if stock['risk'] < 30:
            risk_emoji = "🟢"
            risk_text = "ต่ำ"
        elif stock['risk'] < 60:
            risk_emoji = "🟡"
            risk_text = "ปานกลาง"
        else:
            risk_emoji = "🔴"
            risk_text = "สูง"
        
        # ระดับ RSI
        if stock['rsi'] < 25:
            rsi_status = "Strong Oversold"
        elif stock['rsi'] < 30:
            rsi_status = "Oversold"
        else:
            rsi_status = "เกือบ Oversold"
        
        message += f"💎 <b>#{i}: {stock['symbol']}</b>\n"
        message += f"   ราคา: ${stock['price']:.2f}\n"
        message += f"   Score: {stock['score']}/100 | {risk_emoji} Risk: {risk_text}\n"
        message += f"   RSI: {stock['rsi']:.1f} ({rsi_status})\n"
        message += f"   ต่ำกว่า MA20: {stock['below_ma20']:.1f}%\n"
        
        if stock['upside_pct']:
            message += f"   🎯 Target: ${stock['target']:.2f} (+{stock['upside_pct']:.1f}%)\n"
        
        message += "\n"
    
    # เพิ่มคำแนะนำ
    message += "📋 <b>กลยุทธ์แนะนำ:</b>\n"
    message += "   • ซื้อทีละน้อย แบ่งเป็น 2-3 งวด\n"
    message += "   • รอดูอีก 1-2 วัน ราคาอาจถูกกว่านี้\n"
    message += "   • ตั้ง Stop Loss ไว้ -10% เผื่อลงต่อ\n\n"
    message += "⚠️ <i>High Risk, High Reward - ลงทุนเท่าที่เสียได้</i>"
    
    await send_telegram_message(message)
     



def format_telegram_summary(stats, total_stocks, start_time):
    """สร้างข้อความสรุปสำหรับ Telegram"""
    
    duration = datetime.now() - start_time
    duration_str = str(duration).split('.')[0]  # ตัด microseconds
    
    message = f"""
🤖 <b>Stock Analysis Completed</b>

📊 <b>Summary:</b>
- Total: {total_stocks} stocks
- ✅ Success: {stats['success']}
- ❌ Failed: {stats['failed']}
- ⏱ Duration: {duration_str}

📈 <b>Recommendations:</b>
- 🟢 Strong Buy: {stats['strong_buy']}
- 🟢 Buy: {stats['buy']}
- 🟡 Hold: {stats['hold']}
- 🔴 Sell: {stats['sell']}
"""
    
    # เพิ่ม Confidence ถ้ามี
    if stats['high_confidence'] + stats['medium_confidence'] + stats['low_confidence'] > 0:
        message += f"""
🎯 <b>Confidence:</b>
- 🔥 High: {stats['high_confidence']}
- 📊 Medium: {stats['medium_confidence']}
- ⚠️ Low: {stats['low_confidence']}
"""
    
    # Success Rate
    if total_stocks > 0:
        success_rate = (stats['success'] / total_stocks) * 100
        message += f"\n✨ <b>Success Rate:</b> {success_rate:.1f}%"
    
    message += f"\n\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    return message

def get_current_session():
    """ระบุช่วงเวลาการซื้อขาย"""
    now_utc = datetime.utcnow()
    hour_utc = now_utc.hour
    
    # UTC Time:
    # 14:00-14:30 = Pre-market (21:00-21:30 น.ไทย)
    # 14:30-21:00 = Market Hours (21:30-04:00 น.ไทย)
    # 21:00-00:00 = After Hours (04:00-07:00 น.ไทย)
    # 00:00-14:00 = After Close (07:00-21:00 น.ไทย)
    
    if 14 <= hour_utc < 15:
        return "pre_market", "🌅 ก่อนเปิดตลาด"
    elif 15 <= hour_utc < 21:
        return "market_hours", "📊 ช่วงเปิดตลาด"
    elif 21 <= hour_utc < 24:
        return "after_hours", "🌙 หลังปิดตลาด"
    else:
        return "after_close", "☀️ สรุปตลาดเมื่อคืน"


def get_market_indices_simple():
    """ดึงข้อมูล SPY และ QQQ อย่างง่าย"""
    try:
        indices = {'SPY': 0, 'QQQ': 0}
        
        for symbol in ['SPY', 'QQQ']:
            snapshot = supabase.table("stock_snapshots")\
                .select("change_pct")\
                .eq("symbol", symbol)\
                .order("recorded_at", desc=True)\
                .limit(1)\
                .execute()
            
            if snapshot.data:
                indices[symbol] = snapshot.data[0]['change_pct']
        
        return indices
    except:
        return {'SPY': 0, 'QQQ': 0}


def get_opportunities_by_type(session_type):
    """
    หาโอกาสตามประเภทช่วงเวลา
    
    - Pre-market: โอกาส gap down
    - Market Hours: momentum + oversold
    - After Hours: สรุปวัน + setup พรุ่งนี้
    - After Close: โอกาส DCA คุณภาพดี
    """
    try:
        # เวลาล่าสุด (10 นาทีที่แล้ว)
        cutoff_time = (datetime.now() - timedelta(minutes=10)).isoformat()
        
        # ดึง snapshot + prediction ล่าสุด
        snapshots = supabase.table("stock_snapshots")\
            .select("*")\
            .gte("recorded_at", cutoff_time)\
            .execute()
        
        predictions = supabase.table("ai_predictions")\
            .select("*")\
            .gte("created_at", cutoff_time)\
            .execute()
        
        if not snapshots.data:
            return []
        
        pred_dict = {p['symbol']: p for p in (predictions.data or [])}
        opportunities = []
        
        for snap in snapshots.data:
            symbol = snap['symbol']
            pred = pred_dict.get(symbol, {})
            
            change_pct = snap.get('change_pct', 0)
            price = snap.get('price')
            ema_20 = snap.get('ema_20')
            rsi = snap.get('rsi')
            
            overall_score = pred.get('overall_score', 0)
            confidence = pred.get('confidence', '')
            recommendation = pred.get('recommendation', '')
            
            # กรองตามช่วงเวลา
            is_opportunity = False
            alert_type = ""
            
            if session_type == "pre_market":
                # Pre-market: Gap down ≥ 3% + Score ดี
                if change_pct <= -3 and overall_score >= 65:
                    is_opportunity = True
                    alert_type = "Gap Down"
                    
            elif session_type == "market_hours":
                # Market Hours: ลง ≥ 2% + Oversold
                if change_pct <= -2 and rsi and rsi < 40:
                    is_opportunity = True
                    alert_type = "Intraday Dip"
                    
            elif session_type == "after_hours":
                # After Hours: ลงแรง + คุณภาพดี
                if change_pct <= -2.5 and overall_score >= 70:
                    is_opportunity = True
                    alert_type = "After-Hours Drop"
                    
            else:  # after_close
                # หลังปิดตลาด: DCA คุณภาพสูง
                if (change_pct <= -2 and 
                    overall_score >= 70 and 
                    price and ema_20 and price < ema_20 and
                    confidence in ['High', 'Medium']):
                    is_opportunity = True
                    alert_type = "DCA Setup"
            
            if is_opportunity:
                below_ma_pct = None
                if price and ema_20:
                    below_ma_pct = ((ema_20 - price) / price) * 100
                
                opportunities.append({
                    'symbol': symbol,
                    'price': price,
                    'change_pct': change_pct,
                    'overall_score': overall_score,
                    'confidence': confidence,
                    'recommendation': recommendation,
                    'below_ma_pct': below_ma_pct,
                    'rsi': rsi,
                    'price_target': pred.get('price_target'),
                    'risk_score': pred.get('risk_score', 0),
                    'alert_type': alert_type
                })
        
        # เรียงตาม change_pct (ลงแรงสุดก่อน)
        opportunities.sort(key=lambda x: x['change_pct'])
        return opportunities[:5]
        
    except Exception as e:
        print(f"⚠️ Error finding opportunities: {e}")
        import traceback
        traceback.print_exc()
        return []


def format_alert_by_session(opportunities, market_data, session_type, session_name):
    """สร้างข้อความตามช่วงเวลา"""
    
    now = datetime.now()
    
    # ถ้าไม่มีโอกาส แต่มีการเก็บข้อมูล ให้ส่งสรุปสั้น ๆ
    if not opportunities:
        message = f"{session_name}\n\n"
        message += f"📊 <b>ภาพรวมตลาด</b>\n"
        message += f"S&P 500: <b>{market_data['SPY']:+.1f}%</b> | "
        message += f"NASDAQ: <b>{market_data['QQQ']:+.1f}%</b>\n\n"
        
        if session_type == "after_close":
            message += "ℹ️ ไม่มีหุ้นที่ตรงเงื่อนไข DCA วันนี้"
        else:
            message += "ℹ️ ยังไม่มีสัญญาณที่ชัดเจน"
        
        message += f"\n⏰ {now.strftime('%H:%M น.')}"
        return message
    
    # มีโอกาส → สร้างข้อความเต็ม
    message = f"{session_name}\n\n"
    
    # Header ตามช่วงเวลา
    if session_type == "pre_market":
        message += "🎯 <b>Gap Down ที่น่าสนใจ</b>\n\n"
    elif session_type == "market_hours":
        message += "⚡ <b>โอกาสช่วงเปิดตลาด</b>\n\n"
    elif session_type == "after_hours":
        message += "🌙 <b>After-Hours Alert</b>\n\n"
    else:
        message += "💎 <b>โอกาส DCA วันนี้</b>\n\n"
    
    # แสดงหุ้น (3 อันดับแรก)
    for i, opp in enumerate(opportunities[:3], 1):
        message += f"{i}️⃣ <b>{opp['symbol']}</b> ${opp['price']:.2f} "
        message += f"<b>({opp['change_pct']:+.1f}%)</b>\n"
        
        # Score + Confidence
        message += f"   💎 Score {opp['overall_score']}/100"
        if opp.get('confidence'):
            message += f" | 🎯 {opp['confidence']}"
        message += "\n"
        
        # ข้อมูลเทคนิค
        if opp.get('below_ma_pct'):
            message += f"   📉 ต่ำกว่า MA20: {opp['below_ma_pct']:.1f}%\n"
        
        # สัญญาณพิเศษ
        signals = []
        
        if opp.get('rsi'):
            if opp['rsi'] < 30:
                signals.append(f"RSI {opp['rsi']:.0f} (Strong Oversold)")
            elif opp['rsi'] < 40:
                signals.append(f"RSI {opp['rsi']:.0f} (Oversold)")
        
        if opp.get('risk_score') and opp['risk_score'] < 30:
            signals.append("ความเสี่ยงต่ำ")
        
        if opp.get('recommendation') == 'Strong Buy':
            signals.append("⭐ Strong Buy")
        
        if signals:
            message += f"   ⚡ {' | '.join(signals)}\n"
        
        # Price Target
        if opp.get('price_target'):
            upside = ((opp['price_target'] - opp['price']) / opp['price']) * 100
            message += f"   🎯 Target: ${opp['price_target']:.2f} (+{upside:.1f}%)\n"
        
        message += "\n"
    
    # ภาพรวมตลาด
    message += "📊 <b>ตลาด:</b> "
    message += f"S&P {market_data['SPY']:+.1f}% | "
    message += f"NASDAQ {market_data['QQQ']:+.1f}%\n\n"
    
    # คำแนะนำตามช่วงเวลา
    avg_change = (market_data['SPY'] + market_data['QQQ']) / 2
    
    if session_type == "pre_market":
        message += "💡 <b>แนะนำ:</b> รอ 30 นาทีแรกให้ราคาเสถียร"
    elif session_type == "market_hours":
        if avg_change <= -1.5:
            message += "💡 <b>แนะนำ:</b> ตลาดลงแรง → โอกาสซื้อเพิ่ม"
        else:
            message += "💡 <b>แนะนำ:</b> ติดตามความเคลื่อนไหว"
    elif session_type == "after_hours":
        message += "💡 <b>แนะนำ:</b> ตั้ง Limit Order สำหรับพรุ่งนี้"
    else:  # after_close
        if avg_change <= -2:
            message += "💡 <b>แนะนำ:</b> ตลาดปรับฐานแรง → โอกาสทอง DCA"
        elif avg_change <= -1:
            message += "💡 <b>แนะนำ:</b> แบ่งเงินซื้อ 2-3 ครั้ง"
        else:
            message += "💡 <b>แนะนำ:</b> พิจารณาซื้อเพิ่มหุ้นคุณภาพดี"
    
    message += f"\n⏰ {now.strftime('%H:%M น.')}"
    
    return message


# ========================================
# 📊 ANALYTICS FUNCTIONS
# ========================================

def get_market_overview_detailed():
    """ดึงข้อมูลตลาดแบบละเอียด พร้อมเปรียบเทียบ"""
    try:
        today = datetime.now().date().isoformat()
        yesterday = (datetime.now() - timedelta(days=1)).date().isoformat()
        
        result = {
            'SPY': {'current': 0, 'prev': 0, 'change': 0},
            'QQQ': {'current': 0, 'prev': 0, 'change': 0},
            'VIX': {'current': 0, 'prev': 0, 'change': 0}
        }
        
        for symbol in ['SPY', 'QQQ', 'VIX']:
            # ข้อมูลวันนี้
            today_data = supabase.table("stock_snapshots")\
                .select("price, change_pct")\
                .eq("symbol", symbol)\
                .gte("recorded_at", today)\
                .order("recorded_at", desc=True)\
                .limit(1)\
                .execute()
            
            # ข้อมูลเมื่อวาน
            yesterday_data = supabase.table("stock_snapshots")\
                .select("price")\
                .eq("symbol", symbol)\
                .gte("recorded_at", yesterday)\
                .lt("recorded_at", today)\
                .order("recorded_at", desc=True)\
                .limit(1)\
                .execute()
            
            if today_data.data:
                result[symbol]['current'] = today_data.data[0]['price']
                result[symbol]['change'] = today_data.data[0]['change_pct']
                
                if yesterday_data.data:
                    result[symbol]['prev'] = yesterday_data.data[0]['price']
        
        return result
        
    except Exception as e:
        print(f"⚠️ Error getting market overview: {e}")
        return None


def get_portfolio_health_distribution():
    """แบ่งหุ้นตาม Health Zone"""
    try:
        today = datetime.now().date().isoformat()
        
        # ดึง snapshots ล่าสุด
        snapshots = supabase.table("stock_snapshots")\
            .select("symbol, rsi, price, ema_20, change_pct")\
            .gte("recorded_at", today)\
            .execute()
        
        if not snapshots.data:
            return None
        
        zones = {
            'strong_bullish': [],    # RSI 50-70, Above EMA20
            'bullish': [],           # RSI 40-70, Above EMA20
            'neutral': [],           # RSI 30-70
            'oversold': [],          # RSI < 30
            'overbought': [],        # RSI > 70
            'bearish': []            # Below EMA20
        }
        
        for snap in snapshots.data:
            symbol = snap['symbol']
            rsi = snap.get('rsi')
            price = snap.get('price')
            ema_20 = snap.get('ema_20')
            change = snap.get('change_pct', 0)
            
            if not rsi:
                continue
            
            # จัด Zone
            if rsi > 70:
                zones['overbought'].append({'symbol': symbol, 'rsi': rsi, 'change': change})
            elif rsi < 30:
                zones['oversold'].append({'symbol': symbol, 'rsi': rsi, 'change': change})
            elif price and ema_20:
                if price > ema_20:
                    if 50 <= rsi <= 70:
                        zones['strong_bullish'].append({'symbol': symbol, 'rsi': rsi, 'change': change})
                    else:
                        zones['bullish'].append({'symbol': symbol, 'rsi': rsi, 'change': change})
                else:
                    zones['bearish'].append({'symbol': symbol, 'rsi': rsi, 'change': change})
            else:
                zones['neutral'].append({'symbol': symbol, 'rsi': rsi, 'change': change})
        
        return zones
        
    except Exception as e:
        print(f"⚠️ Error calculating portfolio health: {e}")
        return None


def get_top_movers():
    """หาหุ้นที่ขึ้น/ลงมากที่สุด พร้อม Score"""
    try:
        today = datetime.now().date().isoformat()
        
        # ดึง snapshots
        snapshots = supabase.table("stock_snapshots")\
            .select("symbol, price, change_pct, rsi")\
            .gte("recorded_at", today)\
            .execute()
        
        # ดึง predictions
        predictions = supabase.table("ai_predictions")\
            .select("symbol, overall_score, recommendation")\
            .gte("created_at", today)\
            .execute()
        
        if not snapshots.data:
            return None
        
        # สร้าง dict สำหรับ predictions
        pred_dict = {p['symbol']: p for p in (predictions.data or [])}
        
        # รวมข้อมูล
        movers = []
        for snap in snapshots.data:
            symbol = snap['symbol']
            pred = pred_dict.get(symbol, {})
            
            movers.append({
                'symbol': symbol,
                'price': snap['price'],
                'change': snap['change_pct'],
                'rsi': snap.get('rsi'),
                'score': pred.get('overall_score', 0),
                'recommendation': pred.get('recommendation', 'N/A')
            })
        
        # เรียงตาม change_pct
        movers.sort(key=lambda x: x['change'], reverse=True)
        
        return {
            'gainers': movers[:5],
            'losers': movers[-5:][::-1]  # กลับลำดับ
        }
        
    except Exception as e:
        print(f"⚠️ Error getting top movers: {e}")
        return None


def get_sector_performance():
    """วิเคราะห์ Performance แยกตาม Sector/Category"""
    try:
        today = datetime.now().date().isoformat()
        
        # ดึง master + snapshots
        stocks = supabase.table("stock_master")\
            .select("symbol, category")\
            .eq("is_active", True)\
            .execute()
        
        snapshots = supabase.table("stock_snapshots")\
            .select("symbol, change_pct")\
            .gte("recorded_at", today)\
            .execute()
        
        if not stocks.data or not snapshots.data:
            return None
        
        # สร้าง dict
        snap_dict = {s['symbol']: s['change_pct'] for s in snapshots.data}
        
        # คำนวณ Average ตาม Category
        categories = {}
        for stock in stocks.data:
            category = stock.get('category', 'Core')
            symbol = stock['symbol']
            
            if symbol in snap_dict:
                if category not in categories:
                    categories[category] = []
                categories[category].append(snap_dict[symbol])
        
        # คำนวณค่าเฉลี่ย
        result = {}
        for cat, changes in categories.items():
            result[cat] = {
                'avg_change': round(sum(changes) / len(changes), 2),
                'count': len(changes)
            }
        
        # เรียงตาม performance
        sorted_cats = sorted(result.items(), key=lambda x: x[1]['avg_change'], reverse=True)
        
        return dict(sorted_cats)
        
    except Exception as e:
        print(f"⚠️ Error calculating sector performance: {e}")
        return None


def get_score_changes():
    """เปรียบเทียบ Score วันนี้กับเมื่อวาน"""
    try:
        today = datetime.now().date().isoformat()
        yesterday = (datetime.now() - timedelta(days=1)).date().isoformat()
        
        # ดึง predictions วันนี้
        today_preds = supabase.table("ai_predictions")\
            .select("symbol, overall_score")\
            .gte("created_at", today)\
            .execute()
        
        # ดึง predictions เมื่อวาน
        yesterday_preds = supabase.table("ai_predictions")\
            .select("symbol, overall_score")\
            .gte("created_at", yesterday)\
            .lt("created_at", today)\
            .execute()
        
        if not today_preds.data:
            return None
        
        # สร้าง dict
        today_dict = {p['symbol']: p['overall_score'] for p in today_preds.data}
        yesterday_dict = {p['symbol']: p['overall_score'] for p in (yesterday_preds.data or [])}
        
        # คำนวณการเปลี่ยนแปลง
        changes = []
        for symbol, today_score in today_dict.items():
            if symbol in yesterday_dict:
                yesterday_score = yesterday_dict[symbol]
                diff = today_score - yesterday_score
                
                if abs(diff) >= 10:  # เปลี่ยนแปลงอย่างน้อย 10 คะแนน
                    changes.append({
                        'symbol': symbol,
                        'today': today_score,
                        'yesterday': yesterday_score,
                        'change': diff
                    })
        
        # เรียงตามการเปลี่ยนแปลง
        changes.sort(key=lambda x: abs(x['change']), reverse=True)
        
        return changes[:10]  # แสดง 10 อันดับแรก
        
    except Exception as e:
        print(f"⚠️ Error calculating score changes: {e}")
        return None


def get_fear_greed_indicator():
    """สร้าง Fear & Greed Indicator จาก VIX + RSI Portfolio"""
    try:
        today = datetime.now().date().isoformat()
        
        # ดึง VIX
        vix_data = supabase.table("stock_snapshots")\
            .select("price")\
            .eq("symbol", "VIX")\
            .gte("recorded_at", today)\
            .order("recorded_at", desc=True)\
            .limit(1)\
            .execute()
        
        # ดึง RSI เฉลี่ยของ Portfolio
        snapshots = supabase.table("stock_snapshots")\
            .select("rsi")\
            .gte("recorded_at", today)\
            .execute()
        
        vix = vix_data.data[0]['price'] if vix_data.data else 15
        
        rsi_values = [s['rsi'] for s in (snapshots.data or []) if s.get('rsi')]
        avg_rsi = sum(rsi_values) / len(rsi_values) if rsi_values else 50
        
        # คำนวณ Fear & Greed Score (0-100)
        # VIX: ต่ำ = Greed, สูง = Fear
        # RSI: ต่ำ = Fear, สูง = Greed
        
        vix_score = max(0, min(100, (30 - vix) / 30 * 100))  # VIX 30 = Fear, VIX 10 = Greed
        rsi_score = avg_rsi
        
        fg_score = int((vix_score + rsi_score) / 2)
        
        # กำหนดระดับ
        if fg_score >= 75:
            level = "Extreme Greed"
            emoji = "🔴"
        elif fg_score >= 60:
            level = "Greed"
            emoji = "🟠"
        elif fg_score >= 40:
            level = "Neutral"
            emoji = "🟡"
        elif fg_score >= 25:
            level = "Fear"
            emoji = "🔵"
        else:
            level = "Extreme Fear"
            emoji = "🟢"
        
        return {
            'score': fg_score,
            'level': level,
            'emoji': emoji,
            'vix': vix,
            'avg_rsi': round(avg_rsi, 1)
        }
        
    except Exception as e:
        print(f"⚠️ Error calculating fear/greed: {e}")
        return None


# ========================================
# 📱 ENHANCED ALERT FUNCTIONS
# ========================================

async def send_market_pulse_alert():
    """
    📊 Market Pulse - ส่งทุกครั้งที่เก็บข้อมูล
    แสดงภาพรวมตลาด + Portfolio Health + Top Movers
    """
    
    print("\n📊 Generating Market Pulse Alert...")
    
    # ดึงข้อมูล
    market = get_market_overview_detailed()
    health = get_portfolio_health_distribution()
    movers = get_top_movers()
    sectors = get_sector_performance()
    fear_greed = get_fear_greed_indicator()
    
    if not market:
        print("⚠️ Market data not available")
        return
    
    # สร้างข้อความ
    session_type, session_name = get_current_session()
    
    message = f"{session_name}\n\n"
    
    # === MARKET OVERVIEW ===
    message += "📊 <b>Market Overview</b>\n"
    message += f"S&P 500: ${market['SPY']['current']:.2f} "
    message += f"(<b>{market['SPY']['change']:+.1f}%</b>)\n"
    message += f"NASDAQ: ${market['QQQ']['current']:.2f} "
    message += f"(<b>{market['QQQ']['change']:+.1f}%</b>)\n"
    
    if fear_greed:
        message += f"\n{fear_greed['emoji']} <b>Market Sentiment:</b> {fear_greed['level']}\n"
        message += f"VIX: {fear_greed['vix']:.1f} | Avg RSI: {fear_greed['avg_rsi']:.0f}\n"
    
    # === PORTFOLIO HEALTH ===
    if health:
        total_stocks = sum(len(v) for v in health.values())
        message += f"\n🏥 <b>Portfolio Health</b> ({total_stocks} stocks)\n"
        
        if health['strong_bullish']:
            message += f"💪 Strong Bullish: {len(health['strong_bullish'])}\n"
        if health['bullish']:
            message += f"🟢 Bullish: {len(health['bullish'])}\n"
        if health['neutral']:
            message += f"🟡 Neutral: {len(health['neutral'])}\n"
        if health['oversold']:
            message += f"🔵 Oversold: {len(health['oversold'])} "
            message += f"({', '.join([s['symbol'] for s in health['oversold'][:3]])})\n"
        if health['overbought']:
            message += f"🔴 Overbought: {len(health['overbought'])} "
            message += f"({', '.join([s['symbol'] for s in health['overbought'][:3]])})\n"
        if health['bearish']:
            message += f"📉 Bearish: {len(health['bearish'])}\n"
    
    # === TOP MOVERS ===
    if movers:
        message += "\n🚀 <b>Top Movers</b>\n"
        
        # Top Gainer
        if movers['gainers']:
            top = movers['gainers'][0]
            message += f"▲ {top['symbol']}: "
            message += f"<b>+{top['change']:.1f}%</b> "
            message += f"(Score: {top['score']}/100)\n"
        
        # Top Loser
        if movers['losers']:
            bottom = movers['losers'][0]
            message += f"▼ {bottom['symbol']}: "
            message += f"<b>{bottom['change']:.1f}%</b> "
            message += f"(RSI: {bottom['rsi']:.0f} | Score: {bottom['score']}/100)\n"
    
    # === SECTOR PERFORMANCE ===
    if sectors:
        message += "\n📈 <b>Sector Performance</b>\n"
        sector_emoji = {
            'Tech': '💻',
            'Growth': '🚀',
            'Value': '💎',
            'Dividend': '💰',
            'ETF': '📊',
            'Core': '🏛️'
        }
        
        for cat, data in list(sectors.items())[:5]:  # แสดง 5 อันดับแรก
            emoji = sector_emoji.get(cat, '📊')
            change_str = f"+{data['avg_change']:.1f}%" if data['avg_change'] >= 0 else f"{data['avg_change']:.1f}%"
            message += f"{emoji} {cat}: <b>{change_str}</b> ({data['count']} stocks)\n"
    
    # === ACTIONABLE INSIGHT ===
    message += "\n💡 <b>Quick Take:</b> "
    
    if fear_greed:
        if fear_greed['level'] == "Extreme Fear":
            message += "ตลาดกลัวมาก → โอกาสซื้อ! "
        elif fear_greed['level'] == "Extreme Greed":
            message += "ตลาดโลภมาก → ระวังปรับฐาน "
    
    if health:
        if len(health['oversold']) >= 3:
            message += f"มี {len(health['oversold'])} หุ้น Oversold น่าสนใจ"
        elif len(health['strong_bullish']) >= 5:
            message += f"มี {len(health['strong_bullish'])} หุ้นแข็งแกร่ง"
    
    message += f"\n\n⏰ {datetime.now().strftime('%H:%M น.')}"
    
    await send_telegram_message(message)


async def send_daily_trend_analysis():
    """
    📈 Daily Trend Analysis - ส่งวันละ 1 ครั้ง (After Close)
    วิเคราะห์แนวโน้ม 7 วัน + Score Changes
    """
    
    print("\n📈 Generating Daily Trend Analysis...")
    
    score_changes = get_score_changes()
    
    if not score_changes:
        print("⚠️ No significant score changes")
        return
    
    message = "📈 <b>Daily Trend Analysis</b>\n\n"
    
    message += "🔄 <b>Significant Score Changes (24h)</b>\n"
    
    for change in score_changes[:5]:
        arrow = "🟢" if change['change'] > 0 else "🔴"
        message += f"{arrow} <b>{change['symbol']}</b>: "
        message += f"{change['yesterday']} → {change['today']} "
        message += f"(<b>{change['change']:+d}</b>)\n"
    
    message += f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    
    await send_telegram_message(message)


async def send_enhanced_no_opportunity_message():
    """ส่งข้อความเมื่อไม่มีโอกาส พร้อมข้อมูลประกอบ"""
    
    market = get_market_overview_detailed()
    health = get_portfolio_health_distribution()
    fear_greed = get_fear_greed_indicator()
    
    message = f"⏸️ <b>DCA Alert</b>: {datetime.now().strftime('%d %b %Y')}\n\n"
    message += "ไม่พบโอกาสที่ดีในการลงทุนวันนี้\n\n"
    
    message += "<b>สาเหตุ:</b>\n"
    message += "   • ไม่มีหุ้น Score สูง (≥70)\n"
    message += "   • ไม่มีหุ้น Oversold ที่น่าสนใจ\n\n"
    
    # เพิ่มข้อมูลตลาด
    if market:
        message += "<b>ภาพรวมตลาด:</b>\n"
        message += f"   • S&P 500: {market['SPY']['change']:+.1f}%\n"
        message += f"   • NASDAQ: {market['QQQ']['change']:+.1f}%\n"
        
        if fear_greed:
            message += f"   • Sentiment: {fear_greed['level']}\n"
        
        message += "\n"
    
    # เพิ่มข้อมูล Portfolio
    if health:
        message += "<b>Portfolio Status:</b>\n"
        if health['oversold']:
            message += f"   • Oversold: {len(health['oversold'])} stocks (แต่ Score ไม่ดี)\n"
        if health['overbought']:
            message += f"   • Overbought: {len(health['overbought'])} stocks (ระวัง!)\n"
        message += "\n"
    
    message += "💡 <b>แนะนำ:</b>\n"
    message += "   • รอดูสถานการณ์ตลาด\n"
    message += "   • เก็บเงินไว้รอโอกาสที่ดีกว่า\n\n"
    message += "💰 <i>\"Cash is also a position\"</i>"
    
    await send_telegram_message(message)




async def send_market_alert_after_collection():
    """ส่ง Alert หลังเก็บข้อมูลทุกครั้ง - ปรับข้อความตามช่วงเวลา"""
    
    print(f"\n{'='*60}")
    print("📱 Generating Market Alert...")
    print(f"{'='*60}\n")

    await send_market_pulse_alert()
    
    # 1. ระบุช่วงเวลา
    session_type, session_name = get_current_session()
    print(f"🕐 Session: {session_type} - {session_name}")
    
    # 2. หาโอกาสตามช่วงเวลา
    opportunities = get_opportunities_by_type(session_type)
    print(f"🔍 Found {len(opportunities)} opportunities")
    
    # 3. ดึงข้อมูลตลาด
    market_data = get_market_indices_simple()
    print(f"📊 Market: SPY {market_data['SPY']:+.1f}% | QQQ {market_data['QQQ']:+.1f}%")
    
    # 4. สร้างข้อความ (ส่งเสมอ แม้ไม่มีโอกาส)
    message = format_alert_by_session(opportunities, market_data, session_type, session_name)
    
    if message:
        print(f"\n📱 Sending alert...")
        print("="*60)
        print(message.replace('<b>', '').replace('</b>', ''))
        print("="*60)
        
        # ส่ง Telegram
        success = await send_telegram_message(message)
        
        if success:
            print("\n✅ Market alert sent successfully!")
        else:
            print("\n⚠️ Failed to send alert")
    
    print(f"{'='*60}\n")
 




async def main():
    global supabase
    
    # ดึงข้อมูลหุ้นทั้งหมด
    res = supabase.table("stock_master")\
        .select("symbol, category")\
        .eq("is_active", True)\
        .execute()
    stocks = res.data
    
    if not stocks:
        print("📭 No active symbols found in stock_master.")
        return

    # ✅ บันทึกเวลาเริ่มต้น
    start_time = datetime.now()
    
    # ✅ ส่ง notification เริ่มต้น
    await send_telegram_message(
        f"🚀 <b>Stock Analysis Started</b>\n\n"
        f"📊 Processing {len(stocks)} symbols\n"
        f"⏰ {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    print(f"\n🚀 Starting technical analysis for {len(stocks)} symbols")
    print(f"📅 Analysis time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # ตัวแปรสำหรับสถิติ
    stats = {
        'success': 0,
        'failed': 0,
        'strong_buy': 0,
        'buy': 0,
        'hold': 0,
        'sell': 0,
        'high_confidence': 0,
        'medium_confidence': 0,
        'low_confidence': 0
    }
    
    # ============================================
    # 🔄 LOOP: ประมวลผลหุ้นทีละตัว
    # ============================================
    for idx, stock_data in enumerate(stocks, 1):
        symbol = stock_data['symbol']
        category = stock_data.get('category', 'Core')
        
        print(f"\n{'='*60}")
        print(f"[{idx}/{len(stocks)}] Processing: {symbol} ({category})")
        print(f"{'='*60}")
        
        # ... (โค้ดประมวลผลหุ้นทั้งหมด - ไม่เปลี่ยนแปลง) ...
        # STEP 1: ดึงข้อมูล Technical
        # STEP 2: ดึง Market Cap + Fundamental Data
        # STEP 3: บันทึก Snapshot
        # STEP 4: ดึงและบันทึกข่าว
        # STEP 5: คำนวณ AI Prediction
        # STEP 6: บันทึก AI Prediction
        
        # หน่วงเวลาก่อนประมวลผลหุ้นถัดไป
        await asyncio.sleep(3)
    
    # ============================================
    # ✅ สรุปผลการทำงาน (หลังเก็บข้อมูลเสร็จ)
    # ============================================
    print(f"\n{'='*60}")
    print("✅ Technical data collection completed!")
    print(f"{'='*60}")
    print(f"\n📊 Summary Statistics:")
    print(f"   Total Processed: {len(stocks)}")
    print(f"   ✅ Success: {stats['success']}")
    print(f"   ❌ Failed: {stats['failed']}")
    
    print(f"\n📈 Recommendations Breakdown:")
    print(f"   🟢 Strong Buy: {stats['strong_buy']}")
    print(f"   🟢 Buy: {stats['buy']}")
    print(f"   🟡 Hold: {stats['hold']}")
    print(f"   🔴 Sell: {stats['sell']}")
    
    # แสดงสถิติ Confidence
    if stats['high_confidence'] + stats['medium_confidence'] + stats['low_confidence'] > 0:
        print(f"\n🎯 Confidence Distribution:")
        print(f"   🔥 High Confidence: {stats['high_confidence']}")
        print(f"   📊 Medium Confidence: {stats['medium_confidence']}")
        print(f"   ⚠️ Low Confidence: {stats['low_confidence']}")
    
    # คำนวณ success rate
    if len(stocks) > 0:
        success_rate = (stats['success'] / len(stocks)) * 100
        print(f"\n✨ Success Rate: {success_rate:.1f}%")
    
    print(f"\n⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # ============================================
    # 📱 ส่งสรุปผล (ต้องส่งก่อน Alert!)
    # ============================================
    try:
        summary_message = format_telegram_summary(stats, len(stocks), start_time)
        await send_telegram_message(summary_message)
        print("✅ Summary sent to Telegram")
    except Exception as e:
        print(f"⚠️ Failed to send summary: {e}")
    
    # ============================================
    # 📱 ส่ง Market Alert (หลังเก็บข้อมูลเสร็จแล้ว)
    # ============================================
    try:
        await send_market_alert_after_collection()
        print("✅ Market alert sent to Telegram")
    except Exception as e:
        print(f"⚠️ Failed to send market alert: {e}")
    
    # ============================================
    # 📱 ส่ง Trend Analysis (เฉพาะหลังปิดตลาด)
    # ============================================
    try:
        session_type, _ = get_current_session()
        if session_type == "after_close":
            await send_daily_trend_analysis()
            print("✅ Daily trend analysis sent to Telegram")
    except Exception as e:
        print(f"⚠️ Failed to send trend analysis: {e}")
    
    print(f"\n{'='*60}")
    print("🎉 All tasks completed successfully!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())  # ✅ เรียก main() ไม่ใช่ send_daily_dca_recommendation()
  
