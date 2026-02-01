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


async def main():
    global supabase
    
    # ดึงข้อมูลหุ้นทั้งหมด (ไม่ดึง market_cap จาก DB)
    res = supabase.table("stock_master")\
        .select("symbol, category")\
        .eq("is_active", True)\
        .execute()
    stocks = res.data
    
    if not stocks:
        print("📭 No active symbols found in stock_master.")
        return

    print(f"\n🚀 Starting technical analysis for {len(stocks)} symbols")
    print(f"📅 Analysis time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # ตัวแปรสำหรับสถิติ
    stats = {
        'success': 0,
        'failed': 0,
        'strong_buy': 0,
        'buy': 0,
        'hold': 0,
        'sell': 0
    }
    
    for idx, stock_data in enumerate(stocks, 1):
        symbol = stock_data['symbol']
        category = stock_data.get('category', 'Core')
        
        print(f"\n{'='*60}")
        print(f"[{idx}/{len(stocks)}] Processing: {symbol} ({category})")
        print(f"{'='*60}")
        
        # ============================================
        # STEP 1: ดึงข้อมูล Technical (เหมือนเดิม)
        # ============================================
        data = await fetch_data_waterfall(symbol)
        
        if not data:
            print(f"❌ Failed: {symbol}")
            stats['failed'] += 1
            await asyncio.sleep(5)
            continue
        
        # 🆕 ดึง market_cap จาก yfinance (ไม่ใช่จาก DB)
        market_cap = None
        try:
            if category != 'ETF':
                stock = yf.Ticker(symbol)
                market_cap = stock.info.get('marketCap')
                
                if market_cap:
                    market_cap_str = f"${market_cap/1e9:.1f}B" if market_cap >= 1e9 else f"${market_cap/1e6:.1f}M"
                    print(f"Market Cap: {market_cap_str}")
        except Exception as mc_error:
            print(f"⚠️ Could not fetch market cap: {mc_error}")
        
        if not data.get("ema_200"):
            print(f"⚠️ {symbol}: No EMA 200 data available")
        
        # ============================================
        # STEP 2: คำนวณ Metrics (เหมือนเดิม)
        # ============================================
        print(f"📊 Calculating metrics for {symbol}...")
        
        upside_pct = calculate_upside_pct(
            data.get("price"), 
            data.get("ema_200"),
            data.get("ema_50")
        )
        
        # ข้าม analyst/sentiment สำหรับ ETF (เหมือนเดิม)
        analyst_pct = None if category == 'ETF' else fetch_analyst_data(symbol)
        sentiment = None if category == 'ETF' else fetch_sentiment_score(symbol)
        
        # ============================================
        # STEP 3: บันทึก Snapshot (เหมือนเดิม)
        # ============================================
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
        
        # บันทึก snapshot (เหมือนเดิม)
        max_db_retries = 3
        snapshot_saved = False
        
        for db_attempt in range(max_db_retries):
            try:
                supabase.table("stock_snapshots").insert(snapshot_payload).execute()
                print(f"✅ Snapshot saved: {symbol}")
                print(f"   Price: ${data.get('price'):.2f} | Change: {data.get('change_pct'):.2f}%")
                if data.get('rsi'):
                    print(f"   RSI: {data.get('rsi'):.2f} | Upside: {upside_pct}%")
                snapshot_saved = True
                break
            except Exception as db_error:
                print(f"⚠️ Database error (attempt {db_attempt + 1}/{max_db_retries}): {db_error}")
                if db_attempt < max_db_retries - 1:
                    await asyncio.sleep(2)
                    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                else:
                    print(f"❌ Failed to save snapshot for {symbol}")
                    break
        
        if not snapshot_saved:
            stats['failed'] += 1
            await asyncio.sleep(3)
            continue
        
        # ============================================
        # STEP 4: ดึงและบันทึกข่าว (เหมือนเดิม + ปรับ Sentiment)
        # ============================================
        news_sentiment_advanced = None
        
        if category != 'ETF':
            print(f"📰 Fetching news for {symbol}...")
            news_records = fetch_news_data(symbol)
            
            print(f"📊 Retrieved {len(news_records)} valid news articles")
            
            if news_records:
                try:
                    saved_count = 0
                    sentiment_scores = []
                    
                    for news in news_records:
                        try:
                            supabase.table("stock_news").insert(news).execute()
                            saved_count += 1
                            
                            # 🆕 คำนวณ Sentiment แบบใหม่ (ถ้ามีฟังก์ชัน)
                            if 'calculate_news_sentiment_advanced' in globals():
                                adv_sentiment = calculate_news_sentiment_advanced(
                                    news.get('title', ''),
                                    news.get('summary', '')
                                )
                                sentiment_scores.append(adv_sentiment)
                            
                        except Exception as dup_error:
                            if "duplicate" not in str(dup_error).lower():
                                print(f"⚠️ News error: {dup_error}")
                    
                    print(f"✅ Saved {saved_count}/{len(news_records)} news for {symbol}")
                    
                    # คำนวณ Sentiment เฉลี่ย
                    if sentiment_scores:
                        news_sentiment_advanced = round(sum(sentiment_scores) / len(sentiment_scores), 2)
                        print(f"   Advanced Sentiment: {news_sentiment_advanced:.2f}")
                    
                except Exception as news_error:
                    print(f"⚠️ Failed to save news for {symbol}: {news_error}")
            else:
                print(f"📭 No valid news found for {symbol}")
        
        # ============================================
        # STEP 5: คำนวณ AI Prediction
        # ============================================
        print(f"🤖 Calculating AI prediction for {symbol}...")
        
        # ดึง fundamental data (เหมือนเดิม)
        fundamental_data = None if category == 'ETF' else fetch_fundamental_data(symbol)
        
        # เตรียมข้อมูล Technical
        tech_data_full = {
            'price': data.get('price'),
            'rsi': data.get('rsi'),
            'macd': data.get('macd'),
            'macd_signal': data.get('macd_signal'),
            'ema_20': data.get('ema_20'),
            'ema_50': data.get('ema_50'),
            'ema_200': data.get('ema_200'),
            'bb_upper': data.get('bb_upper'),
            'bb_lower': data.get('bb_lower'),
            'upside_pct': upside_pct,
            'analyst_buy_pct': analyst_pct
        }
        
        # ใช้ Sentiment แบบใหม่ถ้ามี
        final_sentiment = news_sentiment_advanced if news_sentiment_advanced is not None else sentiment
        
        # คำนวณ Overall Score (ใช้ฟังก์ชันใหม่ถ้ามี)
        if 'calculate_overall_score_with_risk' in globals():
            overall_score = calculate_overall_score_with_risk(
                symbol=symbol,
                tech_data=tech_data_full,
                fundamental_data=fundamental_data,
                news_sentiment=final_sentiment,
                category=category,
                market_cap=market_cap  # ← ใช้ market_cap ที่ดึงมา
            )
            risk_score = calculate_risk_score(tech_data_full, fundamental_data, market_cap)
        else:
            # ใช้เวอร์ชันเดิม
            overall_score = calculate_overall_score(
                symbol=symbol,
                tech_data=tech_data_full,
                fundamental_data=fundamental_data,
                news_sentiment=final_sentiment
            )
            risk_score = 0
        
        # สร้างคำแนะนำ
        if 'generate_recommendation_advanced' in globals():
            recommendation_data = generate_recommendation_advanced(
                overall_score=overall_score,
                price=data.get('price'),
                upside_pct=upside_pct,
                risk_score=risk_score,
                category=category
            )
            
            recommendation = recommendation_data['recommendation']
            reason = recommendation_data['reason']
            price_target = recommendation_data['price_target']
            confidence = recommendation_data.get('confidence', 'Medium')
            time_horizon = recommendation_data.get('time_horizon', '6 months')
        else:
            # ใช้เวอร์ชันเดิม
            recommendation, reason, price_target = generate_recommendation(
                overall_score=overall_score,
                price=data.get('price'),
                upside_pct=upside_pct
            )
            confidence = None
            time_horizon = None
        
        # ============================================
        # STEP 6: บันทึก AI Prediction
        # ============================================
        prediction_payload = {
            "symbol": symbol,
            "ai_model": "rule_based_v2" if 'calculate_overall_score_with_risk' in globals() else "rule_based_v1",
            "overall_score": overall_score,
            "recommendation": recommendation,
            "price_at_prediction": data.get('price'),
            "actual_outcome": None
        }
        
        # เพิ่มฟิลด์ใหม่ถ้ามี
        if risk_score > 0:
            prediction_payload["risk_score"] = risk_score
        if confidence:
            prediction_payload["confidence"] = confidence
        if price_target:
            prediction_payload["price_target"] = price_target
        if time_horizon:
            prediction_payload["time_horizon"] = time_horizon
        
        try:
            supabase.table("ai_predictions").insert(prediction_payload).execute()
            
            print(f"✅ AI Prediction saved: {symbol}")
            print(f"   Score: {overall_score}/100 | {recommendation}")
            
            if risk_score > 0:
                risk_level = 'High' if risk_score >= 60 else 'Medium' if risk_score >= 30 else 'Low'
                print(f"   Risk: {risk_score}/100 ({risk_level})")
            
            if confidence:
                print(f"   Confidence: {confidence}")
            
            print(f"   Reason: {reason}")
            
            if price_target:
                upside_to_target = ((price_target - data.get('price')) / data.get('price')) * 100
                print(f"   Target: ${price_target:.2f} (+{upside_to_target:.1f}%)")
            
            if time_horizon:
                print(f"   Horizon: {time_horizon}")
            
            # อัพเดตสถิติ
            stats['success'] += 1
            if recommendation == 'Strong Buy':
                stats['strong_buy'] += 1
            elif recommendation == 'Buy':
                stats['buy'] += 1
            elif recommendation == 'Hold':
                stats['hold'] += 1
            elif recommendation in ['Sell', 'Strong Sell']:
                stats['sell'] += 1
                
        except Exception as pred_error:
            print(f"⚠️ Failed to save prediction for {symbol}: {pred_error}")
            stats['failed'] += 1
        
        await asyncio.sleep(3)
    
    # ============================================
    # สรุปผลการทำงาน
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
    print(f"\n⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
 
