#save060369 11:10

import os
import asyncio
import yfinance as yf
import pandas as pd
import talib 
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from supabase import create_client, Client
import requests 
from datetime import datetime, timedelta
from deep_translator import GoogleTranslator 


# --- Configuration ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TWELVE_DATA_KEY = os.getenv("TWELVE_DATA_KEY")
FINNHUB_KEY = os.getenv("FINNHUB_KEY") 
# Investor Level Configuration 

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
        
        # 5. เอาแค่ 20 ข่าวล่าสุด (เพิ่มจาก 10)
        news_list = data[:20]  # ✅ เปลี่ยนจาก 10 เป็น 20

        
        
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
            "pe_ratio": info.get('trailingPE'),
            "pe_ratio_forward": info.get('forwardPE'),
            "peg_ratio": info.get('pegRatio'),
            "eps_growth_pct": info.get('earningsGrowth', 0) * 100 if info.get('earningsGrowth') else None,
            "market_cap": info.get('marketCap'),
            "analyst_price_target": info.get('targetMeanPrice') or info.get('targetMedianPrice') 
            
        }
    except Exception as e:
        print(f"⚠️ Cannot fetch fundamental data for {symbol}: {e}")
        return {}


def calculate_technical_indicators(df):
    """คำนวณค่าเทคนิคด้วย ta library"""
    try:
        if len(df) < 200:
            return None
        
        # Import ta library
        from ta.momentum import RSIIndicator
        from ta.trend import MACD, EMAIndicator
        from ta.volatility import BollingerBands
        
        # คำนวณ indicators
        # RSI
        rsi_indicator = RSIIndicator(close=df['Close'], window=14)
        df['rsi'] = rsi_indicator.rsi()
        
        # MACD
        macd_indicator = MACD(
            close=df['Close'],
            window_slow=26,
            window_fast=12,
            window_sign=9
        )
        df['macd'] = macd_indicator.macd()
        df['macd_signal'] = macd_indicator.macd_signal()
        
        # EMA
        ema_20 = EMAIndicator(close=df['Close'], window=20)
        df['ema_20'] = ema_20.ema_indicator()
        
        ema_50 = EMAIndicator(close=df['Close'], window=50)
        df['ema_50'] = ema_50.ema_indicator()
        
        ema_200 = EMAIndicator(close=df['Close'], window=200)
        df['ema_200'] = ema_200.ema_indicator()
        
        # Bollinger Bands
        bb_indicator = BollingerBands(close=df['Close'], window=20, window_dev=2)
        df['bb_upper'] = bb_indicator.bollinger_hband()
        df['bb_lower'] = bb_indicator.bollinger_lband()
        
        # ดึงค่าล่าสุด
        close = df['Close'].values

        # === Volume Analysis ===
        vol_series = df['Volume'].astype(float)
        vol_ma20   = vol_series.rolling(20).mean()
        vol_ratio  = (vol_series / vol_ma20 * 100).round(1)

        # OBV (On Balance Volume)
        obv = (vol_series * df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))).cumsum()

        # VWAP (ใช้ราคาปิดแทน intraday เพราะเป็น daily data)
        typical_price = (df['High'] + df['Low'] + df['Close']) / 3
        vwap = (typical_price * vol_series).cumsum() / vol_series.cumsum()

        # ATR (Average True Range, window=14) — ใช้คำนวณ Stop Loss
        from ta.volatility import AverageTrueRange
        atr_indicator = AverageTrueRange(
            high=df['High'], low=df['Low'], close=df['Close'], window=14
        )
        df['atr'] = atr_indicator.average_true_range()

        # Stochastic RSI (window=14)
        from ta.momentum import StochasticOscillator
        stoch = StochasticOscillator(
            high=df['High'], low=df['Low'], close=df['Close'], window=14, smooth_window=3
        )
        df['stoch_k'] = stoch.stoch()
        df['stoch_d'] = stoch.stoch_signal()

        return {
            "price":       float(close[-1]),
            "rsi":         float(df['rsi'].iloc[-1])         if pd.notna(df['rsi'].iloc[-1])         else None,
            "macd":        float(df['macd'].iloc[-1])        if pd.notna(df['macd'].iloc[-1])        else None,
            "macd_signal": float(df['macd_signal'].iloc[-1]) if pd.notna(df['macd_signal'].iloc[-1]) else None,
            "ema_20":      float(df['ema_20'].iloc[-1])      if pd.notna(df['ema_20'].iloc[-1])      else None,
            "ema_50":      float(df['ema_50'].iloc[-1])      if pd.notna(df['ema_50'].iloc[-1])      else None,
            "ema_200":     float(df['ema_200'].iloc[-1])     if pd.notna(df['ema_200'].iloc[-1])     else None,
            "bb_upper":    float(df['bb_upper'].iloc[-1])    if pd.notna(df['bb_upper'].iloc[-1])    else None,
            "bb_lower":    float(df['bb_lower'].iloc[-1])    if pd.notna(df['bb_lower'].iloc[-1])    else None,
            # === ใหม่ ===
            "volume":      float(vol_series.iloc[-1])        if pd.notna(vol_series.iloc[-1])        else None,
            "vol_ma20":    float(vol_ma20.iloc[-1])          if pd.notna(vol_ma20.iloc[-1])          else None,
            "vol_ratio":   float(vol_ratio.iloc[-1])         if pd.notna(vol_ratio.iloc[-1])         else None,
            "obv":         float(obv.iloc[-1])               if pd.notna(obv.iloc[-1])               else None,
            "vwap":        float(vwap.iloc[-1])              if pd.notna(vwap.iloc[-1])              else None,
            "atr":         float(df['atr'].iloc[-1])         if pd.notna(df['atr'].iloc[-1])         else None,
            "stoch_k":     float(df['stoch_k'].iloc[-1])     if pd.notna(df['stoch_k'].iloc[-1])     else None,
            "stoch_d":     float(df['stoch_d'].iloc[-1])     if pd.notna(df['stoch_d'].iloc[-1])     else None,
        }
 
        
    except Exception as e:
        print(f"❌ Error calculating indicators: {e}")
        import traceback
        traceback.print_exc()
        return None
         
def calculate_upside_pct(current_price, ema_200, ema_50=None, analyst_target=None):  # ✅ เพิ่ม analyst_target
    if not current_price:
        return None
    
    # ✅ ใช้ analyst price target จริงก่อน (แม่นยำกว่า EMA)
    if analyst_target and analyst_target > 0:
        return round(((analyst_target - current_price) / current_price) * 100, 2)
    
    # fallback เดิม — ไม่แตะ logic เดิมเลย
    if ema_200 and ema_200 > 0:
        return round(((ema_200 - current_price) / current_price) * 100, 2)
    
    if ema_50 and ema_50 > 0:
        return round(((ema_50 - current_price) / current_price) * 100, 2)
    
    return None         

def fetch_analyst_data(symbol):
    try:
        stock = yf.Ticker(symbol)
        
        # ✅ วิธีที่ 1: ใช้ get_recommendations() → ได้ตัวเลขตรงๆ
        rec_summary = stock.get_recommendations()
        if rec_summary is not None and not rec_summary.empty:
            latest = rec_summary.iloc[0]  # เดือนล่าสุด (0m)
            strong_buy = latest.get('strongBuy', 0)
            buy        = latest.get('buy', 0)
            hold       = latest.get('hold', 0)
            sell       = latest.get('sell', 0)
            strong_sell = latest.get('strongSell', 0)
            
            total = strong_buy + buy + hold + sell + strong_sell
            if total > 0:
                buy_pct = round(((strong_buy + buy) / total) * 100, 2)
                print(f"   📊 Analyst: SB={strong_buy} B={buy} H={hold} S={sell} SS={strong_sell} → Buy%={buy_pct}")
                return buy_pct
        
        # ✅ วิธีที่ 2: fallback ใช้ recommendations (upgrade/downgrade)
        recommendations = stock.recommendations
        if recommendations is not None and not recommendations.empty:
            recent = recommendations.tail(10)
            buy_grades = ['buy', 'strong buy', 'outperform', 'overweight']
            buy_count = 0
            
            for _, row in recent.iterrows():
                if row.get('toGrade'):
                    grade = str(row.get('toGrade')).lower().strip()
                elif row.get('To Grade'):
                    grade = str(row.get('To Grade')).lower().strip()
                elif row.get('action'):
                    grade = str(row.get('action')).lower().strip()
                else:
                    grade = ''
                
                if any(buy_word in grade for buy_word in buy_grades):
                    buy_count += 1
            
            total = len(recent)
            return round((buy_count / total) * 100, 2) if total > 0 else None
        
        # ✅ วิธีที่ 3: fallback ใช้ recommendationMean จาก info
        info = stock.info
        recommend_mean = info.get('recommendationMean')
        if recommend_mean:
            if recommend_mean <= 1.5:   return 90.0
            elif recommend_mean <= 2.0: return 75.0
            elif recommend_mean <= 2.5: return 60.0
            elif recommend_mean <= 3.0: return 40.0
            else:                       return 20.0
        
    except Exception as e:
        print(f"⚠️ Cannot fetch analyst data for {symbol}: {e}")
    
    return None
 
  

def fetch_macro_data():
    print("🔍 fetch_macro_data() called — NEW VERSION_030869") 
    """
    ดึงข้อมูล Macro: VIX, SPY, QQQ, XLK, Bond 10Y
    ไม่กระทบ logic เดิม — เรียกแยกเสมอ
    """
    try:
        macro_tickers = {
            "vix":   "^VIX",
            "spy":   "^GSPC",
            "qqq":   "QQQ",
            "xlk":   "XLK",
            "bond":  "^TNX",
        }
        result = {}  
        for key, ticker in macro_tickers.items():
            try:
                df = yf.download(ticker, period="5d", interval="1d", 
                                 progress=False, auto_adjust=True)
                
                print(f"   {ticker}: rows={len(df)}")
                
                # ✅ แก้ปัญหา yfinance ใหม่ที่ df["Close"] เป็น DataFrame
                close = df["Close"]
                if isinstance(close, pd.DataFrame):
                    close = close.iloc[:, 0]
                close = close.dropna()
                
                if len(close) >= 2:
                    val = float(close.iloc[-1])
                    chg = float((close.iloc[-1] / close.iloc[-2] - 1) * 100)
                    result[key]          = round(val, 2)
                    result[f"{key}_chg"] = round(chg, 2)
                elif len(close) == 1:
                    result[key]          = round(float(close.iloc[-1]), 2)
                    result[f"{key}_chg"] = None
                else:
                    result[key]          = None
                    result[f"{key}_chg"] = None
            except Exception as e:
                print(f"   ⚠️ {ticker} failed: {e}")
                result[key]          = None
                result[f"{key}_chg"] = None
                
        # สรุป Market Sentiment
        spy_chg = result.get("spy_chg") or 0
        qqq_chg = result.get("qqq_chg") or 0
        vix_val = result.get("vix") or 20
        avg_chg = (spy_chg + qqq_chg) / 2

        if vix_val > 30 or avg_chg < -1.5:
            result["market_sentiment"] = "Bearish"
        elif vix_val < 15 and avg_chg > 1.0:
            result["market_sentiment"] = "Bullish"
        else:
            result["market_sentiment"] = "Neutral"

        # VIX Risk Level
        if vix_val > 40:   result["vix_signal"] = "Panic"
        elif vix_val > 25: result["vix_signal"] = "Fear"
        elif vix_val > 15: result["vix_signal"] = "Caution"
        else:              result["vix_signal"] = "Calm"

        print(f"📊 Macro: VIX={result.get('vix')} ({result.get('vix_signal')}) | SPY={spy_chg:+.2f}% | QQQ={qqq_chg:+.2f}% | Sentiment={result.get('market_sentiment')}")
        return result

    except Exception as e:
        print(f"⚠️ fetch_macro_data failed: {e}")
        return {}
        
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
 

            # เพิ่มตรงนี้
            print(f"\n🔬 RAW tech_data keys: {list(tech_data.keys()) if tech_data else 'None'}")
            print(f"   volume   = {tech_data.get('volume') if tech_data else 'N/A'}")
            print(f"   vol_ratio= {tech_data.get('vol_ratio') if tech_data else 'N/A'}")
            print(f"   atr      = {tech_data.get('atr') if tech_data else 'N/A'}")
            print(f"   stoch_k  = {tech_data.get('stoch_k') if tech_data else 'N/A'}")
            
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
            score += 10        # ← แก้จาก technical_score เป็น score
        elif upside_pct > 10:
            score += 7
        elif upside_pct > 5:
            score += 4

    # Volume Confirmation (bonus ±5)
    vol_ratio = tech_data.get('vol_ratio')
    change    = tech_data.get('change_pct', 0) or 0

    if vol_ratio:
        if change > 0 and vol_ratio > 120:
            score += 5         # ← แก้จาก technical_score เป็น score
        elif change < 0 and vol_ratio > 150:
            score -= 5
        elif change < 0 and vol_ratio < 80:
            score += 2

    # Stochastic Confirmation (bonus ±3)
    stoch_k = tech_data.get('stoch_k')
    if stoch_k:
        if stoch_k < 20:
            score += 3         # ← แก้จาก technical_score เป็น score
        elif stoch_k > 80:
            score -= 3

    return score

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
    
    base_score = calculate_overall_score(
        symbol=symbol,
        tech_data=tech_data,
        fundamental_data=fundamental_data,
        news_sentiment=news_sentiment,
        category=category,
        market_cap=market_cap
    )
    # คำนวณความเสี่ยง
    risk_score = calculate_risk_score(tech_data, fundamental_data, market_cap)
    
    # ปรับ Score
    final_score = adjust_score_by_risk(base_score, risk_score)
    
    print(f"   Base Score: {base_score} | Risk: {risk_score} | Final: {final_score}")
    
    return final_score 


def generate_recommendation_advanced(overall_score, price, upside_pct, risk_score, category, tech_data=None):
    """
    สร้างคำแนะนำแบบละเอียด พิจารณาทั้ง Score + Risk + Upside + MACD + RSI + EMA 200
    
    🆕 Logic ใหม่:
    - ถ้า Buy Signal แต่ price < EMA 200 → เปลี่ยนเป็น "Alert Only" (สัญญาณเตือน)
    - แนะนำเปิด Pilot Position 5-10% แทน All-in
    """
    
    # ดึงข้อมูล Technical (ถ้าส่งมา)
    macd = tech_data.get('macd') if tech_data else None
    macd_signal = tech_data.get('macd_signal') if tech_data else None
    rsi = tech_data.get('rsi') if tech_data else None
    ema_200 = tech_data.get('ema_200') if tech_data else None
    
    # 🆕 ตัวแปรเพื่อเช็คสัญญาณ
    has_macd_crossover = False
    has_strong_rsi = False
    is_above_ema200 = False
    
    # 🆕 เช็ค MACD Crossover (MACD > Signal)
    if macd and macd_signal:
        has_macd_crossover = macd > macd_signal
    
    # 🆕 เช็ค RSI > 50 (แนวโน้มขาขึ้น)
    if rsi:
        has_strong_rsi = rsi > 50
    
    # 🆕 เช็คว่าราคาอยู่เหนือ EMA 200 หรือไม่
    if price and ema_200:
        is_above_ema200 = price > ema_200
    
    # 1. คำนวณ Confidence Level
    if risk_score < 20:
        confidence = "High"
    elif risk_score < 50:
        confidence = "Medium"
    else:
        confidence = "Low"
    
    # 2. กำหนด Recommendation (เพิ่มเงื่อนไข Alert Only)
    recommendation = None
    reason = None
    position_size = None  # 🆕 เพิ่มคำแนะนำขนาด position
    
    if overall_score >= 75 and risk_score < 50:
        # 🆕 เช็คว่ามี MACD Crossover + RSI > 50 หรือไม่
        if has_macd_crossover and has_strong_rsi:
            if is_above_ema200:
                # ✅ Strong Buy เต็มรูปแบบ
                recommendation = "Strong Buy"
                reason = f"สัญญาณดีเยี่ยม: MACD Crossover + RSI {rsi:.0f} + อยู่เหนือ EMA 200"
                position_size = "15-20% ของพอร์ต (แบ่ง 2-3 ครั้ง)"
            else:
                # ⚠️ Alert Only - ยังไม่ถึงจุดซื้อที่ดีที่สุด
                recommendation = "Alert Only"
                reason = f"สัญญาณเตือน: MACD Crossover + RSI {rsi:.0f} แต่ยังต่ำกว่า EMA 200"
                position_size = "5-10% Pilot Position (ชิมลาง)"
        else:
            # Buy ปกติ แต่ไม่มีสัญญาณครบ
            recommendation = "Buy"
            reason = f"คะแนนดี แต่รอ MACD Crossover + RSI > 50 จะดีกว่า"
            position_size = "10-15% ของพอร์ต"
        
    elif overall_score >= 60:
        if risk_score >= 60:
            recommendation = "Hold"
            reason = f"คะแนนดีแต่เสี่ยงสูง ({risk_score}/100) - รอให้ชัดเจนกว่านี้"
            position_size = "ไม่แนะนำเพิ่ม - ถือที่มีอยู่"
        else:
            # 🆕 เช็คเงื่อนไข MACD + RSI + EMA 200
            if has_macd_crossover and has_strong_rsi:
                if is_above_ema200:
                    recommendation = "Buy"
                    reason = f"สัญญาณดี: MACD Crossover + RSI {rsi:.0f} + อยู่เหนือ EMA 200"
                    position_size = "10-15% ของพอร์ต"
                else:
                    # ⚠️ Alert Only
                    recommendation = "Alert Only"
                    reason = f"สัญญาณเตือน: MACD Crossover + RSI {rsi:.0f} แต่ยังต่ำกว่า EMA 200"
                    position_size = "5-10% Pilot Position (ชิมลาง)"
            else:
                recommendation = "Buy"
                reason = f"โมเมนตัมดี แต่รอ MACD + RSI จะดีกว่า"
                position_size = "10-15% ของพอร์ต"
    
    elif overall_score >= 45:
        recommendation = "Hold"
        reason = "รอสัญญาณที่ชัดเจนกว่านี้"
        position_size = "ไม่แนะนำเพิ่ม"
    
    elif overall_score >= 30:
        recommendation = "Sell"
        reason = f"ผลงานอ่อนแอ พิจารณาลดสัดส่วน"
        position_size = "ขาย 30-50% ของ position"
    
    else:
        recommendation = "Strong Sell"
        reason = "ตัวชี้วัดทุกด้านอ่อนแอ"
        position_size = "ขายทั้งหมดหรือเกือบหมด"
    
 
    # 3. คำนวณ Price Target (ไม่เปลี่ยน)
    if upside_pct and upside_pct > 0:
        adjusted_upside = upside_pct * (1 - risk_score / 200)
        price_target = round(price * (1 + adjusted_upside / 100), 2)
    else:
        price_target = None

    # === ใหม่: ATR-based Stop Loss ===
    atr = tech_data.get('atr') if tech_data else None
    if atr and price:
        stop_loss = round(price - (atr * 2), 2)   # 2×ATR = standard
        if price_target:
            rr_ratio = round((price_target - price) / (price - stop_loss), 2) if price > stop_loss else 0
        else:
            rr_ratio = None
    else:
        stop_loss = None
        rr_ratio  = None
 
    
    # 4. เพิ่ม Time Horizon (ระยะเวลาที่แนะนำ)
    if category in ['Growth', 'Momentum']:
        time_horizon = "3-6 เดือน"
    elif category in ['Value', 'Dividend']:
        time_horizon = "6-12 เดือน"
    else:
        time_horizon = "6 เดือน"
    
    # 🆕 5. เพิ่มคำเตือนพิเศษสำหรับ Alert Only
    if recommendation == "Alert Only":
        warning = "⚠️ ห้าม All-in! ใช้เงินเล็กน้อยชิมลางก่อน รอให้ราคาทะลุ EMA 200 จึงเพิ่มเต็มที่"
    else:
        warning = None
    
    # 🆕 6. Return ค่าทั้งหมด (แก้ไขตรงนี้ - ไม่ให้มี recommendation_data.get)
    return {
        'recommendation': recommendation,
        'reason': reason,
        'confidence': confidence,
        'price_target': price_target,
        'time_horizon': time_horizon,
        'risk_level': 'High' if risk_score >= 60 else 'Medium' if risk_score >= 30 else 'Low',
        'position_size': position_size,  # ✅ ใช้ตัวแปร position_size โดยตรง
        'warning': warning,  # ✅ ใช้ตัวแปร warning โดยตรง
        'technical_signals': {  # 🆕 รายละเอียดสัญญาณ
            'macd_crossover': has_macd_crossover,
            'rsi_above_50': has_strong_rsi,
            'above_ema200': is_above_ema200
        },
        'stop_loss': stop_loss,
        'risk_reward': rr_ratio,
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
    
    # คำนวณคะแนนแต่ละส่วน
    technical_score   = calculate_technical_score(tech_data)       # 0-40
    fundamental_score = calculate_fundamental_score(fundamental_data)  # 0-30
    sentiment_score   = calculate_sentiment_score(news_sentiment, tech_data)  # 0-30
    
    # Dynamic Weighting
    tech_w, fund_w, sent_w = get_scoring_weights(symbol, category, market_cap)
    
    final_score = (
        (technical_score   / 40) * 100 * tech_w +
        (fundamental_score / 30) * 100 * fund_w +
        (sentiment_score   / 30) * 100 * sent_w
    )

    # Macro Penalty (ไม่กระทบ weight เดิม)
    macro        = tech_data.get('macro') or {}
    vix_val      = macro.get('vix') or 0
    market_sent  = macro.get('market_sentiment') or 'Neutral'

    if vix_val > 35:
        final_score *= 0.80   # Panic → ลด 20%
    elif vix_val > 25:
        final_score *= 0.90   # Fear  → ลด 10%

    if market_sent == 'Bearish':
        final_score *= 0.95   # ตลาดลง → ลด 5%

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



def format_telegram_summary(stats, total_stocks, start_time):
    """สร้างข้อความสรุปสำหรับ Telegram พร้อมแสดง Symbol ของ Strong Buy/Buy"""
    
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
- 🟢 Strong Buy: {stats['strong_buy']}"""
    
    # แสดง Symbol ของ Strong Buy (ถ้ามี)
    if stats['strong_buy'] > 0 and stats.get('strong_buy_symbols'):
        symbols_str = ", ".join(stats['strong_buy_symbols'])
        message += f" ({symbols_str})"
    
    message += f"\n- 🟢 Buy: {stats['buy']}"
    
    # แสดง Symbol ของ Buy (ถ้ามี)
    if stats['buy'] > 0 and stats.get('buy_symbols'):
        symbols_str = ", ".join(stats['buy_symbols'])
        message += f" ({symbols_str})"
    
    message += f"""
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

def get_comparative_insights(symbol):
    """
    ดึงข้อมูลเปรียบเทียบ: สัปดาห์ที่แล้ว, ค่าเฉลี่ย 30 วัน
    
    Returns: {
        'week_ago_change': float,
        'avg_30d_score': float,
        'current_vs_30d': float,
        'price_52w_high': float,
        'price_52w_low': float
    }
    """
    try:
        # 1. ดึงข้อมูล 7 วันที่แล้ว
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()
        week_snapshot = supabase.table("stock_snapshots")\
            .select("price")\
            .eq("symbol", symbol)\
            .lte("recorded_at", week_ago)\
            .order("recorded_at", desc=True)\
            .limit(1)\
            .execute()
        
        # 2. ดึงข้อมูล 30 วันล่าสุด
        month_ago = (datetime.now() - timedelta(days=30)).isoformat()
        month_snapshots = supabase.table("stock_snapshots")\
            .select("price")\
            .eq("symbol", symbol)\
            .gte("recorded_at", month_ago)\
            .execute()
        
        # 3. ดึง Score เฉลี่ย 30 วัน
        month_predictions = supabase.table("ai_predictions")\
            .select("overall_score")\
            .eq("symbol", symbol)\
            .gte("created_at", month_ago)\
            .execute()
        
        # 4. ดึงข้อมูลปัจจุบัน
        current_snapshot = supabase.table("stock_snapshots")\
            .select("price")\
            .eq("symbol", symbol)\
            .order("recorded_at", desc=True)\
            .limit(1)\
            .execute()
        
        current_prediction = supabase.table("ai_predictions")\
            .select("overall_score")\
            .eq("symbol", symbol)\
            .order("created_at", desc=True)\
            .limit(1)\
            .execute()
        
        # 5. คำนวณค่าต่างๆ
        result = {
            'week_ago_change': None,
            'avg_30d_score': None,
            'current_vs_30d': None,
            'price_52w_high': None,
            'price_52w_low': None
        }
        
        # เปลี่ยนแปลงจากสัปดาห์ที่แล้ว
        if week_snapshot.data and current_snapshot.data:
            week_price = week_snapshot.data[0]['price']
            current_price = current_snapshot.data[0]['price']
            result['week_ago_change'] = round(((current_price - week_price) / week_price) * 100, 2)
        
        # Score เฉลี่ย 30 วัน
        if month_predictions.data:
            scores = [p['overall_score'] for p in month_predictions.data if p.get('overall_score')]
            if scores:
                result['avg_30d_score'] = round(sum(scores) / len(scores), 1)
        
        # เปรียบเทียบ Score ปัจจุบันกับค่าเฉลี่ย
        if current_prediction.data and result['avg_30d_score']:
            current_score = current_prediction.data[0]['overall_score']
            result['current_vs_30d'] = round(current_score - result['avg_30d_score'], 1)
        
        # ราคาสูง/ต่ำสุดใน 30 วัน (ใช้แทน 52 สัปดาห์)
        if month_snapshots.data:
            prices = [s['price'] for s in month_snapshots.data if s.get('price')]
            if prices:
                result['price_52w_high'] = round(max(prices), 2)
                result['price_52w_low'] = round(min(prices), 2)
        
        return result
        
    except Exception as e:
        print(f"⚠️ Error getting comparative insights for {symbol}: {e}")
        return {
            'week_ago_change': None,
            'avg_30d_score': None,
            'current_vs_30d': None,
            'price_52w_high': None,
            'price_52w_low': None
        }


def get_top_movers():
    """
    หาหุ้นที่เปลี่ยนแปลงมากที่สุด (ทั้งขึ้นและลง)
    
    Returns: {
        'top_gainers': [...],
        'top_losers': [...]
    }
    """
    try:
        # ดึง snapshot ล่าสุดทั้งหมด (ภายใน 15 นาที)
        cutoff_time = (datetime.now() - timedelta(minutes=15)).isoformat()
        
        snapshots = supabase.table("stock_snapshots")\
            .select("symbol, price, change_pct")\
            .gte("recorded_at", cutoff_time)\
            .order("recorded_at", desc=True)\
            .execute()
        
        if not snapshots.data:
            return {'top_gainers': [], 'top_losers': []}
        
        # กรองหุ้นซ้ำ (เอาล่าสุดสุด)
        unique_stocks = {}
        for snap in snapshots.data:
            symbol = snap['symbol']
            if symbol not in unique_stocks:
                unique_stocks[symbol] = snap
        
        stocks_list = list(unique_stocks.values())
        
        # เรียงตาม change_pct
        sorted_stocks = sorted(stocks_list, key=lambda x: x.get('change_pct', 0), reverse=True)
        
        # Top 3 gainers และ losers
        top_gainers = sorted_stocks[:3]
        top_losers = sorted_stocks[-3:]
        top_losers.reverse()  # ให้ลงมากสุดอยู่ก่อน
        
        return {
            'top_gainers': top_gainers,
            'top_losers': top_losers
        }
        
    except Exception as e:
        print(f"⚠️ Error getting top movers: {e}")
        return {'top_gainers': [], 'top_losers': []}

def format_beginner_alert(opportunities, market_data):
    """
    Alert สำหรับ Beginner: เน้น DCA + ข้อมูลพื้นฐาน
    """
    if not opportunities:
        return None
    
    message = "🌱 <b>มือใหม่เริ่มต้น - โอกาส DCA</b>\n\n"
    message += "💡 <b>คำแนะนำ:</b> DCA (Dollar Cost Averaging) คือการซื้อเป็นงวดๆ เพื่อลดความเสี่ยง\n\n"
    
    for i, opp in enumerate(opportunities[:2], 1):  # แค่ 2 อันดับแรก
        message += f"{i}️⃣ <b>{opp['symbol']}</b>\n"
        message += f"   💰 ราคา: ${opp['price']:.2f}\n"
        message += f"   📉 ลง: {abs(opp['change_pct']):.1f}%\n"
        message += f"   ⭐ คะแนน: {opp['overall_score']}/100\n"
        
        # คำอธิบายง่ายๆ
        if opp['overall_score'] >= 70:
            message += f"   ✅ คุณภาพดี เหมาะลงทุนระยะยาว\n"
        
        # แนะนำการซื้อ
        message += f"   💵 แนะนำ: ซื้อ 1/3 ของเงินที่ตั้งไว้\n"
        message += "\n"
    
    message += "📚 <b>เคล็ดลับ:</b>\n"
    message += "• แบ่งเงินซื้อ 3 ครั้ง ห่างกัน 1-2 สัปดาห์\n"
    message += "• ไม่ต้องเร่งซื้อทันที รอให้ราคาเสถียร\n"
    message += "• ถือระยะยาว 6-12 เดือนขึ้นไป\n"
    
    return message


def format_intermediate_alert(opportunities, market_data):
    """
    Alert สำหรับ Intermediate: Technical + Fundamental
    """
    if not opportunities:
        return None
    
    message = "📊 <b>นักลงทุนระดับกลาง - การวิเคราะห์เชิง Technical</b>\n\n"
    
    # แสดง Market Overview
    message += f"🌍 <b>ภาพรวมตลาด:</b>\n"
    message += f"S&P 500: {market_data['SPY']:+.2f}% | NASDAQ: {market_data['QQQ']:+.2f}%\n\n"
    
    for i, opp in enumerate(opportunities[:3], 1):
        message += f"{i}️⃣ <b>{opp['symbol']}</b> ${opp['price']:.2f} ({opp['change_pct']:+.1f}%)\n"
        
        # Technical Indicators
        message += f"   📈 Technical:\n"
        if opp.get('rsi'):
            rsi_status = "Oversold" if opp['rsi'] < 30 else "Neutral"
            message += f"      • RSI: {opp['rsi']:.0f} ({rsi_status})\n"
        
        if opp.get('below_ma_pct'):
            message += f"      • Below MA20: {opp['below_ma_pct']:.1f}%\n"
        
        # Score + Risk
        message += f"   💎 Score: {opp['overall_score']}/100"
        if opp.get('confidence'):
            message += f" | Confidence: {opp['confidence']}\n"
        else:
            message += "\n"
        
        if opp.get('risk_score'):
            risk_level = "Low" if opp['risk_score'] < 30 else "Medium" if opp['risk_score'] < 60 else "High"
            message += f"   ⚠️ Risk: {risk_level} ({opp['risk_score']}/100)\n"
        
        # Entry Strategy
        if opp.get('price_target'):
            upside = ((opp['price_target'] - opp['price']) / opp['price']) * 100
            message += f"   🎯 Target: ${opp['price_target']:.2f} (+{upside:.1f}%)\n"
        
        message += "\n"
    
    message += "💡 <b>Strategy:</b> ใช้ Limit Order + Stop Loss ป้องกันความเสี่ยง\n"
    
    return message


def format_advanced_alert(opportunities, market_data, top_movers):
    """
    Alert สำหรับ Advanced: รวม Options flow, Volume analysis, Sector trends
    """
    if not opportunities:
        return None
    
    message = "🔥 <b>นักลงทุนขั้นสูง - Advanced Analysis</b>\n\n"
    
    # Market Overview with Sentiment
    avg_change = (market_data['SPY'] + market_data['QQQ']) / 2
    sentiment = "Bearish 🐻" if avg_change < -1 else "Bullish 🐂" if avg_change > 1 else "Neutral ⚖️"
    
    message += f"🌍 <b>Market Sentiment:</b> {sentiment}\n"
    message += f"SPY: {market_data['SPY']:+.2f}% | QQQ: {market_data['QQQ']:+.2f}%\n\n"
    
    # Top Opportunities with detailed metrics
    message += "🎯 <b>Top Setups:</b>\n\n"
    
    for i, opp in enumerate(opportunities[:3], 1):
        message += f"{i}️⃣ <b>{opp['symbol']}</b> ${opp['price']:.2f} ({opp['change_pct']:+.1f}%)\n"
        
        # Multi-factor Score
        message += f"   📊 Metrics:\n"
        message += f"      • Overall Score: {opp['overall_score']}/100\n"
        
        if opp.get('rsi'):
            message += f"      • RSI: {opp['rsi']:.0f}"
            if opp['rsi'] < 30:
                message += " ⚡ Strong Oversold\n"
            elif opp['rsi'] < 40:
                message += " 📉 Oversold\n"
            else:
                message += "\n"
        
        if opp.get('below_ma_pct'):
            message += f"      • Distance from MA20: {opp['below_ma_pct']:.1f}%\n"
        
        # Risk-Reward Analysis
        if opp.get('price_target') and opp.get('risk_score'):
            upside = ((opp['price_target'] - opp['price']) / opp['price']) * 100
            risk_reward = upside / max(opp['risk_score'], 1) * 10
            message += f"   💰 Risk/Reward: {risk_reward:.2f}:1\n"
            message += f"      • Upside: +{upside:.1f}% | Risk: {opp['risk_score']}/100\n"
        
        # Entry/Exit Strategy
        if opp.get('alert_type') == 'Gap Down':
            message += f"   🎯 Strategy: Wait for bounce confirmation (15-30min)\n"
        elif opp.get('alert_type') == 'DCA Setup':
            message += f"   🎯 Strategy: Scale in 2-3 positions, 5-7 days apart\n"
        
        message += "\n"
    
    # Top Movers Section
    if top_movers.get('top_gainers') or top_movers.get('top_losers'):
        message += "📈 <b>Market Movers:</b>\n"
        
        if top_movers.get('top_gainers'):
            message += "🟢 Gainers: "
            gainers_str = " | ".join([f"{g['symbol']} +{g['change_pct']:.1f}%" 
                                     for g in top_movers['top_gainers'][:3]])
            message += gainers_str + "\n"
        
        if top_movers.get('top_losers'):
            message += "🔴 Losers: "
            losers_str = " | ".join([f"{l['symbol']} {l['change_pct']:.1f}%" 
                                    for l in top_movers['top_losers'][:3]])
            message += losers_str + "\n"
    
    message += "\n💡 <b>Pro Tip:</b> Monitor unusual volume + check options flow for institutional activity\n"
    
    return message
    
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
    """สร้างข้อความตามช่วงเวลา พร้อม Comparative Insights"""
    
    now = datetime.now()
    
    # 1. ดึงข้อมูล Top Movers
    top_movers = get_top_movers()
    
    # 2. ถ้าไม่มีโอกาส แต่มีการเก็บข้อมูล ให้ส่งสรุปสั้น ๆ
    if not opportunities:
        message = f"{session_name}\n\n"
        message += f"📊 <b>ภาพรวมตลาด</b>\n"
        message += f"S&P 500: <b>{market_data['SPY']:+.1f}%</b> | "
        message += f"NASDAQ: <b>{market_data['QQQ']:+.1f}%</b>\n\n"
        
        # แสดง Top Movers แม้ไม่มีโอกาส
        if top_movers.get('top_gainers') or top_movers.get('top_losers'):
            message += "📈 <b>Market Movers:</b>\n"
            
            if top_movers.get('top_gainers'):
                message += "🟢 " + " | ".join([f"{g['symbol']} +{g['change_pct']:.1f}%" 
                                               for g in top_movers['top_gainers'][:3]]) + "\n"
            
            if top_movers.get('top_losers'):
                message += "🔴 " + " | ".join([f"{l['symbol']} {l['change_pct']:.1f}%" 
                                              for l in top_movers['top_losers'][:3]]) + "\n"
            message += "\n"
        
        if session_type == "after_close":
            message += "ℹ️ ไม่มีหุ้นที่ตรงเงื่อนไข DCA วันนี้"
        else:
            message += "ℹ️ ยังไม่มีสัญญาณที่ชัดเจน"
        
        message += f"\n⏰ {now.strftime('%H:%M น.')}"
        return message
    
    # 3. มีโอกาส → สร้างข้อความเต็ม
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
    
    # แสดงหุ้น (3 อันดับแรก) พร้อม Comparative Insights
    for i, opp in enumerate(opportunities[:3], 1):
        message += f"{i}️⃣ <b>{opp['symbol']}</b> ${opp['price']:.2f} "
        message += f"<b>({opp['change_pct']:+.1f}%)</b>\n"
        
        # Score + Confidence
        message += f"   💎 Score {opp['overall_score']}/100"
        if opp.get('confidence'):
            message += f" | 🎯 {opp['confidence']}"
        message += "\n"
        
        # 🆕 Comparative Insights
        insights = get_comparative_insights(opp['symbol'])
        
        if insights.get('week_ago_change') is not None:
            week_trend = "📈" if insights['week_ago_change'] > 0 else "📉"
            message += f"   {week_trend} เทียบ 7 วันก่อน: {insights['week_ago_change']:+.1f}%\n"
        
        if insights.get('current_vs_30d') is not None:
            score_trend = "⬆️" if insights['current_vs_30d'] > 0 else "⬇️"
            message += f"   {score_trend} Score vs เฉลี่ย 30d: {insights['current_vs_30d']:+.1f} แต้ม\n"
        
        if insights.get('price_52w_high') and insights.get('price_52w_low'):
            current = opp['price']
            high = insights['price_52w_high']
            low = insights['price_52w_low']
            
            # คำนวณตำแหน่งราคาใน range
            price_position = ((current - low) / (high - low)) * 100 if high != low else 50
            
            message += f"   📊 ช่วง 30d: ${low:.2f} - ${high:.2f} "
            
            if price_position < 30:
                message += "(ใกล้จุดต่ำ 🟢)\n"
            elif price_position > 70:
                message += "(ใกล้จุดสูง 🔴)\n"
            else:
                message += "(กลางๆ 🟡)\n"
        
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
    
    # 🆕 แสดง Top Movers ท้ายข้อความ
    if top_movers.get('top_gainers') or top_movers.get('top_losers'):
        message += "📊 <b>หุ้นที่เปลี่ยนแปลงมากที่สุดวันนี้:</b>\n"
        
        if top_movers.get('top_gainers'):
            message += "🟢 ขึ้นสูงสุด: "
            gainers_str = " | ".join([f"{g['symbol']} +{g['change_pct']:.1f}%" 
                                     for g in top_movers['top_gainers'][:3]])
            message += gainers_str + "\n"
        
        if top_movers.get('top_losers'):
            message += "🔴 ลงต่ำสุด: "
            losers_str = " | ".join([f"{l['symbol']} {l['change_pct']:.1f}%" 
                                    for l in top_movers['top_losers'][:3]])
            message += losers_str + "\n"
        
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


async def send_market_alert_after_collection():
    """
    ส่ง Alert หลังเก็บข้อมูลทุกครั้ง - ปรับข้อความตามช่วงเวลา
    """
    
    print(f"\n{'='*60}")
    print("📱 Generating Market Alert...")
    print(f"{'='*60}\n")
    
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
    message = format_alert_by_session(
        opportunities, 
        market_data, 
        session_type, 
        session_name
    )
    
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
  



async def analyze_single_stock(symbol):
    """
    วิเคราะห์หุ้นตัวเดียวและคืนข้อมูลครบถ้วน
    
    Returns: dict with analysis results or None if failed
    """
    try:
        print(f"\n{'='*60}")
        print(f"🔍 Analyzing: {symbol}")
        print(f"{'='*60}")
        
        # 1. ดึงข้อมูล Technical
        data = await fetch_data_waterfall(symbol)
        
        if not data:
            return {
                'success': False,
                'error': f"❌ Cannot fetch data for {symbol}"
            }
        
        # 2. ดึง Market Cap + Fundamental (ถ้าไม่ใช่ ETF)
        market_cap = None
        fundamental_data = None
        category = 'Core'  # default
        
        # ตรวจสอบ category จาก stock_master
        try:
            stock_info = supabase.table("stock_master")\
                .select("category")\
                .eq("symbol", symbol)\
                .execute()
            
            if stock_info.data:
                category = stock_info.data[0].get('category', 'Core')
        except:
            pass
        
        if category != 'ETF':
            try:
                stock = yf.Ticker(symbol)
                info = stock.info
                
                market_cap = info.get('marketCap')
             
                pe_ratio   = info.get('trailingPE')
                eps_growth = info.get('earningsGrowth', 0) * 100 if info.get('earningsGrowth') else None
                peg_ratio  = info.get('pegRatio') or info.get('trailingPegRatio')
                
                # ✅ fallback คำนวณเองถ้าไม่มีค่า
                if not peg_ratio and pe_ratio and eps_growth and eps_growth > 0:
                    peg_ratio = round(pe_ratio / eps_growth, 2)
                
                fundamental_data = {
                    "pe_ratio": pe_ratio,
                    "pe_ratio_forward": info.get('forwardPE'),
                    "peg_ratio": peg_ratio,
                    "eps_growth_pct": eps_growth,
                    "market_cap": market_cap,
                    "analyst_price_target": info.get('targetMeanPrice') or info.get('targetMedianPrice')
                }
         
            
            except Exception as e:
                print(f"⚠️ Could not fetch fundamental data: {e}")
        
        # 3. คำนวณ Upside
        _analyst_target = fundamental_data.get("analyst_price_target") if fundamental_data else None  # ✅
        upside_pct = calculate_upside_pct(
            data.get("price"), 
            data.get("ema_200"),
            data.get("ema_50"),
            analyst_target=_analyst_target  # ✅ ส่งเข้าไปเพิ่ม
        )
        
        # 4. ดึง Analyst & Sentiment (ถ้าไม่ใช่ ETF)
        analyst_pct = None if category == 'ETF' else fetch_analyst_data(symbol)
        sentiment = None if category == 'ETF' else fetch_sentiment_score(symbol)
        
        # 5. ดึงข่าว (ถ้าไม่ใช่ ETF)
        news_records = []
        news_sentiment_advanced = None
        
        if category != 'ETF':
            print(f"📰 Fetching news for {symbol}...")
            news_records = fetch_news_data(symbol)
            
            if news_records:
                sentiment_scores = []
                for news in news_records:
                    if 'calculate_news_sentiment_advanced' in globals():
                        adv_sentiment = calculate_news_sentiment_advanced(
                            news.get('title', ''),
                            news.get('summary', '')
                        )
                        sentiment_scores.append(adv_sentiment)
                
                if sentiment_scores:
                    news_sentiment_advanced = round(sum(sentiment_scores) / len(sentiment_scores), 2)
        
        # 6. คำนวณ AI Prediction
        macro_data = fetch_macro_data()
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
            'analyst_buy_pct': analyst_pct,
            'volume':         data.get('volume'),
            'vol_ma20':       data.get('vol_ma20'),
            'vol_ratio':      data.get('vol_ratio'),
            'obv':            data.get('obv'),
            'vwap':           data.get('vwap'),
            'atr':            data.get('atr'),
            'stoch_k':        data.get('stoch_k'),
            'stoch_d':        data.get('stoch_d'),
            'change_pct':     data.get('change_pct'),
            'macro':          macro_data,
        }
        
        final_sentiment = news_sentiment_advanced if news_sentiment_advanced is not None else sentiment
        
        # คำนวณ Score
        if 'calculate_overall_score_with_risk' in globals():
            overall_score = calculate_overall_score_with_risk(
                symbol=symbol,
                tech_data=tech_data_full,
                fundamental_data=fundamental_data,
                news_sentiment=final_sentiment,
                category=category,
                market_cap=market_cap
            )
            risk_score = calculate_risk_score(tech_data_full, fundamental_data, market_cap)
        else:
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
            recommendation, reason, price_target = generate_recommendation(
                overall_score=overall_score,
                price=data.get('price'),
                upside_pct=upside_pct
            )
            confidence = None
            time_horizon = None
        
        # 7. ดึง Comparative Insights
        insights = get_comparative_insights(symbol) if 'get_comparative_insights' in globals() else {}
        
        # 8. Return ผลลัพธ์
        return {
            'success': True,
            'symbol': symbol,
            'category': category,
            'price': data.get('price'),
            'change_pct': data.get('change_pct'),
            'rsi': data.get('rsi'),
            'macd': data.get('macd'),
            'macd_signal': data.get('macd_signal'),
            'ema_20': data.get('ema_20'),
            'ema_50': data.get('ema_50'),
            'ema_200': data.get('ema_200'),
            'upside_pct': upside_pct,
            'analyst_buy_pct': analyst_pct,
            'sentiment_score': final_sentiment,
            'news_count': len(news_records),
            'news_sample': news_records[0] if news_records else None,
            'news_records': news_records[:3],  # ✅ เพิ่มบรรทัดนี้ - ส่ง 3 ข่าวแรก
            'overall_score': overall_score,
            'risk_score': risk_score,
            'recommendation': recommendation,
            'reason': reason,
            'price_target': price_target,
            'confidence': confidence,
            'time_horizon': time_horizon,
            'position_size': recommendation_data.get('position_size') if isinstance(recommendation_data, dict) else None,
            'warning': recommendation_data.get('warning') if isinstance(recommendation_data, dict) else None,
            'technical_signals': recommendation_data.get('technical_signals') if isinstance(recommendation_data, dict) else {},
            'market_cap': market_cap,
            'fundamental': fundamental_data,
            'insights': insights
        }
        
    except Exception as e:
        print(f"❌ Error analyzing {symbol}: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': f"❌ Error: {str(e)}"
        }



def format_stock_analysis_message(result):
    """
    จัดรูปแบบข้อความวิเคราะห์หุ้นสำหรับ Telegram (ภาษาไทย + ละเอียด)
    """
    if not result.get('success'):
        return result.get('error', '❌ การวิเคราะห์ล้มเหลว')
    
    symbol = result['symbol']
    
    # ========================================
    # Header + ราคา
    # ========================================
    message = f"📊 <b>การวิเคราะห์: {symbol}</b>"
    if result.get('category'):
        category_th = {
            'Core': 'หุ้นหลัก',
            'Growth': 'หุ้นเติบโต', 
            'Value': 'หุ้นมูลค่า',
            'Dividend': 'หุ้นปันผล',
            'ETF': 'กองทุนรวม',
            'Momentum': 'หุ้นโมเมนตัม'
        }
        cat_display = category_th.get(result['category'], result['category'])
        message += f" ({cat_display})"
    message += "\n\n"
    
    # ราคาและการเปลี่ยนแปลง
    message += f"💰 <b>ราคาปัจจุบัน:</b> ${result['price']:.2f}"
    if result.get('change_pct') is not None:
        change = result['change_pct']
        if change > 0:
            change_emoji = "🟢"
            change_text = "เพิ่มขึ้น"
        elif change < 0:
            change_emoji = "🔴"
            change_text = "ลดลง"
        else:
            change_emoji = "⚪"
            change_text = "ไม่เปลี่ยนแปลง"
        
        message += f" {change_emoji} {change_text} {abs(change):.2f}%"
    message += "\n\n"
    
    # ========================================
    # ตัวชี้วัดทางเทคนิค
    # ========================================
    message += "📈 <b>ตัวชี้วัดทางเทคนิค:</b>\n"
    
    # RSI
    if result.get('rsi'):
        rsi = result['rsi']
        message += f"   • RSI (14): {rsi:.1f}"
        
        if rsi < 30:
            message += " → <b>ขายเกิน</b> 🟢 (โอกาสซื้อ)\n"
        elif rsi < 40:
            message += " → ขายมาก 🟢\n"
        elif rsi < 50:
            message += " → เป็นกลางแนวลง 🟡\n"
        elif rsi < 60:
            message += " → เป็นกลางแนวขึ้น 🟡\n"
        elif rsi < 70:
            message += " → ซื้อมาก 🟠\n"
        else:
            message += " → <b>ซื้อเกิน</b> 🔴 (ระวังปรับฐาน)\n"
    
    # MACD
    if result.get('macd') and result.get('macd_signal'):
        macd = result['macd']
        signal = result['macd_signal']
        
        if macd > signal:
            message += f"   • MACD: <b>สัญญาณขาขึ้น</b> 🟢 (แนวโน้มดี)\n"
        else:
            message += f"   • MACD: <b>สัญญาณขาลง</b> 🔴 (แนวโน้มอ่อนแอ)\n"
    
    # Upside Potential
    if result.get('upside_pct'):
        upside = result['upside_pct']
        if upside > 0:
            message += f"   • โอกาสเพิ่มขึ้น: <b>+{upside:.1f}%</b> 📈\n"
        else:
            message += f"   • ระยะห่างจากเป้า: <b>{upside:.1f}%</b> 📉\n"
    
    # ========================================
    # ข้อมูลเปรียบเทียบ
    # ========================================
    insights = result.get('insights', {})
    
    if any(insights.values()):
        message += "\n📊 <b>การเปรียบเทียบ:</b>\n"
        
        # เปลี่ยนแปลง 7 วัน
        if insights.get('week_ago_change') is not None:
            week_change = insights['week_ago_change']
            if week_change > 0:
                message += f"   • 7 วันที่แล้ว: 📈 เพิ่มขึ้น <b>+{week_change:.1f}%</b>\n"
            else:
                message += f"   • 7 วันที่แล้ว: 📉 ลดลง <b>{week_change:.1f}%</b>\n"
        
        # Score เทียบค่าเฉลี่ย
        if insights.get('current_vs_30d') is not None:
            score_diff = insights['current_vs_30d']
            if score_diff > 0:
                message += f"   • คะแนนวันนี้ vs เฉลี่ย 30 วัน: ⬆️ สูงกว่า <b>+{score_diff:.1f}</b> แต้ม\n"
            else:
                message += f"   • คะแนนวันนี้ vs เฉลี่ย 30 วัน: ⬇️ ต่ำกว่า <b>{score_diff:.1f}</b> แต้ม\n"
        
        # ช่วงราคา 30 วัน
        if insights.get('price_52w_high') and insights.get('price_52w_low'):
            high = insights['price_52w_high']
            low = insights['price_52w_low']
            current = result['price']
            
            # คำนวณตำแหน่ง
            if high != low:
                position = ((current - low) / (high - low)) * 100
            else:
                position = 50
            
            message += f"   • ช่วงราคา 30 วัน: ${low:.2f} - ${high:.2f} "
            
            if position < 30:
                message += "→ ใกล้จุดต่ำสุด 🟢\n"
            elif position > 70:
                message += "→ ใกล้จุดสูงสุด 🔴\n"
            else:
                message += "→ กลางช่วง 🟡\n"
    
    # ========================================
    # การวิเคราะห์ของ AI
    # ========================================
    message += "\n🤖 <b>การวิเคราะห์จาก AI:</b>\n"
    message += f"   • คะแนนรวม: <b>{result['overall_score']}/100</b>\n"
    
    # คำแนะนำ
   


    rec = result['recommendation']
    rec_th = {
        'Strong Buy': '🟢 <b>แนะนำซื้อเข้ม</b>',
        'Buy': '🟢 แนะนำซื้อ',
        'Hold': '🟡 แนะนำถือ',
        'Sell': '🔴 แนะนำขาย',
        'Strong Sell': '🔴 <b>แนะนำขายเร่งด่วน</b>',
        'Alert Only': '⚠️ <b>สัญญาณเตือน (ยังไม่ถึงจุดซื้อ)</b>'  # 🆕
    }
    message += f"   • คำแนะนำ: {rec_th.get(rec, rec)}\n"
    
    # 🆕 แสดง Position Size (ขนาดการลงทุนแนะนำ)
    if result.get('position_size'):
        message += f"   • ขนาดลงทุนแนะนำ: <b>{result['position_size']}</b>\n"
    
    # 🆕 แสดงคำเตือนพิเศษ (สำหรับ Alert Only)
    if result.get('warning'):
        message += f"\n   ⚠️ <i>{result['warning']}</i>\n"

    
    # Confidence
    if result.get('confidence'):
        conf = result['confidence']
        conf_th = {
            'High': '🔥 สูง',
            'Medium': '📊 ปานกลาง',
            'Low': '⚠️ ต่ำ'
        }
        message += f"   • ความมั่นใจ: {conf_th.get(conf, conf)}\n"
    
    # ความเสี่ยง
    if result.get('risk_score'):
        risk = result['risk_score']
        if risk < 30:
            risk_level = "ต่ำ 🟢"
        elif risk < 60:
            risk_level = "ปานกลาง 🟡"
        else:
            risk_level = "สูง 🔴"
        message += f"   • ระดับความเสี่ยง: {risk_level} ({risk}/100)\n"
    
    # เหตุผล
    reason = result['reason']
    reason_th = {
        'Excellent signals with high risk': 'สัญญาณดีเยี่ยม แต่มีความเสี่ยงสูง',
        'Excellent signals with medium risk': 'สัญญาณดีเยี่ยม ความเสี่ยงปานกลาง',
        'Excellent signals with low risk': 'สัญญาณดีเยี่ยม ความเสี่ยงต่ำ',
        'Good score but high risk': 'คะแนนดี แต่ความเสี่ยงสูง',
        'Positive momentum with high risk': 'โมเมนตัมดี แต่มีความเสี่ยงสูง',
        'Positive momentum with medium risk': 'โมเมนตัมดี ความเสี่ยงปานกลาง',
        'Positive momentum with low risk': 'โมเมนตัมดี ความเสี่ยงต่ำ',
        'Wait for clearer signals': 'รอสัญญาณที่ชัดเจนกว่านี้',
        'Weak performance, consider reducing position': 'ผลงานอ่อนแอ พิจารณาลดสัดส่วน',
        'Poor metrics across the board': 'ตัวชี้วัดทุกด้านอ่อนแอ'
    }
    message += f"   • เหตุผล: {reason_th.get(reason, reason)}\n"
    
    # ราคาเป้าหมาย
    if result.get('price_target'):
        target = result['price_target']
        upside_to_target = ((target - result['price']) / result['price']) * 100
        message += f"   • ราคาเป้าหมาย: ${target:.2f} (+{upside_to_target:.1f}%)\n"
    
    


    # ระยะเวลา
    if result.get('time_horizon'):
        horizon = result['time_horizon']
        horizon_th = {
            '3-6 months': '3-6 เดือน',
            '6-12 months': '6-12 เดือน',
            '6 months': '6 เดือน',
            '3-6 เดือน': '3-6 เดือน',
            '6-12 เดือน': '6-12 เดือน'
        }
        message += f"   • ระยะเวลาลงทุนแนะนำ: {horizon_th.get(horizon, horizon)}\n"
    
    # 🆕 แสดงสัญญาณทางเทคนิค
    tech_signals = result.get('technical_signals', {})
    if tech_signals:
        message += f"\n   📊 <b>สัญญาณทางเทคนิค:</b>\n"
        
        if tech_signals.get('macd_crossover'):
            message += f"      ✅ MACD Crossover (ขาขึ้น)\n"
        else:
            message += f"      ❌ ยังไม่เกิด MACD Crossover\n"
        
        if tech_signals.get('rsi_above_50'):
            message += f"      ✅ RSI > 50 (แนวโน้มดี)\n"
        else:
            message += f"      ❌ RSI < 50 (แนวโน้มอ่อน)\n"
        
        if tech_signals.get('above_ema200'):
            message += f"      ✅ ราคาเหนือ EMA 200 (เทรนด์ขาขึ้น)\n"
        else:
            message += f"      ⚠️ ราคาต่ำกว่า EMA 200 (ยังไม่ปลอดภัย)\n"
    # ========================================
    # ข่าวสาร (แสดง 3 ข่าวแรก)
    # ========================================
    if result.get('news_count', 0) > 0:
        message += f"\n📰 <b>ข่าวล่าสุด:</b> (พบ {result['news_count']} ข่าว)\n"
        
        # ใช้ข้อมูล news_records ถ้ามี (ต้องส่งมาจาก analyze_single_stock)
        news_list = result.get('news_records', [])
        
        if not news_list and result.get('news_sample'):
            news_list = [result['news_sample']]
        
        # แสดงสูงสุด 3 ข่าว
        for i, news in enumerate(news_list[:3], 1):
            title = news.get('title_th') or news.get('title', '')
            if len(title) > 80:
                title = title[:80] + "..."
            
            message += f"\n   {i}. {title}\n"
            
            # แสดง sentiment ของแต่ละข่าว
            if news.get('sentiment_score') is not None:
                sent_score = news['sentiment_score']
                if sent_score > 0.3:
                    sent_emoji = "🟢 บวก"
                elif sent_score < -0.3:
                    sent_emoji = "🔴 ลบ"
                else:
                    sent_emoji = "🟡 กลางๆ"
                
                message += f"      ความรู้สึก: {sent_emoji} ({sent_score:.2f})\n"
            
            # แสดงแหล่งข่าว
            if news.get('source'):
                message += f"      แหล่งที่มา: {news['source']}\n"
        
        if result['news_count'] > 3:
            message += f"\n   ... และอีก {result['news_count'] - 3} ข่าว\n"
    
    # ========================================
    # ข้อมูลพื้นฐาน
    # ========================================
    fundamental = result.get('fundamental')
    if fundamental and any(fundamental.values()):
        message += "\n💼 <b>ข้อมูลพื้นฐาน:</b>\n"
        
        if fundamental.get('pe_ratio'):
            pe = fundamental['pe_ratio']
            message += f"   • P/E Ratio: {pe:.2f}"
            if pe < 15:
                message += " → ถูก 🟢\n"
            elif pe < 25:
                message += " → พอดี 🟡\n"
            else:
                message += " → แพง 🔴\n"
        
        if fundamental.get('peg_ratio'):
            peg = fundamental['peg_ratio']
            message += f"   • PEG Ratio: {peg:.2f}"
            if peg < 1:
                message += " → คุ้มค่า 🟢\n"
            elif peg < 2:
                message += " → ปานกลาง 🟡\n"
            else:
                message += " → แพง 🔴\n"
        
        if fundamental.get('eps_growth_pct'):
            eps_growth = fundamental['eps_growth_pct']
            message += f"   • การเติบโตของกำไร: {eps_growth:.1f}%"
            if eps_growth > 20:
                message += " 🚀\n"
            elif eps_growth > 10:
                message += " 📈\n"
            elif eps_growth > 0:
                message += " 📊\n"
            else:
                message += " 📉\n"
        
        if result.get('market_cap'):
            mc = result['market_cap']
            if mc >= 1e12:
                mc_str = f"${mc/1e12:.2f}T"
                mc_type = "(บริษัทขนาดใหญ่มาก)"
            elif mc >= 1e11:
                mc_str = f"${mc/1e9:.1f}B"
                mc_type = "(บริษัทขนาดใหญ่)"
            elif mc >= 1e10:
                mc_str = f"${mc/1e9:.1f}B"
                mc_type = "(บริษัทขนาดกลาง-ใหญ่)"
            elif mc >= 1e9:
                mc_str = f"${mc/1e9:.1f}B"
                mc_type = "(บริษัทขนาดกลาง)"
            else:
                mc_str = f"${mc/1e6:.1f}M"
                mc_type = "(บริษัทขนาดเล็ก)"
            
            message += f"   • มูลค่าตลาด: {mc_str} {mc_type}\n"
    
    # ========================================
    # Footer
    # ========================================
    message += f"\n⏰ {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
    
    return message
 


async def handle_symbol_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle เมื่อ user ส่ง symbol มา - เพิ่ม Detailed Logging
    """
    user = None
    loading_msg = None
    symbol = None
    
    try:
        # 1. ดึงข้อมูล User
        user = update.effective_user
        user_id = user.id
        username = user.username or user.first_name
        user_message = update.message.text.strip().upper()
        
        # 🆕 Detailed Log
        print(f"\n{'='*60}")
        print(f"📨 NEW MESSAGE RECEIVED")
        print(f"{'='*60}")
        print(f"👤 User: {username}")
        print(f"🆔 User ID: {user_id}")
        print(f"💬 Message: '{user_message}'")
        print(f"📏 Length: {len(user_message)} chars")
        print(f"🕐 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        # 2. Validate Input
        if not user_message:
            print("⚠️ VALIDATION FAILED: Empty message")
            await update.message.reply_text("❌ กรุณาส่งข้อความ")
            return
        
        if len(user_message) > 5:
            print(f"⚠️ VALIDATION FAILED: Too long ({len(user_message)} chars)")
            await update.message.reply_text(
                "❌ Stock Symbol ต้องมีความยาว 1-5 ตัวอักษร\n"
                "ตัวอย่าง: AAPL, MSFT, GOOGL"
            )
            return
        
        if not user_message.isalpha():
            print(f"⚠️ VALIDATION FAILED: Contains non-alpha chars")
            await update.message.reply_text(
                "❌ Stock Symbol ต้องเป็นตัวอักษรเท่านั้น\n"
                "ตัวอย่าง: AAPL, MSFT, GOOGL"
            )
            return
        
        symbol = user_message
        print(f"✅ VALIDATION PASSED: '{symbol}'")
        
        # 3. Send Loading Message
        try:
            print(f"📤 Sending loading message...")
            loading_msg = await update.message.reply_text(
                f"🔍 กำลังวิเคราะห์ <b>{symbol}</b>...\n"
                "⏳ โปรดรอสักครู่ (10-30 วินาที)",
                parse_mode='HTML'
            )
            print(f"✅ Loading message sent (ID: {loading_msg.message_id})")
        except Exception as load_err:
            print(f"⚠️ Failed to send loading message: {load_err}")
            loading_msg = None
        
        # 4. Analyze Stock
        print(f"\n🔬 STARTING ANALYSIS")
        print(f"   Symbol: {symbol}")
        print(f"   Timeout: 90 seconds")
        print(f"{'─'*60}")
        
        try:
            result = await asyncio.wait_for(
                analyze_single_stock(symbol),
                timeout=90.0
            )
            
            print(f"{'─'*60}")
            print(f"✅ ANALYSIS COMPLETED")
            print(f"   Success: {result.get('success', False)}")
            
        except asyncio.TimeoutError:
            print(f"❌ TIMEOUT ERROR (90s exceeded)")
            
            error_msg = (
                f"⏱️ การวิเคราะห์ <b>{symbol}</b> ใช้เวลานานเกินไป\n\n"
                f"💡 กรุณาลองใหม่อีกครั้ง"
            )
            
            if loading_msg:
                await loading_msg.edit_text(error_msg, parse_mode='HTML')
            else:
                await update.message.reply_text(error_msg, parse_mode='HTML')
            return
            
        except Exception as analysis_err:
            print(f"❌ ANALYSIS ERROR:")
            print(f"   Error: {analysis_err}")
            
            import traceback
            traceback.print_exc()
            
            error_msg = (
                f"❌ ไม่สามารถวิเคราะห์ <b>{symbol}</b> ได้\n\n"
                f"<i>Error: {str(analysis_err)[:100]}</i>"
            )
            
            if loading_msg:
                await loading_msg.edit_text(error_msg, parse_mode='HTML')
            else:
                await update.message.reply_text(error_msg, parse_mode='HTML')
            return
        
        # 5. Check Result
        if not result or not result.get('success'):
            error_detail = result.get('error', 'Unknown error') if result else 'No result'
            print(f"⚠️ ANALYSIS FAILED: {error_detail}")
            
            error_msg = (
                f"❌ ไม่สามารถดึงข้อมูล <b>{symbol}</b> ได้\n\n"
                f"💡 ตรวจสอบว่า symbol ถูกต้อง"
            )
            
            if loading_msg:
                await loading_msg.edit_text(error_msg, parse_mode='HTML')
            else:
                await update.message.reply_text(error_msg, parse_mode='HTML')
            return
        
        # 6. Format Message
        try:
            print(f"📝 Formatting message...")
            response = format_stock_analysis_message(result)
            print(f"✅ Message formatted ({len(response)} chars)")
            
        except Exception as format_err:
            print(f"❌ FORMAT ERROR: {format_err}")
            
            error_msg = f"❌ ไม่สามารถจัดรูปแบบข้อมูลได้"
            
            if loading_msg:
                await loading_msg.edit_text(error_msg)
            else:
                await update.message.reply_text(error_msg)
            return
        
        # 7. Send Response
        try:
            print(f"📤 Sending response...")
            
            if loading_msg:
                await loading_msg.edit_text(response, parse_mode='HTML')
            else:
                await update.message.reply_text(response, parse_mode='HTML')
            
            print(f"✅ RESPONSE SENT SUCCESSFULLY")
            print(f"{'='*60}\n")
            
        except Exception as send_err:
            print(f"❌ SEND ERROR: {send_err}")
            
            # Try plain text
            try:
                plain = response.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
                
                if loading_msg:
                    await loading_msg.edit_text(plain)
                else:
                    await update.message.reply_text(plain)
                    
                print(f"✅ Plain text sent")
                
            except Exception as plain_err:
                print(f"❌ Even plain text failed: {plain_err}")
                await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการส่งข้อมูล")
        
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR in handle_symbol_command:")
        print(f"   Symbol: {symbol}")
        print(f"   User: {username if user else 'Unknown'}")
        print(f"   Error: {e}")
        
        import traceback
        traceback.print_exc()
        
        try:
            error_msg = (
                f"❌ เกิดข้อผิดพลาดที่ไม่คาดคิด\n\n"
                f"กรุณาลองใหม่อีกครั้ง"
            )
            
            if loading_msg:
                await loading_msg.edit_text(error_msg)
            else:
                await update.message.reply_text(error_msg)
        except:
            print("❌ Cannot send error message to user")


async def start_telegram_bot():
    """
    เริ่มต้น Telegram Bot - เพิ่ม Detailed Logging
    """
    if not TELEGRAM_BOT_TOKEN:
        print("⚠️ TELEGRAM_BOT_TOKEN not configured, bot disabled")
        return
    
    print(f"\n{'='*60}")
    print("🤖 Starting Telegram Bot (Polling Mode)")
    print(f"   Token: {TELEGRAM_BOT_TOKEN[:20]}...")  # 🆕 Debug token
    print(f"{'='*60}\n")
    
    try:
        # 1. สร้าง Application
        print("📱 Creating Telegram Application...")
        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        print("✅ Application created successfully")
        
        # 2. 🆕 Command Handlers พร้อม Logging
        async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /start command"""
            user = update.effective_user
            user_id = user.id
            username = user.username or user.first_name
            
            print(f"\n🎯 /start command")
            print(f"   User: {username} (ID: {user_id})")
            print(f"   Time: {datetime.now().strftime('%H:%M:%S')}")
            
            await update.message.reply_text(
                "👋 สวัสดีครับ!\n\n"
                "📊 ส่ง Stock Symbol มาเพื่อดูการวิเคราะห์\n"
                "ตัวอย่าง: AAPL, MSFT, GOOGL\n\n"
                "ℹ️ รองรับหุ้นสหรัฐฯ ทุกตัวที่มีในระบบ"
            )
            print(f"✅ /start response sent to {username}")

        async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /help command"""
            user = update.effective_user
            user_id = user.id
            username = user.username or user.first_name
            
            print(f"\n🎯 /help command")
            print(f"   User: {username} (ID: {user_id})")
            
            await update.message.reply_text(
                "📖 <b>วิธีใช้งาน:</b>\n\n"
                "1️⃣ ส่ง Stock Symbol (เช่น AAPL)\n"
                "2️⃣ รอระบบวิเคราะห์ (10-30 วินาที)\n"
                "3️⃣ รับข้อมูลครบถ้วน พร้อมคำแนะนำ\n\n"
                "💡 <b>ตัวอย่าง Symbol:</b>\n"
                "• AAPL - Apple\n"
                "• MSFT - Microsoft\n"
                "• GOOGL - Google\n"
                "• TSLA - Tesla\n"
                "• NVDA - NVIDIA",
                parse_mode='HTML'
            )
            print(f"✅ /help response sent to {username}")


        async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /status command"""
            user = update.effective_user
            username = user.username or user.first_name
            
            print(f"\n🎯 /status command from {username}")
            
            await update.message.reply_text(
                f"🤖 <b>Bot Status</b>\n\n"
                f"✅ Online and Running\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                f"💡 Ready to analyze stocks!",
                parse_mode='HTML'
            )
            print(f"✅ /status response sent")
        
        # 3. Register Command Handlers
        from telegram.ext import CommandHandler
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("status", status_command))
        print("✅ Command handlers registered: /start, /help, /status")
        
        # 4. 🆕 Register Message Handler พร้อม Debug
        print("📝 Registering message handler...")
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_symbol_command))
        print("✅ Message handler registered (TEXT & ~COMMAND)")


        async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle errors"""
            print(f"\n❌ ERROR OCCURRED")
            print(f"   Update: {update}")
            print(f"   Error: {context.error}")
            
            import traceback
            traceback.print_exc()
            
            if update and update.effective_message:
                try:
                    await update.effective_message.reply_text(
                        "❌ เกิดข้อผิดพลาดในระบบ\nกรุณาลองใหม่อีกครั้ง"
                    )
                except Exception as e:
                    print(f"   Cannot send error message: {e}")
        
        app.add_error_handler(error_handler)
        print("✅ Error handler registered")
        
        # 6. Initialize & Start
        print("\n🔄 Initializing bot...")
        await app.initialize()
        print("✅ Bot initialized")
        
        print("🔄 Starting bot...")
        await app.start()
        print("✅ Bot started")
        
        # 7. 🆕 Start Polling พร้อม Parameters ที่ชัดเจน
        print("\n🔄 Starting polling...")
        print("   Settings:")
        print("   - Poll interval: 1.0 seconds")
        print("   - Allowed updates: ALL_TYPES")
        print("   - Drop pending: True")
        
        await app.updater.start_polling(
            poll_interval=1.0,
            timeout=10,
            bootstrap_retries=-1,  # 🆕 Retry forever
            read_timeout=2,
            write_timeout=None,
            connect_timeout=None,
            pool_timeout=None,
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
        
        print("\n" + "="*60)
        print("🎉 BOT IS NOW RUNNING!")
        print("="*60)
        print("📱 Waiting for messages...")
        print("💡 Commands: /start, /help, /status")
        print("📊 Send stock symbol (e.g., AAPL)")
        print("⏹️  Press Ctrl+C to stop")
        print("="*60 + "\n")
        
        # 8. 🆕 Keep Alive Loop พร้อม Status Updates
        import time
        last_status = time.time()
        message_count = 0
        
        async def print_status():
            """พิมพ์สถานะทุก 60 วินาที"""
            nonlocal last_status
            current_time = time.time()
            
            if current_time - last_status >= 60:
                uptime = current_time - last_status
                print(f"\n💚 Bot Alive - {datetime.now().strftime('%H:%M:%S')}")
                print(f"   Uptime: {int(uptime)} seconds")
                print(f"   Messages handled: {message_count}\n")
                last_status = current_time
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            await print_status()
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Shutting down bot...")
        if 'app' in locals():
            await app.stop()
            await app.shutdown()
        print("✅ Bot stopped gracefully")
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR in start_telegram_bot:")
        print(f"   {e}")
        import traceback
        traceback.print_exc()
        raise



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

    # ✅ เพิ่ม: บันทึกเวลาเริ่มต้น
    start_time = datetime.now()
    
    # ✅ เพิ่ม: ส่ง notification เริ่มต้น
    await send_telegram_message(
        f"🚀 <b>Stock Analysis Started</b>\n\n"
        f"📊 Processing {len(stocks)} symbols\n"
        f"⏰ {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    print(f"\n🚀 Starting technical analysis for {len(stocks)} symbols")
    print("🌍 Fetching macro data...")
    macro_data = fetch_macro_data()
    print()
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
        'low_confidence': 0,
        'strong_buy_symbols': [],  # 🆕 เก็บ symbol ของ Strong Buy
        'buy_symbols': []           # 🆕 เก็บ symbol ของ Buy
    }
    
    for idx, stock_data in enumerate(stocks, 1):
        symbol = stock_data['symbol']
        category = stock_data.get('category', 'Core')
        
        print(f"\n{'='*60}")
        print(f"[{idx}/{len(stocks)}] Processing: {symbol} ({category})")
        print(f"{'='*60}")
        
        # ============================================
        # STEP 1: ดึงข้อมูล Technical
        # ============================================
        data = await fetch_data_waterfall(symbol)
        
        if not data:
            print(f"❌ Failed: {symbol}")
            stats['failed'] += 1
            await asyncio.sleep(5)
            continue
        
        if not data.get("ema_200"):
            print(f"⚠️ {symbol}: No EMA 200 data available")
        
        # ============================================
        # STEP 2: ดึง Market Cap + Fundamental Data
        # ============================================
        print(f"📊 Calculating metrics for {symbol}...")
        
        market_cap = None
        fundamental_data = None
        
        if category != 'ETF':
            try:
                stock = yf.Ticker(symbol)
                info = stock.info
                
                # ดึง market_cap
                market_cap = info.get('marketCap')
                
                # ดึง fundamental data
            

            
                pe_ratio   = info.get('trailingPE')
                eps_growth = info.get('earningsGrowth', 0) * 100 if info.get('earningsGrowth') else None
                peg_ratio  = info.get('pegRatio') or info.get('trailingPegRatio')
                
                # ✅ fallback คำนวณเองถ้าไม่มีค่า
                if not peg_ratio and pe_ratio and eps_growth and eps_growth > 0:
                    peg_ratio = round(pe_ratio / eps_growth, 2)
                
                fundamental_data = {
                    "pe_ratio": pe_ratio,
                    "pe_ratio_forward": info.get('forwardPE'),
                    "peg_ratio": peg_ratio,
                    "eps_growth_pct": eps_growth,
                    "market_cap": market_cap,
                    "analyst_price_target": info.get('targetMeanPrice') or info.get('targetMedianPrice')
                }
 
                
                if market_cap:
                    market_cap_str = f"${market_cap/1e9:.1f}B" if market_cap >= 1e9 else f"${market_cap/1e6:.1f}M"
                    print(f"   Market Cap: {market_cap_str}")
                
            except Exception as yf_error:
                print(f"⚠️ Could not fetch yfinance data: {yf_error}")
        
        # คำนวณ Upside
      

        upside_pct = calculate_upside_pct(
            data.get("price"), 
            data.get("ema_200"),
            data.get("ema_50"),
            analyst_target=fundamental_data.get("analyst_price_target") if fundamental_data else None
        )
        # ข้าม analyst/sentiment สำหรับ ETF
        analyst_pct = None if category == 'ETF' else fetch_analyst_data(symbol)
        sentiment = None if category == 'ETF' else fetch_sentiment_score(symbol)
        
        # ============================================
        # STEP 3: บันทึก Snapshot
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
            "recorded_at": datetime.now().isoformat(),
            # ── ใหม่ Volume ──────────────────────────
            "volume":     data.get("volume"),
            "vol_ma20":   data.get("vol_ma20"),
            "vol_ratio":  data.get("vol_ratio"),
            "obv":        data.get("obv"),
            "vwap":       data.get("vwap"),
            # ── ใหม่ Technical ───────────────────────
            "atr":        data.get("atr"),
            "stoch_k":    data.get("stoch_k"),
            "stoch_d":    data.get("stoch_d"),
            # ── ใหม่ Macro ───────────────────────────
            "vix":              macro_data.get("vix"),
            "vix_signal":       macro_data.get("vix_signal"),
            "spy_chg":          macro_data.get("spy_chg"),
            "qqq_chg":          macro_data.get("qqq_chg"),
            "xlk_chg":          macro_data.get("xlk_chg"),
            "bond_10y":         macro_data.get("bond"),
            "market_sentiment": macro_data.get("market_sentiment"),
        }
        
         
        
        # เพิ่ม fundamental data ใน snapshot
        if fundamental_data:
            snapshot_payload["pe_ratio"] = fundamental_data.get("pe_ratio")
            snapshot_payload["peg_ratio"] = fundamental_data.get("peg_ratio")
            snapshot_payload["eps_growth_pct"] = fundamental_data.get("eps_growth_pct")
            snapshot_payload["market_cap"] = market_cap
        
        # บันทึก snapshot
        max_db_retries = 3
        snapshot_saved = False
        
        for db_attempt in range(max_db_retries):
            try:
                # เพิ่มตรงนี้
                print(f"\n📦 snapshot_payload ที่จะ insert:")
                print(f"   volume          = {snapshot_payload.get('volume')}")
                print(f"   vol_ratio       = {snapshot_payload.get('vol_ratio')}")
                print(f"   atr             = {snapshot_payload.get('atr')}")
                print(f"   stoch_k         = {snapshot_payload.get('stoch_k')}")
                print(f"   vix             = {snapshot_payload.get('vix')}")
                print(f"   market_sentiment= {snapshot_payload.get('market_sentiment')}")
                
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
        # STEP 4: ดึงและบันทึกข่าว
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
                            
                            # คำนวณ Sentiment แบบใหม่ (ถ้ามีฟังก์ชัน)
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
                    
                    if sentiment_scores:
                        news_sentiment_advanced = round(sum(sentiment_scores) / len(sentiment_scores), 2)
                        print(f"   Advanced Sentiment: {news_sentiment_advanced:.2f}")
                    
                except Exception as news_error:
                    print(f"⚠️ Failed to save news for {symbol}: {news_error}")
            else:
                print(f"📭 No valid news found for {symbol}")
                
        # ✅ UPDATE sentiment_score หลังได้ข้อมูลจาก Finnhub
            if news_sentiment_advanced is not None and snapshot_saved:
                try:
                    latest_snapshot = supabase.table("stock_snapshots")\
                        .select("id")\
                        .eq("symbol", symbol)\
                        .order("recorded_at", desc=True)\
                        .limit(1)\
                        .execute()
                    
                    if latest_snapshot.data:
                        snap_id = latest_snapshot.data[0]['id']
                        supabase.table("stock_snapshots")\
                            .update({"sentiment_score": news_sentiment_advanced})\
                            .eq("id", snap_id)\
                            .execute()
                        print(f"✅ Updated sentiment_score = {news_sentiment_advanced}")
                except Exception as e:
                    print(f"⚠️ Cannot update sentiment_score: {e}")
        # ============================================
        # STEP 5: คำนวณ AI Prediction
        # ============================================
        print(f"🤖 Calculating AI prediction for {symbol}...")
        
        # เตรียมข้อมูล Technical
        # macro_data = fetch_macro_data()
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
            'analyst_buy_pct': analyst_pct if analyst_pct else None,
            # ── ใหม่ทั้งหมด ──────────────────────────
            'volume':          data.get('volume'),
            'vol_ma20':        data.get('vol_ma20'),
            'vol_ratio':       data.get('vol_ratio'),
            'obv':             data.get('obv'),
            'vwap':            data.get('vwap'),
            'atr':             data.get('atr'),
            'stoch_k':         data.get('stoch_k'),
            'stoch_d':         data.get('stoch_d'),
            'change_pct':      data.get('change_pct'),
            'macro':           macro_data,          # ← ต่างกันนิดนึง (ดูด้านล่าง)
        }
        
        # ใช้ Sentiment แบบใหม่ถ้ามี
        final_sentiment = news_sentiment_advanced if news_sentiment_advanced is not None else sentiment
        
        # คำนวณ Overall Score
        if 'calculate_overall_score_with_risk' in globals():
            # ใช้เวอร์ชันใหม่ที่มี Risk Management
            overall_score = calculate_overall_score_with_risk(
                symbol=symbol,
                tech_data=tech_data_full,
                fundamental_data=fundamental_data,
                news_sentiment=final_sentiment,
                category=category,
                market_cap=market_cap
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
            # ใช้เวอร์ชันใหม่
            recommendation_data = generate_recommendation_advanced(
                overall_score=overall_score,
                price=data.get('price'),
                upside_pct=upside_pct,
                risk_score=risk_score,
                category=category,
                tech_data=tech_data_full
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
        # STEP 6: บันทึก AI Prediction (พร้อมฟิลด์ใหม่)
        # ============================================
        prediction_payload = {
            "symbol": symbol,
            "ai_model": "rule_based_v2" if 'calculate_overall_score_with_risk' in globals() else "rule_based_v1",
            "overall_score": overall_score,
            "recommendation": recommendation,
            "price_at_prediction": data.get('price'),
            "actual_outcome": None,
            "reason": reason,
            # ── ใหม่ ─────────────────────────────────
            "stop_loss":         recommendation_data.get('stop_loss')    if isinstance(recommendation_data, dict) else None,
            "risk_reward":       recommendation_data.get('risk_reward')  if isinstance(recommendation_data, dict) else None,
            "position_size":     recommendation_data.get('position_size') if isinstance(recommendation_data, dict) else None,
            "warning":           recommendation_data.get('warning')      if isinstance(recommendation_data, dict) else None,
            "technical_signals": recommendation_data.get('technical_signals') if isinstance(recommendation_data, dict) else None,
        }
       
        
        # 🆕 เพิ่มฟิลด์ใหม่ (ตอนนี้ใช้ได้แล้ว!)
        if risk_score > 0:
             
            prediction_payload["risk_score"] = risk_score if risk_score is not None else 0
        
        if confidence:
            prediction_payload["confidence"] = confidence
        
        if price_target:
            prediction_payload["price_target"] = price_target
        
        if time_horizon:
            prediction_payload["time_horizon"] = time_horizon
        
        try:
            supabase.table("ai_predictions").insert(prediction_payload).execute()
            
            # แสดงผลแบบละเอียด
            print(f"✅ AI Prediction saved: {symbol}")
            print(f"   📊 Score: {overall_score}/100 | {recommendation}")
            
            if risk_score > 0:
                risk_level = 'High' if risk_score >= 60 else 'Medium' if risk_score >= 30 else 'Low'
                print(f"   💎 Risk: {risk_score}/100 ({risk_level})")
            
            if confidence:
                print(f"   🎯 Confidence: {confidence}")
                
                # นับสถิติ confidence
                if confidence == 'High':
                    stats['high_confidence'] += 1
                elif confidence == 'Medium':
                    stats['medium_confidence'] += 1
                elif confidence == 'Low':
                    stats['low_confidence'] += 1
            
            print(f"   📝 Reason: {reason}")
            
            if price_target:
                upside_to_target = ((price_target - data.get('price')) / data.get('price')) * 100
                print(f"   🎯 Target: ${price_target:.2f} (+{upside_to_target:.1f}%)")
            
            if time_horizon:
                print(f"   ⏰ Horizon: {time_horizon}")
            
            # อัพเดตสถิติ
     

            stats['success'] += 1
            if recommendation == 'Strong Buy':
                stats['strong_buy'] += 1
                stats['strong_buy_symbols'].append(symbol)  # 🆕 เก็บ symbol
            elif recommendation == 'Buy':
                stats['buy'] += 1
                stats['buy_symbols'].append(symbol)  # 🆕 เก็บ symbol
            elif recommendation == 'Hold':
                stats['hold'] += 1
            elif recommendation in ['Sell', 'Strong Sell']:
                stats['sell'] += 1
            
            # ✅ เพิ่ม: แจ้งเตือน Strong Buy ที่มี High Confidence
            if recommendation == 'Strong Buy' and confidence == 'High':
                upside_to_target = ((price_target - data.get('price')) / data.get('price')) * 100 if price_target else 0
                await send_telegram_message(
                    f"🔥 <b>Strong Buy Alert!</b>\n\n"
                    f"📌 {symbol} ({category})\n"
                    f"💰 Price: ${data.get('price'):.2f}\n"
                    f"📊 Score: {overall_score}/100\n"
                    f"🎯 Target: ${price_target:.2f} (+{upside_to_target:.1f}%)\n"
                    
                    f"💎 Risk: {risk_score}/100\n"
                    f"📝 {reason}"
                )
                
        except Exception as pred_error:
            print(f"⚠️ Failed to save prediction for {symbol}: {pred_error}")
            stats['failed'] += 1
        
        # ✅ เพิ่ม: ส่ง progress update ทุกๆ 10 หุ้น
        #if idx % 10 == 0:
        #    progress = (idx / len(stocks)) * 100
        #    await send_telegram_message(
        #        f"📊 <b>Progress Update</b>\n\n"
        #        f"✅ Completed: {idx}/{len(stocks)} ({progress:.1f}%)\n"
        #        f"🟢 Success: {stats['success']}\n"
        #        f"❌ Failed: {stats['failed']}"
        #    )
        
        # หน่วงเวลาก่อนประมวลผลหุ้นถัดไป
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
    
    # ✅ เพิ่ม: ส่งสรุปผลสุดท้าย
    summary_message = format_telegram_summary(stats, len(stocks), start_time)
    await send_telegram_message(summary_message)

    #await send_market_alert_after_collection()
    await send_market_alert_after_collection()


 
if __name__ == "__main__":
    import sys
    
    print(f"\n{'='*60}")
    print("🚀 Stock Analysis Bot & Collector")
    print(f"   Version: 2.0")
    print(f"   Python: {sys.version.split()[0]}")
    print(f"{'='*60}\n")
    
    # 🆕 Debug: แสดง Environment Variables
    print("🔧 Configuration Check:")
    print(f"   TELEGRAM_BOT_TOKEN: {'✅ Set' if TELEGRAM_BOT_TOKEN else '❌ Missing'}")
    print(f"   TELEGRAM_CHAT_ID: {'✅ Set' if TELEGRAM_CHAT_ID else '⚠️ Not set (OK for bot)'}")
    print(f"   SUPABASE_URL: {'✅ Set' if SUPABASE_URL else '❌ Missing'}")
    print(f"   SUPABASE_KEY: {'✅ Set' if SUPABASE_KEY else '❌ Missing'}")
    print(f"   FINNHUB_KEY: {'✅ Set' if FINNHUB_KEY else '⚠️ Not set'}")
    print(f"   TWELVE_DATA_KEY: {'✅ Set' if TWELVE_DATA_KEY else '⚠️ Not set'}")
    print()
    
    # Parse arguments
    mode = "collector"  # default
    
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg == "--bot":
            mode = "bot"
        elif arg == "--collect":
            mode = "collector"
        else:
            print(f"⚠️ Unknown argument: {sys.argv[1]}")
            print("   Valid options: --bot, --collect")
            sys.exit(1)
    
    try:
        if mode == "bot":
            # 🆕 Bot mode พร้อม pre-check
            print("🤖 Mode: Telegram Bot")
            print("="*60)
            
            if not TELEGRAM_BOT_TOKEN:
                print("\n❌ ERROR: TELEGRAM_BOT_TOKEN is required for bot mode")
                print("   Please set the environment variable and try again")
                sys.exit(1)
            
            print("\n✅ Starting bot...\n")
            asyncio.run(start_telegram_bot())
            
        else:
            # Collector mode
            print("📊 Mode: Stock Data Collector")
            print("="*60 + "\n")
            asyncio.run(main())
            
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
        
    except Exception as e:
        print(f"\n❌ FATAL ERROR:")
        print(f"   {e}")
        print("\n📋 Full Traceback:")
        import traceback
        traceback.print_exc()
        sys.exit(1)
