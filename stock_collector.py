import os
import asyncio
import yfinance as yf
import pandas as pd
import talib
from supabase import create_client, Client
import requests
from datetime import datetime
import google.generativeai as genai  
import json
import time


# --- Configuration ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TWELVE_DATA_KEY = os.getenv("TWELVE_DATA_KEY")
GEMINI_API_KEYS = [
    os.getenv("GEMINI_API_KEY_1"),
    os.getenv("GEMINI_API_KEY_2"),
    os.getenv("GEMINI_API_KEY_3"),
    os.getenv("GEMINI_API_KEY_4"),
    os.getenv("GEMINI_API_KEY_5"),
]

# กรองเฉพาะ keys ที่ไม่เป็น None
GEMINI_API_KEYS = [key for key in GEMINI_API_KEYS if key]

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY")

if not GEMINI_API_KEYS:
    raise ValueError("❌ No GEMINI_API_KEY found")

print(f"✅ Loaded {len(GEMINI_API_KEYS)} Gemini API keys")
# ตัวแปรติดตามการใช้งาน key
current_key_index = 0
key_usage_count = {i: 0 for i in range(len(GEMINI_API_KEYS))}
key_cooldown_until = {i: 0 for i in range(len(GEMINI_API_KEYS))}
 
 
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_available_gemini_models():
    """ดึงรายชื่อ model ที่ใช้ได้จริงจาก API"""
    try:
        key_index, api_key = get_next_available_key()
        genai.configure(api_key=api_key)
        
        available_models = []
        for model in genai.list_models():
            if 'generateContent' in model.supported_generation_methods:
                available_models.append(model.name.replace('models/', ''))
        
        print(f"✅ Available models: {available_models}")
        return available_models
    except Exception as e:
        print(f"⚠️ Could not list models: {e}")
        # fallback ไปใช้ model พื้นฐาน
        return ['gemini-pro']
        

def get_next_available_key():
    """หา API key ถัดไปที่พร้อมใช้งาน"""
    global current_key_index
    
    current_time = time.time()
    attempts = 0
    max_attempts = len(GEMINI_API_KEYS) * 2  # ลองวนไปมา 2 รอบ
    
    while attempts < max_attempts:
        # ตรวจสอบว่า key ปัจจุบันพร้อมใช้งานหรือไม่
        if current_time >= key_cooldown_until[current_key_index]:
            key = GEMINI_API_KEYS[current_key_index]
            print(f"🔑 Using API Key #{current_key_index + 1} (Used: {key_usage_count[current_key_index]} times)")
            return current_key_index, key
        
        # ถ้าไม่พร้อม ไปใช้ key ถัดไป
        current_key_index = (current_key_index + 1) % len(GEMINI_API_KEYS)
        attempts += 1
    
    # ถ้าไม่มี key ไหนพร้อม ให้รอและใช้ key แรก
    wait_time = min(key_cooldown_until.values()) - current_time
    if wait_time > 0:
        print(f"⏳ All keys are on cooldown. Waiting {wait_time:.1f} seconds...")
        time.sleep(wait_time + 1)
    
    current_key_index = 0
    return 0, GEMINI_API_KEYS[0]


def mark_key_as_rate_limited(key_index, cooldown_seconds=60):
    """ทำเครื่องหมาย key ว่าเกิน rate limit และต้องพัก"""
    key_cooldown_until[key_index] = time.time() + cooldown_seconds
    print(f"⚠️ API Key #{key_index + 1} rate limited. Cooldown for {cooldown_seconds}s")


def rotate_to_next_key():
    """สลับไปใช้ key ถัดไป"""
    global current_key_index
    old_index = current_key_index
    current_key_index = (current_key_index + 1) % len(GEMINI_API_KEYS)
    print(f"🔄 Rotating from Key #{old_index + 1} to Key #{current_key_index + 1}")



def analyze_with_gemini(symbol, snapshot_data, max_retries=3):
    """ใช้ Gemini วิเคราะห์หุ้น/ETF พร้อม key rotation และ model fallback"""
    
    # ⬇️ ใช้ model ที่มีจริงจากรายการที่ได้
    models_to_try = [
        'gemini-2.5-flash',           # ใหม่ล่าสุด
        'gemini-2.0-flash',            # รุ่นก่อนหน้า
        'gemini-flash-latest',         # Latest stable
        'gemini-pro-latest',           # Pro version
        'gemini-2.5-pro',              # Pro 2.5
    ]
    
    # ตรวจสอบว่าเป็น ETF หรือไม่
    is_etf = snapshot_data.get('category') == 'ETF'
    
    for model_name in models_to_try:
        for attempt in range(max_retries):
            try:
                # หา key ที่พร้อมใช้งาน
                key_index, api_key = get_next_available_key()
                
                # ตั้งค่า Gemini
                genai.configure(api_key=api_key)
                model = genai.GenerativeModel(model_name)
                
                print(f"🤖 Trying model: {model_name}")
                
                # สร้าง prompt ตาม type
                if is_etf:
                    prompt = f"""
You are a professional ETF analyst. Analyze the following ETF data and provide:
1. overall_score (0-100): Overall investment attractiveness for ETF
2. recommendation: One of ["Strong Buy", "Buy", "Hold", "Sell", "Strong Sell"]
3. Brief reasoning (2-3 sentences focusing on ETF characteristics)

ETF: {symbol}
Current Price: ${snapshot_data.get('price', 'N/A')}
Change %: {snapshot_data.get('change_pct', 'N/A')}%

Note: This is an ETF (Exchange-Traded Fund). Analyze based on market trend and diversification benefits.

Respond ONLY in JSON format:
{{
  "overall_score": <number 0-100>,
  "recommendation": "<Strong Buy/Buy/Hold/Sell/Strong Sell>",
  "reasoning": "<brief explanation>"
}}
"""
                else:
                    prompt = f"""
You are a professional stock analyst. Analyze the following stock data and provide:
1. overall_score (0-100): Overall investment attractiveness
2. recommendation: One of ["Strong Buy", "Buy", "Hold", "Sell", "Strong Sell"]
3. Brief reasoning (2-3 sentences)

Stock: {symbol}
Current Price: ${snapshot_data.get('price', 'N/A')}
Change %: {snapshot_data.get('change_pct', 'N/A')}%
RSI: {snapshot_data.get('rsi', 'N/A')}
MACD: {snapshot_data.get('macd', 'N/A')}
EMA 20: ${snapshot_data.get('ema_20', 'N/A')}
EMA 50: ${snapshot_data.get('ema_50', 'N/A')}
EMA 200: ${snapshot_data.get('ema_200', 'N/A')}
Upside Potential: {snapshot_data.get('upside_pct', 'N/A')}%
Analyst Buy %: {snapshot_data.get('analyst_buy_pct', 'N/A')}%
Sentiment Score: {snapshot_data.get('sentiment_score', 'N/A')}

Respond ONLY in JSON format:
{{
  "overall_score": <number 0-100>,
  "recommendation": "<Strong Buy/Buy/Hold/Sell/Strong Sell>",
  "reasoning": "<brief explanation>"
}}
"""
                
                # เรียก API
                response = model.generate_content(prompt)
                result_text = response.text.strip()
                
                # ลบ markdown code blocks
                if result_text.startswith("```json"):
                    result_text = result_text.replace("```json", "").replace("```", "").strip()
                elif result_text.startswith("```"):
                    result_text = result_text.replace("```", "").strip()
                
                # Parse JSON
                result = json.loads(result_text)
                
                # ✅ สำเร็จ - นับการใช้งาน
                key_usage_count[key_index] += 1
                print(f"✅ Successfully used model: {model_name}")
                
                return {
                    "overall_score": int(result.get("overall_score", 50)),
                    "recommendation": result.get("recommendation", "Hold"),
                    "reasoning": result.get("reasoning", "")
                }
            
            except json.JSONDecodeError as e:
                print(f"⚠️ JSON parse error for {symbol} with {model_name} (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"Response: {result_text[:200] if 'result_text' in locals() else 'N/A'}")
                
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                # ถ้า retry หมดแล้ว ให้ลอง model ถัดไป
                break
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # ถ้าเป็น 404 = model ไม่รองรับ → ลอง model ถัดไป
                if "404" in error_msg or "not found" in error_msg:
                    print(f"⚠️ Model {model_name} not available, trying next model...")
                    break  # ออกจาก retry loop ไปลอง model ถัดไป
                
                # ถ้าเป็น rate limit
                if "rate limit" in error_msg or "quota" in error_msg or "429" in error_msg or "resource_exhausted" in error_msg:
                    print(f"⚠️ Rate limit hit for {symbol} with Key #{key_index + 1}")
                    mark_key_as_rate_limited(key_index, cooldown_seconds=60)
                    rotate_to_next_key()
                    
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                else:
                    print(f"⚠️ Gemini API error for {symbol} with {model_name}: {e}")
                
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                # ถ้า retry หมดแล้ว ให้ลอง model ถัดไป
                break
    
    # ลองทุก model แล้วไม่สำเร็จ
    print(f"❌ Failed to analyze {symbol} after trying all models")
    return None 
 
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
    available_models = get_available_gemini_models()
    
    # ดึงทั้ง symbol และ category
    res = supabase.table("stock_master").select("symbol, category").eq("is_active", True).execute()
    stocks = res.data
    
    if not stocks:
        print("📭 No active symbols found in stock_master.")
        return

    print(f"\n🚀 Starting analysis for {len(stocks)} symbols\n")
    
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
        
        # ⬇️ แก้ไขตรงนี้: ไม่ใส่ category ใน snapshot_payload
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
                break
            except Exception as db_error:
                print(f"⚠️ Database error (attempt {db_attempt + 1}/{max_db_retries}): {db_error}")
                if db_attempt < max_db_retries - 1:
                    await asyncio.sleep(2)
                    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                else:
                    print(f"❌ Failed to save snapshot for {symbol}")
                    break

        # ⬇️ แก้ไขตรงนี้: เพิ่ม category ชั่วคราวสำหรับส่งให้ AI
        print(f"🤖 Analyzing {symbol} with Gemini AI...")
        snapshot_with_category = {**snapshot_payload, "category": category}
        ai_result = analyze_with_gemini(symbol, snapshot_with_category)
        
        if ai_result:
            prediction_payload = {
                "symbol": symbol,
                "ai_model": "gemini-pro",
                "overall_score": ai_result["overall_score"],
                "recommendation": ai_result["recommendation"],
                "price_at_prediction": data.get("price"),
                "created_at": datetime.now().isoformat()
            }
             
            for db_attempt in range(max_db_retries):
                try:
                    supabase.table("ai_predictions").insert(prediction_payload).execute()
                    print(f"🎯 AI Prediction: {symbol} | Score: {ai_result['overall_score']}/100 | {ai_result['recommendation']}")
                    print(f"   Reasoning: {ai_result['reasoning']}")
                    break
                except Exception as db_error:
                    print(f"⚠️ Database error saving prediction (attempt {db_attempt + 1}/{max_db_retries}): {db_error}")
                    if db_attempt < max_db_retries - 1:
                        await asyncio.sleep(2)
                        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                    else:
                        print(f"❌ Failed to save AI prediction for {symbol}")
                        break
        else:
            print(f"⚠️ Could not get AI prediction for {symbol}")
        
        await asyncio.sleep(3)
    
    # แสดงสถิติการใช้งาน API keys
    print(f"\n{'='*60}")
    print("📊 API Key Usage Statistics:")
    print(f"{'='*60}")
    for i, count in key_usage_count.items():
        print(f"Key #{i + 1}: {count} requests")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    asyncio.run(main())
