import os
import asyncio
import yfinance as yf
import pandas as pd
import pandas_ta as ta
from supabase import create_client, Client
import requests
from datetime import datetime

# --- Configuration ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TWELVE_DATA_KEY = os.getenv("TWELVE_DATA_KEY")

# เช็ค env vars
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def calculate_technical_indicators(df):
    """คำนวณค่าเทคนิคด้วยตัวเองผ่าน pandas_ta"""
    try:
        if len(df) < 20: 
            return None
        
        # คำนวณ RSI, MACD, EMA
        df.ta.rsi(length=14, append=True)
        df.ta.macd(fast=12, slow=26, signal=9, append=True)
        df.ta.ema(length=20, append=True)
        df.ta.ema(length=50, append=True)
        df.ta.ema(length=200, append=True)
        df.ta.bbands(length=20, std=2, append=True)
        
        last = df.iloc[-1]
        
        return {
            "price": float(last['Close']),
            "rsi": float(last['RSI_14']) if not pd.isna(last['RSI_14']) else None,
            "macd": float(last['MACD_12_26_9']) if not pd.isna(last['MACD_12_26_9']) else None,
            "macd_signal": float(last['MACDs_12_26_9']) if not pd.isna(last['MACDs_12_26_9']) else None,
            "ema_20": float(last['EMA_20']) if not pd.isna(last['EMA_20']) else None,
            "ema_50": float(last['EMA_50']) if not pd.isna(last['EMA_50']) else None,
            "ema_200": float(last['EMA_200']) if not pd.isna(last['EMA_200']) else None,
            "bb_upper": float(last['BBU_20_2.0']) if not pd.isna(last['BBU_20_2.0']) else None,
            "bb_lower": float(last['BBL_20_2.0']) if not pd.isna(last['BBL_20_2.0']) else None
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
            if tech_data:
                prev_close = df['Close'].iloc[-2]
                current_price = tech_data['price']
                change_pct = ((current_price - prev_close) / prev_close) * 100
                
                tech_data['change_pct'] = round(change_pct, 2)
                tech_data['source'] = 'yfinance'
                return tech_data
            else:
                print(f"⚠️ Could not calculate indicators for {symbol}")
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
    res = supabase.table("stock_master").select("symbol").eq("is_active", True).execute()
    symbols = [item['symbol'] for item in res.data]
    
    if not symbols:
        print("📭 No active symbols found in stock_master.")
        return

    for symbol in symbols:
        data = await fetch_data_waterfall(symbol)
        
        if data:
            if not data.get("ema_200"):
                print(f"⚠️ {symbol}: No EMA 200 data")
            
            print(f"📊 Calculating additional metrics for {symbol}...")
            
            upside_pct = calculate_upside_pct(
                data.get("price"), 
                data.get("ema_200"),
                data.get("ema_50")
            )
            
            analyst_pct = fetch_analyst_data(symbol)
            sentiment = fetch_sentiment_score(symbol)
            
            payload = {
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
            
            supabase.table("stock_snapshots").insert(payload).execute()
            print(f"✅ {symbol} | Analyst: {analyst_pct}% | Sentiment: {sentiment}")
        else:
            print(f"❌ Failed: {symbol}")
            
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
