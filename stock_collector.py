import os
import asyncio
import yfinance as yf
import pandas as pd
import talib
from supabase import create_client, Client
import requests
from datetime import datetime

# --- Configuration ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TWELVE_DATA_KEY = os.getenv("TWELVE_DATA_KEY")
# ⬇️ เพิ่มตรงนี้
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in environment variables")
# ⬆️ 
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

import talib  # ⬅️ เปลี่ยน import

def calculate_technical_indicators(df):
    """คำนวณค่าเทคนิคด้วย TA-Lib"""
    try:
        if len(df) < 200:  # ต้องมีข้อมูลอย่างน้อย 200 แท่ง
            return None
        
        close = df['Close'].values
        high = df['High'].values
        low = df['Low'].values
        
        # คำนวณด้วย talib
        rsi = talib.RSI(close, timeperiod=14)
        macd, macd_signal, _ = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        ema_20 = talib.EMA(close, timeperiod=20)
        ema_50 = talib.EMA(close, timeperiod=50)
        ema_200 = talib.EMA(close, timeperiod=200)
        bb_upper, bb_middle, bb_lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
        
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

async def fetch_data_waterfall(symbol):
    """กลยุทธ์ดึงข้อมูลแบบน้ำตก: yfinance -> Twelve Data"""
    print(f"🔍 Fetching data for {symbol}...")
    
    # --- Source 1: yfinance (Primary) ---
    try:
        # ดึงข้อมูลย้อนหลัง 2 ปีเพื่อให้คำนวณ EMA 200 ได้
        stock = yf.Ticker(symbol)
        df = stock.history(period="2y")
        
        if not df.empty and len(df) >= 2:  # ✅ เช็คว่ามีข้อมูลอย่างน้อย 2 แถว
            tech_data = calculate_technical_indicators(df)
            if tech_data:
                # คำนวณ change_pct จากราคาปิดเมื่อวาน
                prev_close = df['Close'].iloc[-2]
                current_price = tech_data['price']
                change_pct = ((current_price - prev_close) / prev_close) * 100
                
                tech_data['change_pct'] = round(change_pct, 2)  # ✅ ปัดเศษ 2 ตำแหน่ง
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
            resp = requests.get(url, timeout=10)  # ✅ เพิ่ม timeout
            resp.raise_for_status()  # ✅ เช็ค HTTP errors
            
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


def calculate_upside_pct(current_price, ema_200):
    """คำนวณ upside potential จากราคาปัจจุบันถึง EMA 200"""
    if not current_price or not ema_200 or ema_200 == 0:
        return None
    return round(((ema_200 - current_price) / current_price) * 100, 2)


async def main():
    # 1. ดึงรายชื่อหุ้นจาก stock_master
    res = supabase.table("stock_master").select("symbol").eq("is_active", True).execute()
    symbols = [item['symbol'] for item in res.data]
    
    if not symbols:
        print("📭 No active symbols found in stock_master.")
        return

    for symbol in symbols:
        data = await fetch_data_waterfall(symbol)

        if data:
            # คำนวณค่าเพิ่มเติม
                upside_pct = calculate_upside_pct(
                data.get("price"), 
                data.get("ema_200")
            )
            
        if data:
            # 2. บันทึกข้อมูลลง stock_snapshots
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
                "upside_pct": upside_pct,      # ⬅️ ใช้ตัวแปรที่คำนวณแล้ว
                "analyst_buy_pct": None,       # ⬅️ ถ้ามีฟิลด์นี้
                "sentiment_score": None,       # ⬅️ ถ้ามีฟิลด์นี้
                "recorded_at": datetime.now().isoformat()
            }
            
            supabase.table("stock_snapshots").insert(payload).execute()
            print(f"✅ Success: {symbol} via {data['source']}")
        else:
            print(f"❌ Failed: Could not get data for {symbol}")
            
        # หน่วงเวลาสั้นๆ เพื่อถนอม API
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
