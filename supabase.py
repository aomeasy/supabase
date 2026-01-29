import asyncio
import os
from supabase import create_client, Client
import requests
import time

# --- การตั้งค่า (แนะนำให้ใช้ Environment Variables) ---
SUPABASE_URL = "YOUR_SUPABASE_URL"
SUPABASE_KEY = "YOUR_SUPABASE_KEY"
TWELVE_DATA_KEY = "YOUR_TWELVE_DATA_KEY"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

async def fetch_stock_indicators(symbol):
    """ดึงข้อมูลเทคนิคจาก Twelve Data (จัดการ Rate Limit ในตัว)"""
    try:
        # ในฐานะผู้เชี่ยวชาญ SQL การจัดเก็บข้อมูลที่ครบถ้วนจะช่วยให้การ Query ภายหลังทำได้แม่นยำขึ้น
        base_url = "https://api.twelvedata.com"
        # ดึงหลาย Indicators ในครั้งเดียว (ช่วยประหยัดโควตา API)
        params = {
            "symbol": symbol,
            "interval": "1day",
            "apikey": TWELVE_DATA_KEY,
            "outputsize": 1
        }
        
        # ตัวอย่างการดึง RSI (ปรับเพิ่มตัวอื่นๆ ได้ตามโครงสร้าง API)
        # หมายเหตุ: ในโปรเจกต์จริง ควรแยกฟังก์ชันดึงแต่ละตัวและใช้ asyncio.sleep เพื่อเลี่ยง 429 Error
        price_resp = requests.get(f"{base_url}/quote", params=params).json()
        rsi_resp = requests.get(f"{base_url}/rsi", params=params).json()
        
        if "values" in rsi_resp:
            return {
                "symbol": symbol,
                "price": float(price_resp.get("close", 0)),
                "change_pct": float(price_resp.get("percent_change", 0)),
                "rsi": float(rsi_resp["values"][0]["rsi"])
                # เพิ่ม MACD, EMA, BB ต่อได้ที่นี่
            }
    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
    return None

async def main():
    print("🚀 เริ่มกระบวนการ Update Stock Data...")
    
    # 1. ดึงรายชื่อหุ้นจาก Database (Stock Master)
    # วิธีนี้ทำให้รองรับการเพิ่มหุ้นใหม่ในอนาคตได้เพียงแค่เพิ่ม Row ใน Supabase
    response = supabase.table("stock_master").select("symbol").eq("is_active", True).execute()
    symbols = [item['symbol'] for item in response.data]
    
    for symbol in symbols:
        print(f"🔍 Processing {symbol}...")
        
        data = await fetch_stock_indicators(symbol)
        
        if data:
            # 2. บันทึกลง stock_snapshots
            supabase.table("stock_snapshots").insert({
                "symbol": data["symbol"],
                "price": data["price"],
                "change_pct": data["change_pct"],
                "rsi": data["rsi"]
                # ใส่ข้อมูลตัวอื่นๆ ตามที่ออกแบบไว้
            }).execute()
            print(f"✅ Saved {symbol} to Supabase")
        
        # 3. สำคัญมาก: หน่วงเวลาเพื่อเลี่ยง Rate Limit ของ API แผนฟรี
        await asyncio.sleep(8) 

if __name__ == "__main__":
    asyncio.run(main())
