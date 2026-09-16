# 📊 StockAI Data Collector — เอกสารสรุปโปรเจกต์

**Repository:** `aomeasy/supabase`
**ไฟล์หลัก:** `stock_collector.py` (3,557 บรรทัด)
**จัดทำเมื่อ:** กันยายน 2026

---

## สารบัญ

1. [ภาพรวมโปรเจกต์](#1-ภาพรวมโปรเจกต์)
2. [สถาปัตยกรรมระบบ](#2-สถาปัตยกรรมระบบ)
3. [อธิบายโค้ดแบบละเอียด](#3-อธิบายโค้ดแบบละเอียด)
4. [ระบบให้คะแนนหุ้น (Scoring Engine)](#4-ระบบให้คะแนนหุ้น-scoring-engine)
5. [ตารางเวลาการทำงาน (Schedule)](#5-ตารางเวลาการทำงาน-schedule)
6. [การแจ้งเตือน Telegram](#6-การแจ้งเตือน-telegram)
7. [ปัญหาด้านความปลอดภัยที่พบและแก้ไข](#7-ปัญหาด้านความปลอดภัยที่พบและแก้ไข)
8. [ปัญหาเชิงเทคนิคที่พบ](#8-ปัญหาเชิงเทคนิคที่พบ)
9. [Checklist สิ่งที่ต้องทำต่อ](#9-checklist-สิ่งที่ต้องทำต่อ)
10. [โครงสร้างไฟล์ Environment Variables / Secrets](#10-โครงสร้างไฟล์-environment-variables--secrets)

---

## 1. ภาพรวมโปรเจกต์

โปรเจกต์นี้คือ **บอทวิเคราะห์หุ้นอัตโนมัติ** ที่ทำงาน 2 โหมดจากไฟล์ Python เดียวกัน:

| โหมด | คำสั่งรัน | หน้าที่ |
|---|---|---|
| **Collector** | `python stock_collector.py --collect` | เก็บข้อมูลหุ้นตามตาราง `stock_master` ทั้งหมด วิเคราะห์ บันทึกลง Supabase และส่งสรุปเข้า Telegram อัตโนมัติตาม schedule |
| **Bot** | `python stock_collector.py --bot` | รันเป็น Telegram bot แบบ polling รอรับคำสั่งจากผู้ใช้ (พิมพ์ symbol เพื่อขอวิเคราะห์แบบ real-time) |

โปรเจกต์รันบน **GitHub Actions** เป็นหลัก โดยไม่ต้องมีเซิร์ฟเวอร์แยกต่างหาก

---

## 2. สถาปัตยกรรมระบบ

```
┌─────────────────┐
│  GitHub Actions  │
│   (Scheduler)    │
└────────┬─────────┘
         │ trigger ตาม cron / workflow_dispatch
         ▼
┌─────────────────────────────────────────┐
│         stock_collector.py               │
│                                           │
│  ┌──────────────┐    ┌────────────────┐ │
│  │   Collector   │    │   Telegram Bot  │ │
│  │  (--collect)  │    │     (--bot)     │ │
│  └──────┬───────┘    └────────┬────────┘ │
│         │                     │           │
└─────────┼─────────────────────┼───────────┘
          │                     │
    ┌─────▼──────┐       ┌──────▼──────┐
    │  ดึงข้อมูล   │       │  ตอบ user    │
    │            │       │  แบบ realtime │
    └─────┬──────┘       └─────────────┘
          │
    ┌─────▼─────────────────────────┐
    │   แหล่งข้อมูลภายนอก              │
    │  • yfinance (ราคา/fundamental) │
    │  • Twelve Data (fallback ราคา) │
    │  • Finnhub (ข่าว)               │
    │  • Google Translate (แปลข่าว)   │
    └─────┬──────────────────────────┘
          │
    ┌─────▼──────┐        ┌─────────────┐
    │  Supabase   │        │  Telegram    │
    │  (Database) │        │  (แจ้งเตือน)  │
    └─────────────┘        └─────────────┘
```

### ตารางฐานข้อมูล (Supabase) ที่ใช้

| ตาราง | หน้าที่ |
|---|---|
| `stock_master` | รายชื่อหุ้นที่ต้องติดตาม + category (Core/Growth/Value/Dividend/ETF/Momentum) |
| `stock_snapshots` | บันทึกราคา + indicator ทางเทคนิคทุกครั้งที่เก็บข้อมูล |
| `stock_news` | ข่าวที่ดึงมาจาก Finnhub พร้อม sentiment score |
| `ai_predictions` | ผลการวิเคราะห์ + คำแนะนำ (Buy/Sell/Hold) ในแต่ละรอบ |
| `fx_snapshots` | อัตราแลกเปลี่ยน USD/THB |

---

## 3. อธิบายโค้ดแบบละเอียด

### 3.1 ส่วนตั้งค่า (บรรทัด 1–30)

```python
SUPABASE_URL, SUPABASE_KEY     → เชื่อมต่อฐานข้อมูล Supabase (Postgres)
TWELVE_DATA_KEY, FINNHUB_KEY   → API key สำหรับดึงราคาหุ้น/ข่าวสำรอง
TELEGRAM_BOT_TOKEN/CHAT_ID     → เชื่อม Telegram
```

ทุกค่าดึงจาก environment variable ผ่าน `os.getenv()` — **ไม่มีการ hardcode ค่าใดๆ ในโค้ดแล้ว** (แก้ไขจากปัญหาที่พบก่อนหน้า ดูหัวข้อ 7)

มีการ validate ตั้งแต่ต้นไฟล์:
```python
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY")
```
→ ถ้าขาดค่าจำเป็น โปรแกรมจะหยุดทันทีตั้งแต่ต้น ไม่รันไปครึ่งทางแล้วค่อยพัง

### 3.2 ฟังก์ชันดึงข้อมูล (Data Fetching Layer)

| ฟังก์ชัน | หน้าที่ | Fallback |
|---|---|---|
| `fetch_data_waterfall(symbol)` | ดึงราคา + indicator หลัก | yfinance → Twelve Data |
| `calculate_technical_indicators(df)` | คำนวณ RSI, MACD, EMA(20/50/200), Bollinger Bands, ATR, Stochastic, Volume/OBV/VWAP | ต้องมีข้อมูลย้อนหลัง ≥200 วัน ไม่งั้น return `None` |
| `fetch_fundamental_data(symbol)` | ดึง P/E, PEG, EPS growth, market cap | — |
| `fetch_analyst_data(symbol)` | ดึง % นักวิเคราะห์แนะนำซื้อ | 3 ชั้น: `get_recommendations()` → `recommendations` → `recommendationMean` |
| `fetch_news_data(symbol)` | ดึงข่าวจาก Finnhub (ย้อนหลัง 7 วัน, สูงสุด 20 ข่าว) + แปลไทยด้วย Google Translate + คำนวณ sentiment จาก keyword matching | ถ้าแปลไม่ได้ ใช้ภาษาอังกฤษเดิม |
| `fetch_macro_data()` | ดึงภาพรวมตลาด: VIX, S&P500(^GSPC), QQQ, XLK, Bond 10Y(^TNX), USD/THB | แยก error handling รายตัว ไม่ให้ตัวหนึ่งพังแล้วทั้งชุดพัง |
| `fetch_sentiment_score(symbol)` | คำนวณ sentiment จากข่าวของ yfinance โดยตรง (สำรองจาก Finnhub) | — |

**จุดเด่นด้านการออกแบบ:** ทุกฟังก์ชันมี `try/except` ครอบไว้หมด ถ้าแหล่งข้อมูลหนึ่งล่ม ฟังก์ชันจะ return `None`/`{}`/`[]` แทนที่จะทำให้โปรแกรมทั้งตัว crash (defensive coding)

### 3.3 ฟังก์ชันวิเคราะห์เดี่ยว vs วิเคราะห์แบบ batch

- `analyze_single_stock(symbol)` — ใช้ในโหมด `--bot` เมื่อ user พิมพ์ symbol เข้ามา วิเคราะห์ตัวเดียว **ไม่บันทึกลง DB**
- ภายใน loop ของ `main()` — ใช้ในโหมด `--collect` วิเคราะห์ทุกหุ้นใน `stock_master` แล้ว **บันทึกทุกอย่างลง Supabase**

### 3.4 การจัดการ Telegram Bot (โหมด `--bot`)

```python
start_telegram_bot()
```
ทำงานแบบ **polling** (เช็คข้อความใหม่ทุก 1 วินาที) รองรับคำสั่ง:

| คำสั่ง | หน้าที่ |
|---|---|
| `/start` | ทักทาย แนะนำวิธีใช้ |
| `/help` | อธิบายวิธีใช้แบบละเอียด |
| `/status` | เช็คว่าบอทออนไลน์อยู่ |
| พิมพ์ symbol เช่น `AAPL` | validate (1–5 ตัวอักษร, ตัวอักษรล้วน) → เรียก `analyze_single_stock()` → timeout 90 วินาที → ตอบกลับด้วย `format_stock_analysis_message()` |

**Error handling หลายชั้น:** ถ้าส่งข้อความแบบ HTML formatting ไม่สำเร็จ จะลอง fallback เป็น plain text ให้อัตโนมัติ

---

## 4. ระบบให้คะแนนหุ้น (Scoring Engine)

นี่คือ "สมอง" ของบอท คำนวณคะแนนรวม 0–100 จาก 3 องค์ประกอบ:

```
Overall Score = Technical(0-40) × น้ำหนัก + Fundamental(0-30) × น้ำหนัก + Sentiment(0-30) × น้ำหนัก
```

### 4.1 การให้คะแนนแต่ละส่วน

**Technical Score (0–40)** — `calculate_technical_score()`
- RSI อยู่ในโซนสมดุล (30–70): +10
- MACD > Signal: +10
- ราคา > EMA20 > EMA50: +10
- Upside potential > 20%: +10
- Volume confirmation: ±5 (bonus/penalty)
- Stochastic confirmation: ±3

**Fundamental Score (0–30)** — `calculate_fundamental_score()`
- P/E ratio 10–25: +10
- PEG ratio < 1: +10
- EPS growth > 20%: +10

**Sentiment Score (0–30)** — `calculate_sentiment_score()`
- News sentiment > 0.5: +15
- Analyst buy % ≥ 70%: +15

### 4.2 การถ่วงน้ำหนักแบบ Dynamic

`get_scoring_weights()` ปรับน้ำหนักอัตโนมัติตาม **Market Cap**:

| Market Cap | Technical | Fundamental | Sentiment |
|---|---|---|---|
| > $200B (Large Cap) | 25% | 40% | 35% |
| $10B–$200B (Mid Cap) | 35% | 35% | 30% |
| < $10B (Small Cap) | 50% | 30% | 20% |
| ETF | 100% | 0% | 0% |

### 4.3 การปรับคะแนนตามความเสี่ยง

`calculate_risk_score()` คำนวณความเสี่ยง 0–100 จาก:
- RSI overbought/oversold (+15 ถึง +30)
- ราคาทะลุ Bollinger Bands (+15 ถึง +20)
- Market cap เล็ก (+10 ถึง +25)
- P/E หรือ PEG สูงเกินไป (+15 ถึง +20)

จากนั้น `adjust_score_by_risk()` หักคะแนนสุดท้ายลง:

| Risk Score | ผลกระทบ |
|---|---|
| ≥70 (สูงมาก) | ลดคะแนน 30% |
| ≥50 (ปานกลาง) | ลดคะแนน 15% |
| ≥30 (เล็กน้อย) | ลดคะแนน 5% |
| <30 (ต่ำ) | ไม่ลด |

### 4.4 Logic พิเศษ: "Alert Only"

`generate_recommendation_advanced()` มี logic ป้องกันการซื้อเร็วเกินไป:

> ถ้าคะแนนดีมาก + มี MACD Crossover + RSI>50 **แต่ราคายังต่ำกว่า EMA200** → ระบบจะไม่ให้คำแนะนำ "Buy" เต็มรูปแบบ แต่เปลี่ยนเป็น **"Alert Only"** พร้อมคำเตือนให้เปิดแค่ 5–10% ของพอร์ตก่อน (Pilot Position) รอให้ราคาทะลุ EMA200 จริงๆ ค่อยเพิ่มเต็มที่

ระบบยังคำนวณ **Stop Loss แบบ ATR-based** (2×ATR จากราคาปัจจุบัน) และ **Risk/Reward Ratio** ให้อัตโนมัติด้วย

---

## 5. ตารางเวลาการทำงาน (Schedule)

Workflow ชื่อ **"StockAI Data Collector"** ตั้ง cron ไว้ 5 รอบต่อวัน (เวลาไทย, จันทร์–ศุกร์ตามเวลาตลาดหุ้นสหรัฐฯ):

| รอบ | เวลาไทย | Cron (UTC) | จุดประสงค์ |
|---|---|---|---|
| 1 | **21:00 น.** | `0 14 * * 1-5` | ก่อนตลาดเปิด — วิเคราะห์ข่าว Pre-market |
| 2 | **22:00 น.** | `0 15 * * 1-5` | หลังตลาดเปิด 30 นาที — จับ momentum เริ่มต้น |
| 3 | **00:30 น.** (วันถัดไป) | `30 17 * * 1-5` | กลางวันฝั่งสหรัฐฯ — เช็ค intraday trend |
| 4 | **03:30 น.** (วันถัดไป) | `30 20 * * 2-6` | ก่อนตลาดปิด — เตรียมปิดสถานะ |
| 5 | **07:30 น.** (วันถัดไป) | `30 0 * * 2-6` | หลังตลาดปิด — สรุปผลวัน + After-hours |

> **หมายเหตุ:** วันที่ cron ไม่ตรงกัน (`1-5` vs `2-6`) เพราะเวลาไทยเร็วกว่า UTC 7 ชั่วโมง รอบที่ 3–5 ข้ามเที่ยงคืน UTC ไปแล้วจึงต้องเลื่อนวันในสัปดาห์ ผลลัพธ์คือครอบคลุมช่วง **จันทร์เย็น–เสาร์เช้าตี 7 ตามเวลาไทย** พอดีกับเวลาตลาด NYSE

นอกจากนี้ยังมี `workflow_dispatch:` ให้กดรันเองได้ตลอดเวลาโดยไม่ต้องรอ schedule

### ⚠️ ข้อควรระวัง: GitHub ปิด Scheduled Workflow อัตโนมัติ

**ถ้า repository ไม่มี commit ใหม่เกิน 60 วัน GitHub จะปิด scheduled workflow ให้อัตโนมัติ** (ไฟล์ยังอยู่ครบ แต่ cron จะไม่ trigger จนกว่าจะกด "Enable workflow" ด้วยมือ) — นี่คือสาเหตุที่ workflow เคยหยุดทำงานไปช่วงหนึ่ง

**วิธีป้องกันระยะยาว:** เพิ่ม step ท้าย workflow ให้ commit ไฟล์ timestamp กลับเข้า repo ทุกครั้งที่รัน เพื่อ "reset" นาฬิกา 60 วันอัตโนมัติ (ดูตัวอย่างโค้ดในหัวข้อ 9)

---

## 6. การแจ้งเตือน Telegram

บอทส่งข้อความเข้า Telegram ผ่านฟังก์ชัน `send_telegram_message()` ใน **4 กรณี** ต่อการรันหนึ่งรอบ (โหมด `--collect`):

| # | จุดที่ส่ง | เงื่อนไข | ความถี่ |
|---|---|---|---|
| 1 | **เริ่มรัน** | ทุกครั้งที่เริ่มโหมด `--collect` | 1 ครั้ง/รอบ |
| 2 | **Strong Buy Alert** | Score≥75 + Risk<50 + MACD crossover + RSI>50 + ราคาเหนือ EMA200 ครบทุกเงื่อนไข | ตามจำนวนหุ้นที่เข้าเงื่อนไข (อาจไม่มีเลย) |
| 3 | **สรุปผล (Summary)** | หลังวิเคราะห์ครบทุกหุ้นในรอบนั้น | 1 ครั้ง/รอบ |
| 4 | **Market Alert** | ส่งเสมอท้ายสุดของทุกรอบ (ถ้าไม่เจอโอกาสก็ส่งสรุปภาพรวมตลาดแทน) | 1 ครั้ง/รอบ |

**รวมแล้ว:** วันธรรมดา 1 วัน (5 รอบ) จะได้รับข้อความประมาณ **10–15 ข้อความ** ขึ้นอยู่กับจำนวนหุ้น Strong Buy ที่เจอในแต่ละรอบ

**หมายเหตุ:** โหมด `--bot` (ตอบ user ที่พิมพ์ symbol เข้ามา) ใช้ `update.message.reply_text()` แยกต่างหาก **ไม่ใช้** `send_telegram_message()` กับ `TELEGRAM_CHAT_ID` — ดังนั้นปัญหาการส่งข้อความ (เช่น 403 Forbidden) ในโหมด collector จะไม่กระทบการตอบกลับในโหมด bot

---

## 7. ปัญหาด้านความปลอดภัยที่พบและแก้ไข

### 🚨 7.1 Telegram Bot Token ฝังอยู่ในโค้ดโดยตรง (แก้ไขแล้ว ✅)

**ปัญหาเดิม:**
```python
TELEGRAM_BOT_TOKEN = "8473805508:AAE7FqIeUl_H0vdMuIzfHMld_rIfBSUPpbw"
```
Token ถูก hardcode ใน public repository ทำให้ **GitHub Secret Scanning ตรวจพบ 2 alert** เป็น "Public leak" ทั้งคู่ (Bot ID เดียวกัน `8473805508` แต่ต่าง token กัน — แปลว่าเคย regenerate token มาแล้ว 1 ครั้งแต่ก็ยัง commit ค่าจริงซ้ำอีก)

**ความเสี่ยงที่เกิดขึ้นได้:**
- ใครก็ตามที่เห็น token สามารถส่งข้อความปลอมออกจากบอท (ฟิชชิ่ง/สแปม)
- อ่านข้อความที่ user ส่งเข้ามาหาบอท
- แก้ไข logic การแนะนำซื้อ-ขาย → สร้างความเสียหายทางการเงินให้ผู้ใช้จริง

**การแก้ไข:**
1. Revoke token เดิมที่ @BotFather (`/mybots` → เลือกบอท → API Token → Revoke current token)
2. แก้โค้ดเป็น `TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")`
3. ตั้ง token ใหม่เป็น GitHub Secret
4. อัปเดต `env:` ใน workflow ให้ดึงจาก secret

### 🐛 7.2 `TELEGRAM_CHAT_ID` ตั้งค่าผิด (แก้ไขแล้ว ✅)

**ปัญหาเดิม:** เข้าใจผิดว่า Bot ID (`8473805508`) คือ chat_id ทำให้บอทพยายามส่งข้อความหาตัวเอง เกิด `403 Forbidden`

**วิธีหา chat_id ที่ถูกต้อง:**
1. ส่งข้อความอะไรก็ได้ไปหาบอทก่อน (ต้องทำก่อนเสมอ)
2. เปิด `https://api.telegram.org/bot<TOKEN>/getUpdates`
3. หาค่าใน `"chat": {"id": ...}` — นี่คือ chat_id จริง (เป็นเลขของผู้ใช้ ไม่ใช่ของบอท)

**ผลลัพธ์:** chat_id ที่ถูกต้องคือ `8028926248` (คนละตัวกับ Bot ID `8473805508`)

### ⚠️ 7.3 การวาง Token ซ้ำในแชท (เตือนแล้ว)

ระหว่างการ debug มีการวาง token จริงในแชท 2 ครั้ง — แนะนำให้ revoke token เหล่านั้นด้วยเช่นกัน เนื่องจากแชทก็ถือเป็นข้อความที่ถูกบันทึกไว้ ไม่ต่างจาก public repository ในแง่ความเสี่ยงของการรั่วไหล

---

## 8. ปัญหาเชิงเทคนิคที่พบ

| # | ปัญหา | รายละเอียด | สถานะ |
|---|---|---|---|
| 1 | `import talib` ไม่ได้ใช้งาน | อยู่หัวไฟล์แต่โค้ดทั้งหมดใช้ library `ta` แทน ถ้า TA-Lib C library ไม่มีในเครื่องที่รัน จะทำให้ `import` พังตั้งแต่บรรทัดแรก | ⏳ ยังไม่แก้ (แนะนำลบทิ้ง) |
| 2 | Bot polling ใน GitHub Actions มี timeout | โหมด `--bot` วนลูป `while True` ไม่มีวันจบ แต่ job timeout ที่ 170 นาที ทำให้ต้องกดรันใหม่ด้วยมือทุก ~3 ชั่วโมง ไม่เหมาะกับบอทที่ต้องออนไลน์ 24/7 | ⏳ ทราบข้อจำกัดแล้ว (แนะนำย้ายไป Railway/Render/VPS ถ้าต้องการ 24/7 จริง) |
| 3 | `TELEGRAM_BOT_TOKEN` ขาดใน workflow env | ไฟล์ `StockAI Data Collector.yml` เดิมไม่ได้ส่ง `TELEGRAM_BOT_TOKEN` เข้า `env:` (มีแต่ `TELEGRAM_CHAT_ID`) ทำให้การแจ้งเตือนเงียบแม้ data collection จะทำงานปกติ | ✅ ระบุจุดแก้แล้ว |
| 4 | GEMINI_API_KEY 1–5 ไม่ถูกใช้ในโค้ดที่ตรวจสอบ | Workflow ส่ง secret 5 ตัวเข้าไปแต่ในไฟล์ `stock_collector.py` ที่ตรวจสอบไม่มีการเรียกใช้ Gemini เลย | ⏳ รอยืนยัน (อาจเป็นโค้ดเวอร์ชันใหม่กว่าที่ยังไม่ได้ตรวจ หรือ secret ค้างจากของเก่า) |
| 5 | `fetch_analyst_data()` ใช้ `stock.get_recommendations()` | เมธอดนี้มีเฉพาะ yfinance เวอร์ชันใหม่ ต้องเช็ค `requirements.txt` ว่า pin เวอร์ชันที่รองรับหรือไม่ | ⏳ รอตรวจ `requirements.txt` |
| 6 | GitHub ปิด scheduled workflow อัตโนมัติหลัง 60 วันไม่มี activity | เคยเกิดขึ้นจริงกับ workflow "StockAI Data Collector" | ✅ เปิดกลับมาแล้ว + วางแผนป้องกันซ้ำ |
| 7 | Rate limit เสี่ยงจากการแปลข่าว | `GoogleTranslator` แปลข่าวทีละอันแบบ synchronous ถ้าหุ้น+ข่าวเยอะ อาจโดน Google บล็อกชั่วคราว | ⏳ ยังไม่แก้ (มี try/except ครอบไว้ไม่ให้พังทั้งระบบ) |

---

## 9. Checklist สิ่งที่ต้องทำต่อ

- [ ] ลบ `import talib` ออกจากไฟล์ (ไม่ได้ใช้งานจริง)
- [ ] ยืนยันว่า `GEMINI_API_KEY_1-5` ยังใช้งานอยู่จริงหรือไม่ ถ้าไม่ใช้ให้ลบ secret ทิ้ง
- [ ] เพิ่ม step ป้องกัน workflow ถูกปิดอัตโนมัติ (ตัวอย่างด้านล่าง)
- [ ] เช็ค `requirements.txt` ว่า `yfinance` version รองรับ `get_recommendations()`
- [ ] พิจารณาย้ายโหมด `--bot` ไปรันบน Railway/Render/VPS ถ้าต้องการให้ตอบแชทได้ตลอด 24 ชม. จริง
- [ ] เปิด **Dependabot alerts** ใน repo (ปัจจุบัน Disabled) เพื่อรับแจ้งเตือนช่องโหว่ของ library
- [ ] Revoke token ที่เคยวางในแชทระหว่าง debug (ถ้ายังไม่ได้ทำ)

### ตัวอย่าง Step ป้องกัน Workflow ถูกปิดอัตโนมัติ

เพิ่มต่อท้าย step "Run Scraper Script" ใน `.github/workflows/*.yml`:

```yaml
- name: Keep repo active (prevent 60-day auto-disable)
  run: |
    git config user.name "github-actions[bot]"
    git config user.email "github-actions[bot]@users.noreply.github.com"
    echo "Last run: $(date -u '+%Y-%m-%d %H:%M:%S UTC')" > .github/last_run.txt
    git add .github/last_run.txt
    git commit -m "chore: keep workflow active [skip ci]" || echo "Nothing to commit"
    git push
```

> **ข้อกำหนดก่อนใช้:** ต้องตั้งค่า Settings → Actions → General → Workflow permissions เป็น **"Read and write permissions"** ก่อน ไม่งั้นจะ push ไม่ผ่าน

---

## 10. โครงสร้างไฟล์ Environment Variables / Secrets

| Secret Name | ใช้ที่ไหน | จำเป็น? |
|---|---|---|
| `SUPABASE_URL` | เชื่อมต่อฐานข้อมูล | ✅ จำเป็น (โปรแกรมหยุดถ้าไม่มี) |
| `SUPABASE_KEY` | เชื่อมต่อฐานข้อมูล | ✅ จำเป็น |
| `TELEGRAM_BOT_TOKEN` | ส่งข้อความ Telegram | ✅ จำเป็นสำหรับการแจ้งเตือน |
| `TELEGRAM_CHAT_ID` | ปลายทางที่จะส่งข้อความ | ✅ จำเป็นสำหรับการแจ้งเตือน |
| `FINNHUB_KEY` | ดึงข่าวหุ้น | ⚠️ ไม่มีก็รันได้ แต่ข้ามส่วนข่าว |
| `TWELVE_DATA_KEY` | Fallback ดึงราคาหุ้น | ⚠️ ไม่มีก็รันได้ ถ้า yfinance ใช้งานได้ปกติ |
| `GEMINI_API_KEY_1` ถึง `_5` | ไม่พบการใช้งานในโค้ดที่ตรวจสอบ | ❓ รอยืนยัน |

---

*เอกสารนี้จัดทำจากการตรวจสอบโค้ด `stock_collector.py`, ไฟล์ workflow `.github/workflows/`, และการ debug ปัญหาจริงร่วมกันตลอดการสนทนา*
