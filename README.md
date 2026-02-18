<div align="center">

# 🔄 Communication Data ETL Pipeline

**Xom kommunikatsiya ma'lumotlarini Star Schema formatiga o'tkazuvchi ETL tizimi**

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![OpenPyXL](https://img.shields.io/badge/OpenPyXL-Excel-217346?style=for-the-badge&logo=microsoftexcel&logoColor=white)](https://openpyxl.readthedocs.io/)
[![ETL](https://img.shields.io/badge/Pattern-ETL_Pipeline-FF6B35?style=for-the-badge)](/)
[![Schema](https://img.shields.io/badge/Schema-Star_Schema-8A2BE2?style=for-the-badge)](/)

</div>

---

## 📌 Loyiha Haqida

Bu loyiha **kommunikatsiya ma'lumotlarini** (uchrashuvlar, emaillar, chatlar, qo'ng'iroqlar) xom CSV/Excel formatidan **Star Schema** arxitekturasiga o'tkazuvchi to'liq **ETL (Extract → Transform → Load)** pipelineini amalga oshiradi.

Natijada `final.xlsx` faylida **7 ta dimension**, **1 ta fact** va **1 ta bridge** jadvali hosil bo'ladi — Power BI yoki boshqa BI vositalar uchun tayyor holda.

---

## 🏗️ Ma'lumotlar Arxitekturasi

```
📥 raw_data.csv / .xlsx
        │
        ▼
┌───────────────────┐
│   EXTRACT (1)     │  → JSON parse, xato tuzatish
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│  TRANSFORM (2-4)  │  → Normalizatsiya, ID yaratish, tipni aniqlash
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│    LOAD (5-6)     │  → final.xlsx (9 varaq)
└───────────────────┘
```

### 📐 Star Schema Diagrammasi

```
                    ┌──────────────────┐
                    │  dim_comm_type   │
                    │  comm_type_id PK │
                    │  comm_type       │
                    └────────┬─────────┘
                             │
  ┌──────────────┐           │        ┌──────────────────┐
  │  dim_subject │           │        │   dim_calendar   │
  │  subject_id  ├───────────┤        │   calendar_id PK │
  │  subject     │           │        │  raw_calendar_id │
  └──────────────┘           │        └────────┬─────────┘
                             │                 │
  ┌──────────────┐    ┌──────┴──────────┐      │
  │  dim_audio   │    │                 │      │
  │  audio_id PK ├────┤ fact_communica- ├──────┘
  │  raw_audio   │    │      tion       │
  └──────────────┘    │                 │   ┌──────────────────┐
                      │  comm_id PK     ├───┤  bridge_comm_    │
  ┌──────────────┐    │  comm_type_id   │   │      user        │
  │  dim_video   ├────┤  subject_id     │   │  comm_id FK      │
  │  video_id PK │    │  calendar_id    │   │  user_id FK      │
  │  raw_video   │    │  audio_id       │   │  isAttendee      │
  └──────────────┘    │  video_id       │   │  isOrganiser     │
                      │  transcript_id  │   │  isParticipant   │
  ┌──────────────┐    │  datetime_id    │   │  isSpeaker       │
  │dim_transcript├────┤  raw_title      │   └────────┬─────────┘
  │transcript_id │    │  raw_duration   │            │
  │raw_transcript│    └─────────────────┘            │
  └──────────────┘                          ┌────────┴─────────┐
                                            │    dim_user      │
                                            │  user_id PK      │
                                            │  email           │
                                            └──────────────────┘
```

---

## 📂 Fayl Strukturasi

```
📁 python-project-2/
├── 📄 main.py              # Asosiy ETL skript
├── 📄 raw_data.csv         # Kiruvchi xom ma'lumotlar
├── 📄 final.xlsx           # Natija (9 varaq)
└── 📄 README.md
```

---

## ⚙️ ETL Jarayoni — Batafsil

### 1️⃣ Extract — Ma'lumotlarni Yuklash

- CSV yoki Excel faylni avtomatik aniqlash va yuklash
- Har bir qatordagi `raw_content` ustunidan JSON parse qilish
- Buzuq JSON uchun **avtomatik tuzatish mexanizmi** (figurali qavslarni balanslashtirish)

```python
# JSON xato bo'lsa, avtomatik tuzatadi
start_idx = content.find("{")
# ... brace balancing algoritmi
parsed_json = json.loads(clean_content)
```

### 2️⃣ Transform — Ma'lumotlarni O'zgartirish

**Kommunikatsiya turini avtomatik aniqlash:**

| Shart | Tur |
|---|---|
| `audio_url` yoki `video_url` mavjud | `meeting` |
| `title` da `@` yoki `email` so'zi | `email` |
| `title` da `chat` so'zi | `chat` |
| `title` da `call` so'zi | `call` |
| Boshqa holat | `unknown` |

**Foydalanuvchilarni aniqlash:**
```
speakers → participants → meeting_attendees → host → organizer
                    ↓
            Barcha unique emaillar yig'iladi
                    ↓
           UUID orqali user_id yaratiladi
```

### 3️⃣ Load — Excel ga Yozish

`final.xlsx` faylida **9 ta varaq:**

| Varaq | Tavsif | Kalit ustunlar |
|---|---|---|
| `dim_comm_type` | Kommunikatsiya turlari | `comm_type_id`, `comm_type` |
| `dim_subject` | Mavzular | `subject_id`, `subject` |
| `dim_calendar` | Kalendar ma'lumotlari | `calendar_id`, `raw_calendar_id` |
| `dim_audio` | Audio fayllar | `audio_id`, `raw_audio_url` |
| `dim_video` | Video fayllar | `video_id`, `raw_video_url` |
| `dim_transcript` | Transkripsiyalar | `transcript_id`, `raw_transcript_url` |
| `dim_user` | Foydalanuvchilar | `user_id` (UUID), `email` |
| `fact_communication` | Asosiy fact jadval | `comm_id`, barcha FK lar |
| `bridge_comm_user` | Ko'p-ko'plik bog'lanish | `comm_id`, `user_id`, rollar |

---

## 🚀 Ishga Tushirish

### Talablar

```bash
pip install pandas openpyxl
```

### 1. Faylni sozlash

`main.py` ichida fayl yo'llarini ko'rsating:

```python
input_file  = r"E:\your\path\raw_data.csv"    # Kiruvchi fayl
output_file = r"E:\your\path\final.xlsx"       # Chiquvchi fayl
```

### 2. Ishga tushirish

```bash
python main.py
```

### 3. Kutilayotgan natija

```
=== Communication Data ETL Jarayoni ===
1. Ma'lumotlarni yuklash...
Yuklandi: 150 qator
Muvaffaqiyatli parse qilindi: 148 qator
2. Ma'lumotlarni transform qilish...
Transform tugadi:
  - Fact communications: 148
  - Bridge records: 892
  - Unique users: 74
6. Excel ga export qilish...
  - dim_comm_type: 4 qator
  - dim_user: 74 qator
  - fact_communication: 148 qator
  - bridge_comm_user: 892 qator
Export tugadi: final.xlsx
=== ETL Jarayoni Tugadi ===
```

---

## 🧩 Kiruvchi Ma'lumot Formati

`raw_data.csv` faylida `raw_content` ustuni quyidagi JSON strukturasida bo'lishi kerak:

```json
{
  "id": "abc-123",
  "title": "Weekly Team Standup",
  "duration": 3600,
  "dateString": "2024-01-15",
  "calendar_id": "cal-001",
  "audio_url": "https://storage.example.com/audio/abc.mp3",
  "video_url": "https://storage.example.com/video/abc.mp4",
  "transcript_url": "https://storage.example.com/transcript/abc.txt",
  "host_email": "host@company.com",
  "organizer_email": "organizer@company.com",
  "speakers": [
    {"email": "speaker1@company.com"},
    {"email": "speaker2@company.com"}
  ],
  "participants": [
    {"email": "participant@company.com"}
  ],
  "meeting_attendees": [
    {"email": "attendee@company.com"}
  ]
}
```

---

## 🛡️ Xatolarni Boshqarish

| Holat | Yechim |
|---|---|
| Fayl topilmadi | `FileNotFoundError` bilan aniq xabar |
| Buzuq JSON | Avtomatik brace-balancing tuzatish |
| Bo'sh `raw_content` | O'tkazib yuboriladi, log yoziladi |
| Noma'lum ustun | `ValueError` bilan aniq xabar |
| Sana yoki email yo'q | `None` saqlanadi, jarayon davom etadi |

---

## 🔧 Texnik Tafsilotlar

### Identifikatorlar

```python
# Dimension jadvallar uchun → integer counter
comm_type_id = 1, 2, 3, ...

# Foydalanuvchilar uchun → UUID (takrorlanmas)
user_id = "550e8400-e29b-41d4-a716-446655440000"

# Fact va Bridge uchun → UUID
comm_id = "f47ac10b-58cc-4372-a567-0e02b2c3d479"
```

### Email Normalizatsiyasi

```python
email = str(email).strip().lower()  # Katta-kichik harf farqsiz
# Dublikatlar Set orqali olib tashlanadi
all_emails = set(speakers + participants + attendees)
```

---

## 📊 Ishlatish — Power BI integratsiyasi

`final.xlsx` ni Power BI ga ulaganingizdan so'ng:

```
1. Get Data → Excel → final.xlsx
2. Barcha 9 varaqni yuklang
3. Model view da munosabatlar:
   fact_communication.comm_type_id → dim_comm_type.comm_type_id
   fact_communication.subject_id   → dim_subject.subject_id
   fact_communication.comm_id      → bridge_comm_user.comm_id
   bridge_comm_user.user_id        → dim_user.user_id
   ... va h.k.
```

---

## 🗺️ Rivojlanish Rejasi

- [x] CSV va Excel fayllarni qo'llab-quvvatlash
- [x] JSON avtomatik tuzatish
- [x] Star Schema arxitekturasi
- [x] UUID asosidagi user identifikatsiya
- [x] Bridge table (ko'p-ko'plik)
- [ ] PostgreSQL / SQL Server ga to'g'ridan-to'g'ri yuklash
- [ ] CLI argumentlar orqali fayl yo'llarini berish
- [ ] Logging moduli (`loguru` yoki `logging`)
- [ ] Unit testlar (`pytest`)
- [ ] Konfiguratsiya fayli (`config.yaml`)
- [ ] Docker konteyner

---

## 📦 Kutubxonalar

| Kutubxona | Versiya | Maqsad |
|---|---|---|
| `pandas` | `>=1.5` | Ma'lumot yuklash va transformatsiya |
| `openpyxl` | `>=3.0` | Excel yozish |
| `uuid` | built-in | Unikal ID yaratish |
| `json` | built-in | JSON parse qilish |
| `os` | built-in | Fayl tizimi operatsiyalari |

---

<div align="center">

👤 **Muallif:** [@kasimovich2005](https://github.com/kasimovich2005)

📧 sardorbozorqulov636@gmail.com | 💬 [@kasimovich_s](https://t.me/kasimovich_s)

⭐ Foydali bo'lsa **star** bosishni unutmang!

</div>
