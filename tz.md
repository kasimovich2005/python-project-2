## Talab: Email-gina asosida fake ma'lumotlar bilan ishlash

### 1. Ma’lumotlarni yuklash
- Excel fayl: `fake_raw_communications_email_only.xlsx`
- Har bir qatorda `raw_content` ustunida JSON mavjud bo‘lib, quyidagilarni o‘z ichiga oladi:
  - `speakers`: faqat **email** lar (ism yo‘q)
  - `participants`: email ro‘yxati
  - `meeting_attendees`: email obyektlari (`{"email": ...}`)
  - `host_email`, `organizer_email`: alohida email maydonlar

### 2. Tozalash va JSON parsing
- Har bir `raw_content` JSON qiymati to‘liq `json.loads` bilan parse qilinadi.
- Quyidagi elementlar alohida ustunlarga ajratiladi:
  - `title`, `duration`, `calendar_id`, `audio_url`, `video_url`, `transcript_url`, `dateString`
  - `speakers`, `participants`, `meeting_attendees.email`
  - `host_email`, `organizer_email`

### 3. Dimension jadvallar (DIM)
| Jadval nomi       | Maydonlar |
|--------------------|-----------|
| `dim_comm_type`    | `comm_type_id`, `comm_type` |
| `dim_subject`      | `subject_id`, `subject` |
| `dim_calendar`     | `calendar_id` |
| `dim_audio`        | `audio_id`, `audio_url` |
| `dim_video`        | `video_id`, `video_url` |
| `dim_transcript`   | `transcript_id`, `transcript_url` |
| `dim_user`         | `user_id`, `email` (ism yo‘q) |

### 4. Fact va Bridge jadvallar
| Jadval nomi               | Tavsif |
|---------------------------|--------|
| `fact_communication`      | Har bir aloqa (email, meeting, call, chat) uchun asosiy ma'lumotlar. FK: `comm_type_id`, `subject_id`, `calendar_id`, `audio_id`, `video_id`, `transcript_id`. |
| `bridge_comm_user`        | `comm_id`, `user_id`, va roli (`isSpeaker`, `isParticipant`, `isAttendee`, `isOrganiser`) bo‘yicha ko‘p-ko‘pga bog‘lovchi jadval. |

### 5. Muhim eslatmalar
- **Ismlar yo‘q**, shuning uchun **email asosida deduplikatsiya qilinadi**.
- Har bir yagona email — bu `dim_user` dagi **bitta foydalanuvchi** deb hisoblanadi.
- `user_id` — `uuid4()` asosida belgilanadi.

### 6. Chiqarish
- Har bir dimension va fact/bridge jadval alohida Excel sheet ko‘rinishida chiqariladi (`pandas.ExcelWriter` orqali).
