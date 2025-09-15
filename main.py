import pandas as pd
import os
import json
import uuid
from typing import Dict, List, Set, Any
import warnings
warnings.filterwarnings('ignore')

df = pd.read_csv(r"E:\MAAB\python\project\raw_data.csv")

current_dir = os.path.dirname(os.path.abspath(__file__))

# Fayl yo‘llarini to‘g‘ri yaratish
input_file = os.path.join(current_dir, "raw_data.csv")
output_file = os.path.join(r"E:\MAAB\python\project\final.xlsx")

class CommunicationETL:
    def __init__(self):
        # Storage for dimensions
        self.dim_comm_type = []
        self.dim_subject = []
        self.dim_calendar = []
        self.dim_audio = []
        self.dim_video = []
        self.dim_transcript = []
        self.dim_user = []
        
        # Storage for fact and bridge tables
        self.fact_communication = []
        self.bridge_comm_user = []
        
        # Lookup dictionaries for IDs
        self.comm_type_lookup = {}
        self.subject_lookup = {}
        self.calendar_lookup = {}
        self.audio_lookup = {}
        self.video_lookup = {}
        self.transcript_lookup = {}
        self.user_lookup = {}
        
        # Counters for IDs
        self.comm_type_counter = 1
        self.subject_counter = 1
        self.calendar_counter = 1
        self.audio_counter = 1
        self.video_counter = 1
        self.transcript_counter = 1

    def extract_data(self, file_path: str) -> List[Dict]:
        """1. Faylni yuklash va JSON parse qilish"""
        print("1. Ma'lumotlarni yuklash...")

        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Fayl topilmadi: {file_path}")

            # Fayl extension tekshirish
            if file_path.endswith(".csv"):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path, engine="openpyxl")

            print(f"Yuklandi: {len(df)} qator")

            if "raw_content" not in df.columns:
                raise ValueError("'raw_content' ustuni topilmadi!")

            parsed_data = []
            for idx, row in df.iterrows():
                try:
                    if pd.notna(row["raw_content"]):
                        content = str(row["raw_content"]).strip()
                        try:
                            parsed_json = json.loads(content)
                        except json.JSONDecodeError as e:
                            print(f"JSON xatosi {idx}-qatorda, tuzatishga harakat qilinmoqda...")
                            start_idx = content.find("{")
                            if start_idx != -1:
                                brace_count, end_idx = 0, start_idx
                                for i in range(start_idx, len(content)):
                                    if content[i] == "{":
                                        brace_count += 1
                                    elif content[i] == "}":
                                        brace_count -= 1
                                        if brace_count == 0:
                                            end_idx = i
                                            break
                                clean_content = content[start_idx:end_idx+1]
                                parsed_json = json.loads(clean_content)
                                print(f"  Tuzatildi!")
                            else:
                                raise e
                        parsed_data.append({
                            "original_index": idx,
                            "parsed_data": parsed_json
                        })
                except Exception as e:
                    print(f"Xato {idx}-qatorda: {e}")

            if not parsed_data:
                raise ValueError("Hech qanday ma'lumot parse qilinmadi!")

            print(f"Muvaffaqiyatli parse qilindi: {len(parsed_data)} qator")
            return parsed_data

        except Exception as e:
            print(f"Fayl yuklashda xato: {e}")
            raise

    # def extract_data(self, file_path: str) -> List[Dict]:
        """1. Faylni yuklash va JSON parse qilish"""
        print("1. Ma'lumotlarni yuklash...")
        
        try:
            # Fayl mavjudligini tekshirish
            if not pd.io.common.file_exists(file_path):
                raise FileNotFoundError(f"Fayl topilmadi: {file_path}")
            
            # Excel faylni yuklash
            df = pd.read_excel(file_path)
            print(f"Yuklandi: {len(df)} qator")
            
            # raw_content ustunining mavjudligini tekshirish
            if 'raw_content' not in df.columns:
                raise ValueError("'raw_content' ustuni topilmadi!")
            
            # raw_content ustunini parse qilish
            parsed_data = []
            for idx, row in df.iterrows():
                try:
                    if pd.notna(row['raw_content']):
                        content = str(row['raw_content']).strip()
                        
                        # JSON ni parse qilishga urinish
                        try:
                            parsed_json = json.loads(content)
                        except json.JSONDecodeError as e:
                            # Agar JSON buzuq bo'lsa, tuzatishga harakat qilish
                            print(f"JSON xatosi {idx}-qatorda, tuzatishga harakat qilinmoqda...")
                            
                            # Birinchi ochiluvchi figurali qavs topish
                            start_idx = content.find('{')
                            if start_idx != -1:
                                # Balansli JSON yaratishga harakat qilish
                                brace_count = 0
                                end_idx = start_idx
                                
                                for i in range(start_idx, len(content)):
                                    if content[i] == '{':
                                        brace_count += 1
                                    elif content[i] == '}':
                                        brace_count -= 1
                                        if brace_count == 0:
                                            end_idx = i
                                            break
                                
                                # Tuzatilgan JSON ni parse qilish
                                clean_content = content[start_idx:end_idx+1]
                                parsed_json = json.loads(clean_content)
                                print(f"  Tuzatildi!")
                            else:
                                raise e
                        
                        parsed_data.append({
                            'original_index': idx,
                            'parsed_data': parsed_json
                        })
                    else:
                        print(f"Bo'sh raw_content: {idx}-qator")
                except json.JSONDecodeError as e:
                    print(f"JSON parse xatosi {idx}-qatorda: {e}")
                except Exception as e:
                    print(f"Umumiy xato {idx}-qatorda: {e}")
            
            if not parsed_data:
                raise ValueError("Hech qanday ma'lumot parse qilinmadi!")
            
            print(f"Muvaffaqiyatli parse qilindi: {len(parsed_data)} qator")
            return parsed_data
            
        except Exception as e:
            print(f"Fayl yuklashda xato: {e}")
            raise

    def get_or_create_comm_type_id(self, comm_type: str) -> int:
        """Communication type ID olish yoki yaratish"""
        if not comm_type or pd.isna(comm_type):
            comm_type = "unknown"
        
        if comm_type not in self.comm_type_lookup:
            comm_type_id = self.comm_type_counter
            self.comm_type_lookup[comm_type] = comm_type_id
            self.dim_comm_type.append({
                'comm_type': comm_type,
                'comm_type_id': comm_type_id
            })
            self.comm_type_counter += 1
        
        return self.comm_type_lookup[comm_type]

    def get_or_create_subject_id(self, subject: str) -> int:
        """Subject ID olish yoki yaratish"""
        if not subject or pd.isna(subject):
            subject = "No Subject"
        
        if subject not in self.subject_lookup:
            subject_id = self.subject_counter
            self.subject_lookup[subject] = subject_id
            self.dim_subject.append({
                'subject_id': subject_id,
                'subject': subject
            })
            self.subject_counter += 1
        
        return self.subject_lookup[subject]

    def get_or_create_calendar_id(self, calendar_id: str) -> int:
        """Calendar ID olish yoki yaratish"""
        if not calendar_id or pd.isna(calendar_id):
            return None
        
        if calendar_id not in self.calendar_lookup:
            new_calendar_id = self.calendar_counter
            self.calendar_lookup[calendar_id] = new_calendar_id
            self.dim_calendar.append({
                'raw_calendar_id': calendar_id,
                'calendar_id': new_calendar_id
            })
            self.calendar_counter += 1
        
        return self.calendar_lookup[calendar_id]

    def get_or_create_audio_id(self, audio_url: str) -> int:
        """Audio ID olish yoki yaratish"""
        if not audio_url or pd.isna(audio_url):
            return None
        
        if audio_url not in self.audio_lookup:
            audio_id = self.audio_counter
            self.audio_lookup[audio_url] = audio_id
            self.dim_audio.append({
                'raw_audio_url': audio_url,
                'audio_id': audio_id
            })
            self.audio_counter += 1
        
        return self.audio_lookup[audio_url]

    def get_or_create_video_id(self, video_url: str) -> int:
        """Video ID olish yoki yaratish"""
        if not video_url or pd.isna(video_url):
            return None
        
        if video_url not in self.video_lookup:
            video_id = self.video_counter
            self.video_lookup[video_url] = video_id
            self.dim_video.append({
                'raw_video_url': video_url,
                'video_id': video_id
            })
            self.video_counter += 1
        
        return self.video_lookup[video_url]

    def get_or_create_transcript_id(self, transcript_url: str) -> int:
        """Transcript ID olish yoki yaratish"""
        if not transcript_url or pd.isna(transcript_url):
            return None
        
        if transcript_url not in self.transcript_lookup:
            transcript_id = self.transcript_counter
            self.transcript_lookup[transcript_url] = transcript_id
            self.dim_transcript.append({
                'raw_transcript_url': transcript_url,
                'transcript_id': transcript_id
            })
            self.transcript_counter += 1
        
        return self.transcript_lookup[transcript_url]

    def get_or_create_user_id(self, email: str) -> str:
        """User ID olish yoki yaratish"""
        if not email or pd.isna(email):
            return None
        
        email = str(email).strip().lower()  # Normalize email
        
        if email not in self.user_lookup:
            user_id = str(uuid.uuid4())
            self.user_lookup[email] = user_id
            self.dim_user.append({
                'email': email,
                'user_id': user_id
            })
        
        return self.user_lookup[email]

    def extract_emails_from_list(self, data: Any) -> List[str]:
        """List dan email larni ajratib olish"""
        emails = []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and 'email' in item:
                    if item['email'] and not pd.isna(item['email']):
                        emails.append(str(item['email']).strip().lower())
                elif isinstance(item, str) and '@' in item:
                    emails.append(str(item).strip().lower())
        elif isinstance(data, str) and '@' in data:
            emails.append(str(data).strip().lower())
        return list(set(emails))  # Dublikatlarni olib tashlash


    def transform_data(self, parsed_data: List[Dict], input_file: str) -> None:
        """2-4. Ma'lumotlarni transform qilish va dimension jadvallarni yaratish"""
        print("2. Ma'lumotlarni transform qilish...")

        # Fayl extension tekshirish
        if input_file.endswith(".csv"):
            df_original = pd.read_csv(input_file)
        else:
            df_original = pd.read_excel(input_file, engine="openpyxl")

        for item in parsed_data:
            data = item['parsed_data']

            # Asosiy ma'lumotlarni ajratish
            title = data.get('title', '')
            duration = data.get('duration', None)
            calendar_id = data.get('calendar_id', None)
            audio_url = data.get('audio_url', None)
            video_url = data.get('video_url', None)
            transcript_url = data.get('transcript_url', None)
            date_string = data.get('dateString', None)

            # Communication type ni aniqlash
            comm_type = "unknown"
            if audio_url or video_url:
                comm_type = "meeting"
            elif '@' in str(title).lower() or 'email' in str(title).lower():
                comm_type = "email"
            elif 'chat' in str(title).lower():
                comm_type = "chat"
            elif 'call' in str(title).lower():
                comm_type = "call"

            # Dimension ID lar
            comm_type_id = self.get_or_create_comm_type_id(comm_type)
            subject_id = self.get_or_create_subject_id(title)
            calendar_id_fk = self.get_or_create_calendar_id(calendar_id)
            audio_id = self.get_or_create_audio_id(audio_url)
            video_id = self.get_or_create_video_id(video_url)
            transcript_id = self.get_or_create_transcript_id(transcript_url)

            # Email larni ajratish
            speakers = self.extract_emails_from_list(data.get('speakers', []))
            participants = self.extract_emails_from_list(data.get('participants', []))

            # Meeting attendees
            attendees = []
            meeting_attendees = data.get('meeting_attendees', [])
            if isinstance(meeting_attendees, list):
                for attendee in meeting_attendees:
                    if isinstance(attendee, dict) and 'email' in attendee:
                        email = attendee.get('email')
                        if email and not pd.isna(email):
                            attendees.append(str(email).strip().lower())

            # Host va organizer
            host_email = data.get('host_email', None)
            if host_email and not pd.isna(host_email):
                host_email = str(host_email).strip().lower()
            else:
                host_email = None

            organizer_email = data.get('organizer_email', None)
            if organizer_email and not pd.isna(organizer_email):
                organizer_email = str(organizer_email).strip().lower()
            else:
                organizer_email = None

            # Barcha unique email larni to‘plash
            all_emails = set(speakers + participants + attendees)
            if host_email:
                all_emails.add(host_email)
            if organizer_email:
                all_emails.add(organizer_email)

            for email in all_emails:
                self.get_or_create_user_id(email)

            # Fact jadvalga yozish
            comm_id = str(uuid.uuid4())
            raw_row_idx = item['original_index']
            original_row = df_original.iloc[raw_row_idx]

            self.fact_communication.append({
                'comm_id': comm_id,
                'raw_id': data.get('id', ''),
                'source_id': original_row.get('source_id', ''),
                'comm_type_id': comm_type_id,
                'subject_id': subject_id,
                'calendar_id': calendar_id_fk,
                'audio_id': audio_id,
                'video_id': video_id,
                'transcript_id': transcript_id,
                'datetime_id': date_string,
                'ingested_at': original_row.get('ingested_at', ''),
                'processed_at': original_row.get('processed_at', ''),
                'is_processed': original_row.get('is_processed', True),
                'raw_title': title,
                'raw_duration': duration
            })

            # Bridge table yozish
            for email in all_emails:
                user_id = self.user_lookup[email]
                self.bridge_comm_user.append({
                    'comm_id': comm_id,
                    'user_id': user_id,
                    'isAttendee': email in attendees,
                    'isOrganiser': email == organizer_email,
                    'isParticipant': email in participants,
                    'isSpeaker': email in speakers
                })

        print(f"Transform tugadi:")
        print(f"  - Fact communications: {len(self.fact_communication)}")
        print(f"  - Bridge records: {len(self.bridge_comm_user)}")
        print(f"  - Unique users: {len(self.dim_user)}")


    def load_to_excel(self, output_file: str) -> None:
        """6. Barcha jadvallarni Excel ga export qilish"""
        print("6. Excel ga export qilish...")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Dimension tables
            if self.dim_comm_type:
                pd.DataFrame(self.dim_comm_type).to_excel(writer, sheet_name='dim_comm_type', index=False)
                print(f"  - dim_comm_type: {len(self.dim_comm_type)} qator")
            
            if self.dim_subject:
                pd.DataFrame(self.dim_subject).to_excel(writer, sheet_name='dim_subject', index=False)
                print(f"  - dim_subject: {len(self.dim_subject)} qator")
            
            if self.dim_calendar:
                pd.DataFrame(self.dim_calendar).to_excel(writer, sheet_name='dim_calendar', index=False)
                print(f"  - dim_calendar: {len(self.dim_calendar)} qator")
            
            if self.dim_audio:
                pd.DataFrame(self.dim_audio).to_excel(writer, sheet_name='dim_audio', index=False)
                print(f"  - dim_audio: {len(self.dim_audio)} qator")
            
            if self.dim_video:
                pd.DataFrame(self.dim_video).to_excel(writer, sheet_name='dim_video', index=False)
                print(f"  - dim_video: {len(self.dim_video)} qator")
            
            if self.dim_transcript:
                pd.DataFrame(self.dim_transcript).to_excel(writer, sheet_name='dim_transcript', index=False)
                print(f"  - dim_transcript: {len(self.dim_transcript)} qator")
            
            if self.dim_user:
                pd.DataFrame(self.dim_user).to_excel(writer, sheet_name='dim_user', index=False)
                print(f"  - dim_user: {len(self.dim_user)} qator")
            
            # Fact table
            if self.fact_communication:
                pd.DataFrame(self.fact_communication).to_excel(writer, sheet_name='fact_communication', index=False)
                print(f"  - fact_communication: {len(self.fact_communication)} qator")
            
            # Bridge table
            if self.bridge_comm_user:
                pd.DataFrame(self.bridge_comm_user).to_excel(writer, sheet_name='bridge_comm_user', index=False)
                print(f"  - bridge_comm_user: {len(self.bridge_comm_user)} qator")
        
        print(f"Export tugadi: {output_file}")

    def run_etl(self, input_file: str, output_file: str) -> None:
        """To'liq ETL jarayonini bajarish"""
        print("=== Communication Data ETL Jarayoni ===")
        
        try:
            # 1. Extract
            parsed_data = self.extract_data(input_file)
            
            # 2-4. Transform
            self.transform_data(parsed_data, input_file)
            
            # 5. Load
            self.load_to_excel(output_file)
            
            print("=== ETL Jarayoni Tugadi ===")
            
            # Summary
            print(f"\nNatija:")
            print(f"  Dimension jadvallar: 7 ta")
            print(f"  Fact jadval: 1 ta ({len(self.fact_communication)} qator)")
            print(f"  Bridge jadval: 1 ta ({len(self.bridge_comm_user)} qator)")
            print(f"  Jami foydalanuvchilar: {len(self.dim_user)}")
            
        except Exception as e:
            print(f"ETL jarayonida xato: {e}")
            raise


# ETL ni ishga tushirish
if __name__ == "__main__":
    etl = CommunicationETL()
    
    
    # Hozirgi papkada fayllarni qidirish
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    # input_file = os.path.join(current_dir, "E:\MAAB\python\project\raw_data.csv")
    # output_file = os.path.join(current_dir, "E:\MAAB\python\project\final.xlsx")

    # Hozirgi fayl katalogini aniqlash

    print(f"----------- current dir: {current_dir}")

    try:
        # Fayl mavjudligini tekshirish
        if not os.path.exists(input_file):
            print(f"Fayl topilmadi: {input_file}")  
            print("Mavjud fayllar:")
            directory = os.path.dirname(input_file)
            if os.path.exists(directory):
                for file in os.listdir(directory):
                    if file.endswith(('.xlsx', '.csv')):
                        print(f"  - {file}")
            else:
                print(f"Papka mavjud emas: {directory}")
            exit(1)
        
        etl.run_etl(input_file, output_file)
        
    except FileNotFoundError:
        print(f"Fayl topilmadi: {input_file}")
        print("fayl yo'lini tekshiring.")
    except Exception as e:
        print(f"ETL jarayonida xato: {e}")
        import traceback
        traceback.print_exc()