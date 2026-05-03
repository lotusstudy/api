"""
Lotus Academy Management — Complete Backend with SQLite Database
Features:
- SQLite database for persistent storage
- Prevents duplicate dates (one date = one attendance record per class/teacher)
- Full CRUD operations
- Excel file parsing and storage
- Google Drive integration (optional)
"""

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import pandas as pd
import io
import re
import os
import sqlite3
import json
from datetime import datetime
from contextlib import contextmanager
import hashlib

# Google Drive (optional)
try:
    from googleapiclient.discovery import build
    from google.oauth2 import service_account
    GDRIVE_ENABLED = True
except ImportError:
    GDRIVE_ENABLED = False

app = Flask(__name__)
CORS(app, origins=["*"])

# ─── DATABASE SETUP ──────────────────────────────────────────────
DATABASE_PATH = os.environ.get("DATABASE_PATH", "lotus_academy.db")

def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize database tables"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Main records table - UNIQUE constraint prevents duplicates
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS attendance_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            record_type TEXT NOT NULL CHECK(record_type IN ('student', 'teacher')),
            class_name TEXT,
            file_hash TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, record_type, class_name)
        )
    ''')
    
    # Students attendance table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS student_attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id INTEGER NOT NULL,
            student_name TEXT NOT NULL,
            student_class TEXT NOT NULL,
            board TEXT,
            stream TEXT,
            time TEXT,
            status TEXT NOT NULL CHECK(status IN ('PRESENT', 'ABSENT')),
            FOREIGN KEY (record_id) REFERENCES attendance_records(id) ON DELETE CASCADE
        )
    ''')
    
    # Teachers attendance table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS teacher_attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id INTEGER NOT NULL,
            teacher_name TEXT NOT NULL,
            subject TEXT,
            time TEXT,
            status TEXT NOT NULL CHECK(status IN ('PRESENT', 'ABSENT')),
            FOREIGN KEY (record_id) REFERENCES attendance_records(id) ON DELETE CASCADE
        )
    ''')
    
    # Create indexes for better query performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON attendance_records(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_record_type ON attendance_records(record_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_student_name ON student_attendance(student_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_teacher_name ON teacher_attendance(teacher_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date_type ON attendance_records(date, record_type)')
    
    conn.commit()
    conn.close()
    print("✅ Database initialized successfully")

@contextmanager
def db_transaction():
    """Context manager for database transactions"""
    conn = get_db()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# Initialize database
init_db()

# ─── CONFIG ──────────────────────────────────────────────
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# ─── GOOGLE DRIVE HELPERS ────────────────────────────────
def get_drive_service():
    if not GDRIVE_ENABLED or not GOOGLE_SERVICE_ACCOUNT_JSON:
        return None
    try:
        import json
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        print(f"[WARN] Failed to create Drive service: {e}")
        return None

def list_excel_files(service):
    """List all Excel files in the configured Drive folder."""
    try:
        query = f"'{GDRIVE_FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false"
        results = service.files().list(q=query, fields="files(id,name,modifiedTime)").execute()
        return results.get("files", [])
    except Exception as e:
        print(f"[ERROR] Failed to list Drive files: {e}")
        return []

def download_file(service, file_id):
    """Download file bytes from Drive."""
    from googleapiclient.http import MediaIoBaseDownload
    try:
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buf.seek(0)
        return buf
    except Exception as e:
        print(f"[ERROR] Failed to download file {file_id}: {e}")
        raise

def calculate_file_hash(content):
    """Calculate SHA256 hash of file content for duplicate detection"""
    return hashlib.sha256(content).hexdigest()

# ─── EXCEL PARSERS ───────────────────────────────────────
def parse_date_from_filename(fname: str) -> str:
    """Extract YYYY-MM-DD from filename."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
    return m.group(1) if m else datetime.today().strftime("%Y-%m-%d")

def parse_class_from_filename(fname: str) -> str:
    """Extract class label e.g. 'Class 12' from filename."""
    m = re.search(r"Class_(.+?)_\d{4}-\d{2}-\d{2}", fname, re.IGNORECASE)
    return m.group(1).strip() if m else "Unknown"

def parse_student_excel(buf, filename: str) -> dict:
    """
    Parse a student attendance Excel file.
    """
    date = parse_date_from_filename(filename)
    class_label = parse_class_from_filename(filename)

    df_raw = pd.read_excel(buf, header=None)

    # Find the data start row
    data_start = 4
    for i in range(2, min(15, len(df_raw))):
        val = str(df_raw.iloc[i, 6] if df_raw.shape[1] > 6 else "").upper()
        if val in ("PRESENT", "ABSENT"):
            data_start = i
            break

    df = df_raw.iloc[data_start:].reset_index(drop=True)
    df.columns = range(df.shape[1])

    students = []
    for _, row in df.iterrows():
        name = str(row.get(0, "")).strip()
        if not name or len(name) < 2:
            continue
        status = str(row.get(6, "ABSENT")).strip().upper()
        if status not in ("PRESENT", "ABSENT"):
            status = "ABSENT"
        time_val = str(row.get(5, "")).strip()
        if time_val in ("00:00:00", "nan", "NaT", ""):
            time_val = ""
        students.append({
            "name": name,
            "class": str(row.get(1, class_label)).strip() or class_label,
            "board": str(row.get(2, "")).strip(),
            "stream": str(row.get(3, "")).strip(),
            "time": time_val,
            "status": status,
        })

    return {
        "date": date,
        "class": class_label,
        "type": "student",
        "students": students,
        "teachers": [],
    }

def parse_teacher_excel(buf, filename: str) -> dict:
    """
    Parse a teacher attendance Excel file.
    """
    date = parse_date_from_filename(filename)
    df_raw = pd.read_excel(buf, header=None)

    data_start = 4
    for i in range(2, min(15, len(df_raw))):
        val = str(df_raw.iloc[i, -1] if df_raw.shape[1] > 0 else "").upper()
        if val in ("PRESENT", "ABSENT"):
            data_start = i
            break

    df = df_raw.iloc[data_start:].reset_index(drop=True)
    df.columns = range(df.shape[1])

    teachers = []
    for _, row in df.iterrows():
        name = str(row.get(0, "")).strip()
        if not name or len(name) < 2:
            continue
        ncols = df.shape[1]
        status = str(row.get(ncols - 1, "ABSENT")).strip().upper()
        if status not in ("PRESENT", "ABSENT"):
            status = "ABSENT"
        time_val = str(row.get(ncols - 2, "")).strip()
        if time_val in ("00:00:00", "nan", "NaT", ""):
            time_val = ""
        teachers.append({
            "name": name,
            "subject": str(row.get(1, "")).strip(),
            "time": time_val,
            "status": status,
        })

    return {
        "date": date,
        "class": "teachers",
        "type": "teacher",
        "students": [],
        "teachers": teachers,
    }

# ─── DATABASE OPERATIONS ──────────────────────────────────
def record_exists(date, record_type, class_name=None):
    """Check if a record already exists for given date and type"""
    conn = get_db()
    cursor = conn.cursor()
    
    if record_type == "student":
        cursor.execute(
            "SELECT id FROM attendance_records WHERE date = ? AND record_type = ? AND class_name = ?",
            (date, record_type, class_name)
        )
    else:
        cursor.execute(
            "SELECT id FROM attendance_records WHERE date = ? AND record_type = ?",
            (date, record_type)
        )
    
    result = cursor.fetchone()
    conn.close()
    return result is not None

def get_record_id(date, record_type, class_name=None):
    """Get record ID if exists"""
    conn = get_db()
    cursor = conn.cursor()
    
    if record_type == "student":
        cursor.execute(
            "SELECT id FROM attendance_records WHERE date = ? AND record_type = ? AND class_name = ?",
            (date, record_type, class_name)
        )
    else:
        cursor.execute(
            "SELECT id FROM attendance_records WHERE date = ? AND record_type = ?",
            (date, record_type)
        )
    
    result = cursor.fetchone()
    conn.close()
    return result['id'] if result else None

def save_student_record(record, file_hash=None):
    """Save student attendance record to database"""
    date = record['date']
    class_name = record['class']
    students = record['students']
    
    with db_transaction() as conn:
        cursor = conn.cursor()
        
        # Insert main record
        cursor.execute('''
            INSERT INTO attendance_records (date, record_type, class_name, file_hash)
            VALUES (?, ?, ?, ?)
        ''', (date, 'student', class_name, file_hash))
        
        record_id = cursor.lastrowid
        
        # Insert all students
        for student in students:
            cursor.execute('''
                INSERT INTO student_attendance (record_id, student_name, student_class, board, stream, time, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                record_id,
                student['name'],
                student['class'],
                student.get('board', ''),
                student.get('stream', ''),
                student.get('time', ''),
                student['status']
            ))
        
        return record_id

def save_teacher_record(record, file_hash=None):
    """Save teacher attendance record to database"""
    date = record['date']
    teachers = record['teachers']
    
    with db_transaction() as conn:
        cursor = conn.cursor()
        
        # Insert main record
        cursor.execute('''
            INSERT INTO attendance_records (date, record_type, class_name, file_hash)
            VALUES (?, ?, ?, ?)
        ''', (date, 'teacher', None, file_hash))
        
        record_id = cursor.lastrowid
        
        # Insert all teachers
        for teacher in teachers:
            cursor.execute('''
                INSERT INTO teacher_attendance (record_id, teacher_name, subject, time, status)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                record_id,
                teacher['name'],
                teacher.get('subject', ''),
                teacher.get('time', ''),
                teacher['status']
            ))
        
        return record_id

def get_all_records():
    """Fetch all attendance records from database"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, date, record_type, class_name, created_at, updated_at
        FROM attendance_records
        ORDER BY date DESC, record_type
    ''')
    
    records = cursor.fetchall()
    result = []
    
    for record in records:
        record_dict = {
            'id': record['id'],
            'date': record['date'],
            'class': record['class_name'] if record['record_type'] == 'student' else 'teachers',
            'type': record['record_type'],
            'students': [],
            'teachers': []
        }
        
        if record['record_type'] == 'student':
            cursor.execute('''
                SELECT student_name, student_class, board, stream, time, status
                FROM student_attendance
                WHERE record_id = ?
                ORDER BY student_name
            ''', (record['id'],))
            
            students = cursor.fetchall()
            record_dict['students'] = [
                {
                    'name': s['student_name'],
                    'class': s['student_class'],
                    'board': s['board'],
                    'stream': s['stream'],
                    'time': s['time'],
                    'status': s['status']
                }
                for s in students
            ]
        else:
            cursor.execute('''
                SELECT teacher_name, subject, time, status
                FROM teacher_attendance
                WHERE record_id = ?
                ORDER BY teacher_name
            ''', (record['id'],))
            
            teachers = cursor.fetchall()
            record_dict['teachers'] = [
                {
                    'name': t['teacher_name'],
                    'subject': t['subject'],
                    'time': t['time'],
                    'status': t['status']
                }
                for t in teachers
            ]
        
        result.append(record_dict)
    
    conn.close()
    return result

def delete_record_by_id(record_id):
    """Delete a record by its ID"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM attendance_records WHERE id = ?", (record_id,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted > 0

def delete_record(date, record_type, class_name=None):
    """Delete a specific record by date and type"""
    record_id = get_record_id(date, record_type, class_name)
    if record_id:
        return delete_record_by_id(record_id)
    return False

def delete_all_records():
    """Delete all records from database"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM attendance_records")
    cursor.execute("DELETE FROM sqlite_sequence WHERE name='attendance_records'")
    conn.commit()
    conn.close()

def get_stats():
    """Get database statistics"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM attendance_records")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM student_attendance")
    total_students = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM teacher_attendance")
    total_teachers = cursor.fetchone()[0]
    
    cursor.execute("SELECT DISTINCT date FROM attendance_records ORDER BY date")
    dates = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT record_type, COUNT(*) FROM attendance_records GROUP BY record_type")
    type_counts = {row[0]: row[1] for row in cursor.fetchall()}
    
    conn.close()
    
    return {
        'total_records': total_records,
        'total_student_entries': total_students,
        'total_teacher_entries': total_teachers,
        'unique_dates': len(dates),
        'dates': dates,
        'student_records': type_counts.get('student', 0),
        'teacher_records': type_counts.get('teacher', 0)
    }

def get_records_by_date(date):
    """Get all records for a specific date"""
    all_records = get_all_records()
    return [r for r in all_records if r['date'] == date]

def get_student_history(student_name, student_class=None):
    """Get attendance history for a specific student"""
    conn = get_db()
    cursor = conn.cursor()
    
    query = '''
        SELECT a.date, s.status, s.time, a.class_name
        FROM student_attendance s
        JOIN attendance_records a ON s.record_id = a.id
        WHERE s.student_name = ?
    '''
    params = [student_name]
    
    if student_class:
        query += " AND a.class_name = ?"
        params.append(student_class)
    
    query += " ORDER BY a.date DESC"
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    conn.close()
    
    return [{'date': r[0], 'status': r[1], 'time': r[2], 'class': r[3]} for r in results]

def get_teacher_history(teacher_name):
    """Get attendance history for a specific teacher"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT a.date, t.status, t.time
        FROM teacher_attendance t
        JOIN attendance_records a ON t.record_id = a.id
        WHERE t.teacher_name = ?
        ORDER BY a.date DESC
    ''', (teacher_name,))
    
    results = cursor.fetchall()
    conn.close()
    
    return [{'date': r[0], 'status': r[1], 'time': r[2]} for r in results]

# ─── ROUTES ──────────────────────────────────────────────

@app.route("/", methods=["GET"])
def home():
    """API home page"""
    return jsonify({
        "name": "Lotus Academy Management API",
        "version": "2.0",
        "status": "running",
        "endpoints": {
            "GET /health": "Health check",
            "GET /stats": "Database statistics",
            "GET /get-all-records": "Get all attendance records",
            "GET /get-record": "Get specific record (date, type, class)",
            "GET /student-history": "Get student attendance history",
            "GET /teacher-history": "Get teacher attendance history",
            "POST /parse-upload": "Upload and parse Excel file",
            "POST /upload-batch": "Upload multiple Excel files",
            "DELETE /delete-record": "Delete a specific record",
            "DELETE /delete-all": "Delete all records",
            "GET /sync-latest": "Sync from Google Drive"
        }
    })

@app.route("/health", methods=["GET"])
def health():
    stats = get_stats()
    return jsonify({
        "status": "ok",
        "database": "sqlite",
        "gdrive_enabled": GDRIVE_ENABLED and bool(GDRIVE_FOLDER_ID),
        "stats": stats,
        "timestamp": datetime.now().isoformat()
    })

@app.route("/stats", methods=["GET"])
def stats_route():
    """Get database statistics"""
    stats = get_stats()
    return jsonify(stats)

@app.route("/get-all-records", methods=["GET"])
def get_all_records_route():
    """Get all stored records from database"""
    records = get_all_records()
    stats = get_stats()
    return jsonify({
        "success": True,
        "count": len(records),
        "records": records,
        "stats": stats
    })

@app.route("/get-record", methods=["GET"])
def get_record():
    """Get a specific record by date and type"""
    date = request.args.get('date')
    record_type = request.args.get('type')
    class_name = request.args.get('class')
    
    if not date or not record_type:
        return jsonify({"error": "Missing date or type parameter"}), 400
    
    records = get_all_records()
    for record in records:
        if record['date'] == date and record['type'] == record_type:
            if record_type == 'student' and class_name and record['class'] != class_name:
                continue
            return jsonify({"success": True, "record": record})
    
    return jsonify({"success": False, "message": "Record not found"}), 404

@app.route("/student-history", methods=["GET"])
def student_history():
    """Get attendance history for a student"""
    student_name = request.args.get('name')
    student_class = request.args.get('class')
    
    if not student_name:
        return jsonify({"error": "Missing student name"}), 400
    
    history = get_student_history(student_name, student_class)
    
    # Calculate statistics
    total = len(history)
    present = sum(1 for h in history if h['status'] == 'PRESENT')
    absent = total - present
    percentage = (present / total * 100) if total > 0 else 0
    
    return jsonify({
        "success": True,
        "student_name": student_name,
        "student_class": student_class,
        "history": history,
        "stats": {
            "total_days": total,
            "present": present,
            "absent": absent,
            "percentage": round(percentage, 2)
        }
    })

@app.route("/teacher-history", methods=["GET"])
def teacher_history():
    """Get attendance history for a teacher"""
    teacher_name = request.args.get('name')
    
    if not teacher_name:
        return jsonify({"error": "Missing teacher name"}), 400
    
    history = get_teacher_history(teacher_name)
    
    # Calculate statistics
    total = len(history)
    present = sum(1 for h in history if h['status'] == 'PRESENT')
    absent = total - present
    percentage = (present / total * 100) if total > 0 else 0
    
    return jsonify({
        "success": True,
        "teacher_name": teacher_name,
        "history": history,
        "stats": {
            "total_days": total,
            "present": present,
            "absent": absent,
            "percentage": round(percentage, 2)
        }
    })

@app.route("/parse-upload", methods=["POST"])
def parse_upload():
    """
    Accepts a multipart file upload, parses it, saves to database.
    Prevents duplicates - returns error if record already exists.
    """
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    fname = f.filename
    
    # Read file content for hash calculation
    file_content = f.read()
    file_hash = calculate_file_hash(file_content)
    buf = io.BytesIO(file_content)

    try:
        # Check file type from filename
        is_teacher = re.match(r"^Teachers?_\d{4}-\d{2}-\d{2}\.xlsx$", fname, re.IGNORECASE)
        is_student = re.match(r"^Class_.+_\d{4}-\d{2}-\d{2}\.xlsx$", fname, re.IGNORECASE)
        
        if not is_teacher and not is_student:
            return jsonify({
                "error": "Invalid filename format",
                "message": "Expected format: Class_Class 12_2026-04-30.xlsx or Teachers_2026-04-30.xlsx",
                "received": fname
            }), 400

        if is_teacher:
            record = parse_teacher_excel(buf, fname)
            
            # Check for duplicate by date
            if record_exists(record['date'], 'teacher'):
                existing_id = get_record_id(record['date'], 'teacher')
                return jsonify({
                    "success": False,
                    "error": "DUPLICATE_DATE",
                    "message": f"❌ Teacher attendance for {record['date']} already exists!",
                    "message_detail": f"Attendance for {record['date']} has already been recorded. Cannot add duplicate entry for the same date.",
                    "existing": True,
                    "date": record['date'],
                    "record_id": existing_id,
                    "action_required": "Delete the existing record first or choose a different date file."
                }), 409
                
            record_id = save_teacher_record(record, file_hash)
            return jsonify({
                "success": True,
                "message": f"✅ Teacher attendance for {record['date']} saved successfully!",
                "record": record,
                "record_id": record_id,
                "is_new": True
            }), 200
            
        else:
            record = parse_student_excel(buf, fname)
            
            # Check for duplicate by date and class
            if record_exists(record['date'], 'student', record['class']):
                existing_id = get_record_id(record['date'], 'student', record['class'])
                return jsonify({
                    "success": False,
                    "error": "DUPLICATE_DATE",
                    "message": f"❌ Student attendance for {record['class']} on {record['date']} already exists!",
                    "message_detail": f"Attendance for {record['class']} on {record['date']} has already been recorded. Cannot add duplicate entry for the same date and class.",
                    "existing": True,
                    "date": record['date'],
                    "class": record['class'],
                    "record_id": existing_id,
                    "action_required": "Delete the existing record first or choose a different date file."
                }), 409
                
            record_id = save_student_record(record, file_hash)
            return jsonify({
                "success": True,
                "message": f"✅ Student attendance for {record['class']} on {record['date']} saved successfully!",
                "record": record,
                "record_id": record_id,
                "is_new": True
            }), 200
            
    except Exception as e:
        return jsonify({
            "success": False,
            "error": "PROCESSING_ERROR",
            "message": f"Failed to process file: {str(e)}"
        }), 500

@app.route("/upload-batch", methods=["POST"])
def upload_batch():
    """Upload multiple Excel files at once"""
    if "files" not in request.files:
        return jsonify({"error": "No files provided"}), 400
    
    files = request.files.getlist("files")
    results = {
        "successful": [],
        "failed": [],
        "duplicates": [],
        "total": len(files)
    }
    
    for file in files:
        fname = file.filename
        file_content = file.read()
        file_hash = calculate_file_hash(file_content)
        buf = io.BytesIO(file_content)
        
        try:
            is_teacher = re.match(r"^Teachers?_\d{4}-\d{2}-\d{2}\.xlsx$", fname, re.IGNORECASE)
            is_student = re.match(r"^Class_.+_\d{4}-\d{2}-\d{2}\.xlsx$", fname, re.IGNORECASE)
            
            if not is_teacher and not is_student:
                results["failed"].append({
                    "file": fname,
                    "reason": "Invalid filename format"
                })
                continue
            
            if is_teacher:
                record = parse_teacher_excel(buf, fname)
                if record_exists(record['date'], 'teacher'):
                    results["duplicates"].append({
                        "file": fname,
                        "date": record['date'],
                        "type": "teacher"
                    })
                else:
                    record_id = save_teacher_record(record, file_hash)
                    results["successful"].append({
                        "file": fname,
                        "date": record['date'],
                        "type": "teacher",
                        "record_id": record_id
                    })
            else:
                record = parse_student_excel(buf, fname)
                if record_exists(record['date'], 'student', record['class']):
                    results["duplicates"].append({
                        "file": fname,
                        "date": record['date'],
                        "class": record['class'],
                        "type": "student"
                    })
                else:
                    record_id = save_student_record(record, file_hash)
                    results["successful"].append({
                        "file": fname,
                        "date": record['date'],
                        "class": record['class'],
                        "type": "student",
                        "record_id": record_id
                    })
        except Exception as e:
            results["failed"].append({
                "file": fname,
                "reason": str(e)
            })
    
    return jsonify({
        "success": True,
        "results": results,
        "summary": {
            "total": results["total"],
            "successful": len(results["successful"]),
            "duplicates": len(results["duplicates"]),
            "failed": len(results["failed"])
        }
    })

@app.route("/delete-record", methods=["DELETE"])
def delete_record_route():
    """Delete a specific record"""
    data = request.get_json()
    if not data:
        return jsonify({"error": "Missing request body"}), 400
    
    date = data.get('date')
    record_type = data.get('type')
    class_name = data.get('class')
    
    if not date or not record_type:
        return jsonify({"error": "Missing date or type parameter"}), 400
    
    # Get record info before deletion for response
    record_id = get_record_id(date, record_type, class_name)
    
    deleted = delete_record(date, record_type, class_name)
    
    if deleted:
        return jsonify({
            "success": True,
            "message": f"✅ Successfully deleted {record_type} attendance record for {date}" + (f" ({class_name})" if class_name else ""),
            "deleted_record": {
                "date": date,
                "type": record_type,
                "class": class_name,
                "record_id": record_id
            }
        })
    else:
        return jsonify({
            "success": False,
            "message": f"Record not found for {date} - {record_type}" + (f" - {class_name}" if class_name else "")
        }), 404

@app.route("/delete-all", methods=["DELETE"])
def delete_all_route():
    """Delete all records (use with caution!)"""
    confirm = request.args.get('confirm', 'false')
    if confirm != 'true':
        return jsonify({
            "error": "Confirmation required",
            "message": "Set confirm=true to delete all records. This action cannot be undone!"
        }), 400
    
    stats_before = get_stats()
    delete_all_records()
    stats_after = get_stats()
    
    return jsonify({
        "success": True,
        "message": "🗑️ All records have been permanently deleted.",
        "records_deleted": stats_before['total_records'],
        "student_entries_deleted": stats_before['total_student_entries'],
        "teacher_entries_deleted": stats_before['total_teacher_entries'],
        "stats_before": stats_before,
        "stats_after": stats_after
    })

@app.route("/check-exists", methods=["GET"])
def check_exists():
    """Check if a record already exists"""
    date = request.args.get('date')
    record_type = request.args.get('type')
    class_name = request.args.get('class')
    
    if not date or not record_type:
        return jsonify({"error": "Missing date or type parameter"}), 400
    
    exists = record_exists(date, record_type, class_name)
    
    response = {
        "exists": exists,
        "date": date,
        "type": record_type,
        "class": class_name
    }
    
    if exists:
        record_id = get_record_id(date, record_type, class_name)
        response["record_id"] = record_id
        response["message"] = f"⚠️ A record already exists for {date}" + (f" for {class_name}" if class_name else "")
    else:
        response["message"] = f"✅ No record found for {date}" + (f" for {class_name}" if class_name else "")
    
    return jsonify(response)

@app.route("/sync-latest", methods=["GET"])
def sync_latest():
    """
    Fetch latest Excel files from Google Drive, parse them, save to database.
    Prevents duplicates - existing records are skipped.
    """
    service = get_drive_service()
    if not service:
        return jsonify({
            "success": False,
            "error": "Google Drive not configured",
            "message": "Set GDRIVE_FOLDER_ID and GOOGLE_SERVICE_ACCOUNT_JSON environment variables"
        }), 200

    files = list_excel_files(service)
    results = {
        "processed": [],
        "skipped": [],
        "errors": []
    }

    for f in files:
        fname = f["name"]
        try:
            buf = download_file(service, f["id"])
            file_content = buf.getvalue()
            file_hash = calculate_file_hash(file_content)
            buf = io.BytesIO(file_content)
            
            is_teacher = re.match(r"^Teachers?_\d{4}-\d{2}-\d{2}\.xlsx$", fname, re.IGNORECASE)
            is_student = re.match(r"^Class_.+_\d{4}-\d{2}-\d{2}\.xlsx$", fname, re.IGNORECASE)

            if is_teacher:
                record = parse_teacher_excel(buf, fname)
                if record_exists(record['date'], 'teacher'):
                    results["skipped"].append({
                        'file': fname,
                        'date': record['date'],
                        'reason': 'Duplicate - already exists in database'
                    })
                else:
                    record_id = save_teacher_record(record, file_hash)
                    results["processed"].append({
                        'file': fname,
                        'date': record['date'],
                        'type': 'teacher',
                        'record_id': record_id
                    })
                    
            elif is_student:
                record = parse_student_excel(buf, fname)
                if record_exists(record['date'], 'student', record['class']):
                    results["skipped"].append({
                        'file': fname,
                        'date': record['date'],
                        'class': record['class'],
                        'reason': 'Duplicate - already exists in database'
                    })
                else:
                    record_id = save_student_record(record, file_hash)
                    results["processed"].append({
                        'file': fname,
                        'date': record['date'],
                        'class': record['class'],
                        'type': 'student',
                        'record_id': record_id
                    })
            else:
                results["skipped"].append({
                    'file': fname,
                    'reason': 'Filename format not recognised'
                })
                
        except Exception as e:
            results["errors"].append({
                'file': fname,
                'error': str(e)
            })

    return jsonify({
        'success': True,
        'results': results,
        'summary': {
            'total_files': len(files),
            'processed_count': len(results["processed"]),
            'skipped_count': len(results["skipped"]),
            'error_count': len(results["errors"])
        }
    })

@app.route("/export-database", methods=["GET"])
def export_database():
    """Export the entire database as SQLite file"""
    if not os.path.exists(DATABASE_PATH):
        return jsonify({"error": "Database file not found"}), 404
    
    return send_file(
        DATABASE_PATH,
        as_attachment=True,
        download_name=f"lotus_academy_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db",
        mimetype="application/x-sqlite3"
    )

# ─── ENTRY POINT ─────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    host = os.environ.get("HOST", "0.0.0.0")
    
    print("\n" + "="*60)
    print("🚀 LOTUS ACADEMY MANAGEMENT SYSTEM")
    print("="*60)
    print(f"📍 Server: http://{host}:{port}")
    print(f"🗄️  Database: {DATABASE_PATH}")
    print(f"📡 Health Check: http://{host}:{port}/health")
    print(f"📊 Statistics: http://{host}:{port}/stats")
    print(f"📁 All Records: http://{host}:{port}/get-all-records")
    print("="*60)
    print("\n✨ API Ready! Waiting for requests...\n")
    
    app.run(host=host, port=port, debug=False, threaded=True)
