# =====================================================================
# ANNOTATIO — GLOBAL FOUNDATION + CONCERT CONTROL
# PURPOSE: GLOBAL SYSTEM FIRST, CONCERT CONTROL PAGE SECOND
# =====================================================================

import sqlite3
import secrets
import shutil
from email.message import EmailMessage
from pathlib import Path
from fastapi import FastAPI, Request, Form, File, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from datetime import datetime, timedelta
from urllib.parse import parse_qs, quote_plus, unquote_plus, urlparse

app = FastAPI()

# =====================================================================
# LIBRARIAN PAGE FOUNDATION
# PURPOSE: FEED EXISTING librarian.html WITHOUT CHANGING ITS LAYOUT
# =====================================================================

APP_DIR = Path(__file__).parent
TEMPLATES_DIR = APP_DIR / "templates"
STATIC_DIR = APP_DIR / "static"

TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR.mkdir(parents=True, exist_ok=True)
def annotatio_repo_bootstrap_files():
    static_suffixes = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".mp3", ".wav", ".webp"}

    for source in APP_DIR.iterdir():
        if not source.is_file():
            continue

        if source.name == "app.py":
            continue

        if source.suffix.lower() == ".html":
            target = TEMPLATES_DIR / source.name
            if not target.exists():
                shutil.copy2(source, target)
            continue

        if source.suffix.lower() in static_suffixes:
            target = STATIC_DIR / source.name
            if not target.exists():
                shutil.copy2(source, target)


annotatio_repo_bootstrap_files()

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

annotatio_original_template_response = templates.TemplateResponse

def annotatio_template_response_compat(
    name,
    context,
    status_code=200,
    headers=None,
    media_type=None,
    background=None,
):
    return annotatio_original_template_response(
        context["request"],
        name,
        context,
        status_code=status_code,
        headers=headers,
        media_type=media_type,
        background=background,
    )

templates.TemplateResponse = annotatio_template_response_compat

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# =====================================================================
# DATABASE
# =====================================================================

DB_PATH = APP_DIR / "annotatio.db"


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS organisations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        country TEXT,
        city TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        email TEXT UNIQUE,
        role TEXT,
        organisation_id INTEGER
    )
    """)

    cur.execute("PRAGMA table_info(users)")
    user_cols = {row["name"] for row in cur.fetchall()}
    if "organisation_id" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN organisation_id INTEGER")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS musicians (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        instrument TEXT,
        country TEXT,
        city TEXT
    )
    """)

    cur.execute("PRAGMA table_info(musicians)")
    musician_cols = {row["name"] for row in cur.fetchall()}

    if "user_id" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN user_id INTEGER")

    if "email" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN email TEXT")

    if "name" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN name TEXT")

    if "preferred_name" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN preferred_name TEXT")

    if "mobile" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN mobile TEXT")

    if "state_region_territory" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN state_region_territory TEXT")

    if "country_code" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN country_code TEXT")

    if "country_name" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN country_name TEXT")

    if "ensembles" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN ensembles TEXT")

    if "primary_instrument" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN primary_instrument TEXT")

    if "voice_type" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN voice_type TEXT")

    if "other_instruments" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN other_instruments TEXT")

    if "notes" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN notes TEXT")

    if "password_hash" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN password_hash TEXT")

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_musicians_user_id_unique
    ON musicians(user_id)
    WHERE user_id IS NOT NULL
    """)

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique
    ON users(email)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS invites (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        organisation_id INTEGER,
        musician_user_id INTEGER,
        invited_by_user_id INTEGER,
        organisation_country TEXT,
        musician_country TEXT,
        invite_email TEXT,
        target_ensemble_name TEXT,
        target_is_default_ensemble INTEGER NOT NULL DEFAULT 0,
        target_section TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT,
        responded_at TEXT,
        invite_sent_at TEXT,
        musician_comment TEXT,
        confirmed_at TEXT
    )
    """)

    cur.execute("PRAGMA table_info(invites)")
    invite_cols = {row["name"] for row in cur.fetchall()}

    if "invited_by_user_id" not in invite_cols:
        cur.execute("ALTER TABLE invites ADD COLUMN invited_by_user_id INTEGER")

    if "organisation_country" not in invite_cols:
        cur.execute("ALTER TABLE invites ADD COLUMN organisation_country TEXT")

    if "musician_country" not in invite_cols:
        cur.execute("ALTER TABLE invites ADD COLUMN musician_country TEXT")

    if "invite_email" not in invite_cols:
        cur.execute("ALTER TABLE invites ADD COLUMN invite_email TEXT")

    if "target_ensemble_name" not in invite_cols:
        cur.execute("ALTER TABLE invites ADD COLUMN target_ensemble_name TEXT")

    if "target_is_default_ensemble" not in invite_cols:
        cur.execute("ALTER TABLE invites ADD COLUMN target_is_default_ensemble INTEGER NOT NULL DEFAULT 0")

    if "target_section" not in invite_cols:
        cur.execute("ALTER TABLE invites ADD COLUMN target_section TEXT")

    if "invite_sent_at" not in invite_cols:
        cur.execute("ALTER TABLE invites ADD COLUMN invite_sent_at TEXT")

    if "musician_comment" not in invite_cols:
        cur.execute("ALTER TABLE invites ADD COLUMN musician_comment TEXT")

    if "confirmed_at" not in invite_cols:
        cur.execute("ALTER TABLE invites ADD COLUMN confirmed_at TEXT")

    cur.execute("""
    UPDATE invites
    SET invite_sent_at = created_at
    WHERE COALESCE(invite_sent_at, '') = ''
      AND COALESCE(created_at, '') != ''
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS organisation_default_ensemble_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        organisation_id INTEGER NOT NULL,
        ensemble_name TEXT NOT NULL,
        musician_user_id INTEGER NOT NULL,
        instrument TEXT,
        role_title TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(organisation_id, musician_user_id, instrument)
    )
    """)

    cur.execute("PRAGMA table_info(organisation_default_ensemble_members)")
    default_ensemble_cols = {row["name"] for row in cur.fetchall()}

    if "organisation_id" not in default_ensemble_cols:
        cur.execute("ALTER TABLE organisation_default_ensemble_members ADD COLUMN organisation_id INTEGER")

    if "ensemble_name" not in default_ensemble_cols:
        cur.execute("ALTER TABLE organisation_default_ensemble_members ADD COLUMN ensemble_name TEXT")

    if "musician_user_id" not in default_ensemble_cols:
        cur.execute("ALTER TABLE organisation_default_ensemble_members ADD COLUMN musician_user_id INTEGER")

    if "instrument" not in default_ensemble_cols:
        cur.execute("ALTER TABLE organisation_default_ensemble_members ADD COLUMN instrument TEXT")

    if "role_title" not in default_ensemble_cols:
        cur.execute("ALTER TABLE organisation_default_ensemble_members ADD COLUMN role_title TEXT")

    if "created_at" not in default_ensemble_cols:
        cur.execute("ALTER TABLE organisation_default_ensemble_members ADD COLUMN created_at TEXT")

# =====================================================================
# READY TO RECEIVE CONCERT LAYER — DATABASE BLOCK
# ADD INSIDE init_db() AFTER THE invites TABLE BLOCK
# =====================================================================

    cur.execute("""
    CREATE TABLE IF NOT EXISTS musician_concert_receipts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invite_id INTEGER UNIQUE,
        organisation_id INTEGER,
        musician_user_id INTEGER,
        invited_by_user_id INTEGER,
        concert_ref TEXT,
        concert_name TEXT,
        concert_date TEXT,
        current_file_label TEXT,
        current_file_token TEXT,
        current_file_sent_at TEXT,
        access_status TEXT DEFAULT 'ready',
        invite_status TEXT DEFAULT 'pending',
        invite_sent_at TEXT,
        accepted_at TEXT,
        confirmed_at TEXT,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("PRAGMA table_info(musician_concert_receipts)")
    receipt_cols = {row["name"] for row in cur.fetchall()}

    if "invite_id" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN invite_id INTEGER")

    if "organisation_id" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN organisation_id INTEGER")

    if "musician_user_id" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN musician_user_id INTEGER")

    if "invited_by_user_id" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN invited_by_user_id INTEGER")

    if "concert_ref" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN concert_ref TEXT")

    if "concert_name" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN concert_name TEXT")

    if "concert_date" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN concert_date TEXT")

    if "current_file_label" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN current_file_label TEXT")

    if "current_file_token" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN current_file_token TEXT")

    if "current_file_sent_at" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN current_file_sent_at TEXT")

    if "access_status" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN access_status TEXT DEFAULT 'ready'")

    if "invite_status" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN invite_status TEXT DEFAULT 'pending'")

    if "invite_sent_at" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN invite_sent_at TEXT")

    if "accepted_at" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN accepted_at TEXT")

    if "confirmed_at" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN confirmed_at TEXT")

    if "intro_knock_enabled" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN intro_knock_enabled INTEGER NOT NULL DEFAULT 1")

    if "intro_knock_heard_at" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN intro_knock_heard_at TEXT")

    if "is_active" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")

    if "created_at" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN created_at TEXT")

    if "updated_at" not in receipt_cols:
        cur.execute("ALTER TABLE musician_concert_receipts ADD COLUMN updated_at TEXT")

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_musician_concert_receipts_invite_id
    ON musician_concert_receipts(invite_id)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS conductor_upload_receipts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conductor_user_id INTEGER,
        conductor_email TEXT,
        concert_name TEXT,
        concert_ref TEXT,
        upload_filename TEXT,
        upload_stored_path TEXT,
        upload_timestamp TEXT,
        submit_mode TEXT,
        receipt_status TEXT DEFAULT 'idle',
        forward_status TEXT DEFAULT 'idle',
        message_note TEXT,
        batch_token TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("PRAGMA table_info(conductor_upload_receipts)")
    conductor_receipt_cols = {row["name"] for row in cur.fetchall()}

    if "conductor_user_id" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN conductor_user_id INTEGER")

    if "conductor_email" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN conductor_email TEXT")

    if "concert_name" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN concert_name TEXT")

    if "concert_ref" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN concert_ref TEXT")

    if "upload_filename" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN upload_filename TEXT")

    if "upload_stored_path" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN upload_stored_path TEXT")

    if "upload_timestamp" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN upload_timestamp TEXT")

    if "submit_mode" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN submit_mode TEXT")

    if "receipt_status" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN receipt_status TEXT DEFAULT 'idle'")

    if "forward_status" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN forward_status TEXT DEFAULT 'idle'")

    if "message_note" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN message_note TEXT")

    if "score_note" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN score_note TEXT")

    if "batch_token" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN batch_token TEXT")

    if "created_at" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN created_at TEXT")

    if "updated_at" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN updated_at TEXT")

    if "cycle_state" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN cycle_state TEXT DEFAULT 'sent'")

    if "superseded_at" not in conductor_receipt_cols:
        cur.execute("ALTER TABLE conductor_upload_receipts ADD COLUMN superseded_at TEXT")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS concert_control_forwarding_state (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        concert_name TEXT NOT NULL,
        concert_ref TEXT,
        automatic_forwarding_enabled INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("PRAGMA table_info(concert_control_forwarding_state)")
    concert_forwarding_cols = {row["name"] for row in cur.fetchall()}

    if "concert_name" not in concert_forwarding_cols:
        cur.execute("ALTER TABLE concert_control_forwarding_state ADD COLUMN concert_name TEXT")

    if "concert_ref" not in concert_forwarding_cols:
        cur.execute("ALTER TABLE concert_control_forwarding_state ADD COLUMN concert_ref TEXT")

    if "automatic_forwarding_enabled" not in concert_forwarding_cols:
        cur.execute("ALTER TABLE concert_control_forwarding_state ADD COLUMN automatic_forwarding_enabled INTEGER NOT NULL DEFAULT 0")

    if "created_at" not in concert_forwarding_cols:
        cur.execute("ALTER TABLE concert_control_forwarding_state ADD COLUMN created_at TEXT")

    if "updated_at" not in concert_forwarding_cols:
        cur.execute("ALTER TABLE concert_control_forwarding_state ADD COLUMN updated_at TEXT")

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_concert_control_forwarding_state_unique
    ON concert_control_forwarding_state(concert_name, concert_ref)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS organisation_memberships (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        organisation_id INTEGER,
        musician_user_id INTEGER,
        invited_by_user_id INTEGER,
        organisation_country TEXT,
        musician_country TEXT,
        membership_type TEXT DEFAULT 'local_pool',
        status TEXT,
        created_at TEXT,
        UNIQUE(organisation_id, musician_user_id)
    )
    """)

    cur.execute("PRAGMA table_info(organisation_memberships)")
    membership_cols = {row["name"] for row in cur.fetchall()}

    if "invited_by_user_id" not in membership_cols:
        cur.execute("ALTER TABLE organisation_memberships ADD COLUMN invited_by_user_id INTEGER")

    if "organisation_country" not in membership_cols:
        cur.execute("ALTER TABLE organisation_memberships ADD COLUMN organisation_country TEXT")

    if "musician_country" not in membership_cols:
        cur.execute("ALTER TABLE organisation_memberships ADD COLUMN musician_country TEXT")

    if "membership_type" not in membership_cols:
        cur.execute("ALTER TABLE organisation_memberships ADD COLUMN membership_type TEXT DEFAULT 'local_pool'")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS librarian_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER UNIQUE NOT NULL,
        organisation_id INTEGER,
        email TEXT NOT NULL,
        full_name TEXT,
        preferred_name TEXT,
        mobile TEXT,
        notes TEXT,
        role_title TEXT,
        organisation_name TEXT,
        ensemble_name TEXT,
        city TEXT,
        state_region_territory TEXT,
        country_code TEXT,
        country_name TEXT,
        work_email TEXT,
        global_search_visible TEXT DEFAULT 'Yes',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("PRAGMA table_info(librarian_profiles)")
    librarian_profile_cols = {row["name"] for row in cur.fetchall()}

    if "organisation_id" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN organisation_id INTEGER")

    if "email" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN email TEXT")

    if "full_name" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN full_name TEXT")

    if "preferred_name" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN preferred_name TEXT")

    if "mobile" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN mobile TEXT")

    if "notes" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN notes TEXT")

    if "role_title" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN role_title TEXT")

    if "organisation_name" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN organisation_name TEXT")

    if "ensemble_name" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN ensemble_name TEXT")

    if "city" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN city TEXT")

    if "state_region_territory" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN state_region_territory TEXT")

    if "country_code" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN country_code TEXT")

    if "country_name" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN country_name TEXT")

    if "work_email" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN work_email TEXT")

    if "global_search_visible" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN global_search_visible TEXT DEFAULT 'Yes'")

    if "created_at" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN created_at TEXT")

    if "updated_at" not in librarian_profile_cols:
        cur.execute("ALTER TABLE librarian_profiles ADD COLUMN updated_at TEXT")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS conductor_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER UNIQUE NOT NULL,
        email TEXT NOT NULL,
        full_name TEXT,
        preferred_name TEXT,
        mobile TEXT,
        notes TEXT,
        working_area TEXT,
        career_stage TEXT,
        production_types TEXT,
        known_for TEXT,
        city TEXT,
        state_region_territory TEXT,
        country_code TEXT,
        country_name TEXT,
        work_email TEXT,
        global_search_visible TEXT DEFAULT 'Yes',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("PRAGMA table_info(conductor_profiles)")
    conductor_profile_cols = {row["name"] for row in cur.fetchall()}

    if "email" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN email TEXT")

    if "full_name" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN full_name TEXT")

    if "preferred_name" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN preferred_name TEXT")

    if "mobile" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN mobile TEXT")

    if "notes" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN notes TEXT")

    if "working_area" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN working_area TEXT")

    if "career_stage" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN career_stage TEXT")

    if "production_types" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN production_types TEXT")

    if "known_for" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN known_for TEXT")

    if "city" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN city TEXT")

    if "state_region_territory" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN state_region_territory TEXT")

    if "country_code" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN country_code TEXT")

    if "country_name" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN country_name TEXT")

    if "work_email" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN work_email TEXT")

    if "global_search_visible" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN global_search_visible TEXT DEFAULT 'Yes'")

    if "created_at" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN created_at TEXT")

    if "updated_at" not in conductor_profile_cols:
        cur.execute("ALTER TABLE conductor_profiles ADD COLUMN updated_at TEXT")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS conductor_entry_invites (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        librarian_email TEXT NOT NULL,
        conductor_email TEXT NOT NULL,
        invite_token TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        used_at TEXT
    )
    """)

    cur.execute("PRAGMA table_info(conductor_entry_invites)")
    conductor_entry_invite_cols = {row["name"] for row in cur.fetchall()}

    if "librarian_email" not in conductor_entry_invite_cols:
        cur.execute("ALTER TABLE conductor_entry_invites ADD COLUMN librarian_email TEXT")

    if "conductor_email" not in conductor_entry_invite_cols:
        cur.execute("ALTER TABLE conductor_entry_invites ADD COLUMN conductor_email TEXT")

    if "invite_token" not in conductor_entry_invite_cols:
        cur.execute("ALTER TABLE conductor_entry_invites ADD COLUMN invite_token TEXT")

    if "status" not in conductor_entry_invite_cols:
        cur.execute("ALTER TABLE conductor_entry_invites ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")

    if "created_at" not in conductor_entry_invite_cols:
        cur.execute("ALTER TABLE conductor_entry_invites ADD COLUMN created_at TEXT")

    if "used_at" not in conductor_entry_invite_cols:
        cur.execute("ALTER TABLE conductor_entry_invites ADD COLUMN used_at TEXT")

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_conductor_entry_invites_token
    ON conductor_entry_invites(invite_token)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS librarian_notes_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        librarian_email TEXT NOT NULL,
        note_text TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)
    cur.execute("PRAGMA table_info(librarian_notes_entries)")
    librarian_notes_cols = {row["name"] for row in cur.fetchall()}

    if "librarian_email" not in librarian_notes_cols:
        cur.execute("ALTER TABLE librarian_notes_entries ADD COLUMN librarian_email TEXT")

    if "note_text" not in librarian_notes_cols:
        cur.execute("ALTER TABLE librarian_notes_entries ADD COLUMN note_text TEXT")

    if "created_at" not in librarian_notes_cols:
        cur.execute("ALTER TABLE librarian_notes_entries ADD COLUMN created_at TEXT")

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_librarian_notes_entries_email
    ON librarian_notes_entries(librarian_email)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS concert_control_current_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        librarian_email TEXT NOT NULL,
        librarian_country TEXT,
        concert_name TEXT NOT NULL,
        concert_ref TEXT,
        original_filename TEXT NOT NULL,
        stored_rel_path TEXT NOT NULL,
        uploaded_at TEXT NOT NULL
    )
    """)

    cur.execute("PRAGMA table_info(concert_control_current_files)")
    concert_control_current_file_cols = {row["name"] for row in cur.fetchall()}

    if "librarian_email" not in concert_control_current_file_cols:
        cur.execute("ALTER TABLE concert_control_current_files ADD COLUMN librarian_email TEXT")

    if "librarian_country" not in concert_control_current_file_cols:
        cur.execute("ALTER TABLE concert_control_current_files ADD COLUMN librarian_country TEXT")

    if "concert_name" not in concert_control_current_file_cols:
        cur.execute("ALTER TABLE concert_control_current_files ADD COLUMN concert_name TEXT")

    if "concert_ref" not in concert_control_current_file_cols:
        cur.execute("ALTER TABLE concert_control_current_files ADD COLUMN concert_ref TEXT")

    if "original_filename" not in concert_control_current_file_cols:
        cur.execute("ALTER TABLE concert_control_current_files ADD COLUMN original_filename TEXT")

    if "stored_rel_path" not in concert_control_current_file_cols:
        cur.execute("ALTER TABLE concert_control_current_files ADD COLUMN stored_rel_path TEXT")

    if "uploaded_at" not in concert_control_current_file_cols:
        cur.execute("ALTER TABLE concert_control_current_files ADD COLUMN uploaded_at TEXT")

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_concert_control_current_files_concert
    ON concert_control_current_files(concert_name, concert_ref, uploaded_at)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS global_country_venue_registry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country_code TEXT NOT NULL,
        venue_name TEXT NOT NULL,
        venue_key TEXT NOT NULL,
        city TEXT,
        capacity INTEGER,
        added_by_user_id INTEGER,
        is_system_seeded INTEGER NOT NULL DEFAULT 0,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("PRAGMA table_info(global_country_venue_registry)")
    venue_cols = {row["name"] for row in cur.fetchall()}

    if "city" not in venue_cols:
        cur.execute("ALTER TABLE global_country_venue_registry ADD COLUMN city TEXT")

    if "capacity" not in venue_cols:
        cur.execute("ALTER TABLE global_country_venue_registry ADD COLUMN capacity INTEGER")

    if "added_by_user_id" not in venue_cols:
        cur.execute("ALTER TABLE global_country_venue_registry ADD COLUMN added_by_user_id INTEGER")

    if "is_system_seeded" not in venue_cols:
        cur.execute("ALTER TABLE global_country_venue_registry ADD COLUMN is_system_seeded INTEGER NOT NULL DEFAULT 0")

    if "is_active" not in venue_cols:
        cur.execute("ALTER TABLE global_country_venue_registry ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")

    if "created_at" not in venue_cols:
        cur.execute("ALTER TABLE global_country_venue_registry ADD COLUMN created_at TEXT")

    if "updated_at" not in venue_cols:
        cur.execute("ALTER TABLE global_country_venue_registry ADD COLUMN updated_at TEXT")

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_global_country_venue_registry_unique
    ON global_country_venue_registry(country_code, venue_key)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS global_country_ensemble_registry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country_code TEXT NOT NULL,
        ensemble_name TEXT NOT NULL,
        ensemble_key TEXT NOT NULL,
        added_by_email TEXT,
        is_system_seeded INTEGER NOT NULL DEFAULT 0,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("PRAGMA table_info(global_country_ensemble_registry)")
    ensemble_cols = {row["name"] for row in cur.fetchall()}

    if "country_code" not in ensemble_cols:
        cur.execute("ALTER TABLE global_country_ensemble_registry ADD COLUMN country_code TEXT")

    if "ensemble_name" not in ensemble_cols:
        cur.execute("ALTER TABLE global_country_ensemble_registry ADD COLUMN ensemble_name TEXT")

    if "ensemble_key" not in ensemble_cols:
        cur.execute("ALTER TABLE global_country_ensemble_registry ADD COLUMN ensemble_key TEXT")

    if "added_by_email" not in ensemble_cols:
        cur.execute("ALTER TABLE global_country_ensemble_registry ADD COLUMN added_by_email TEXT")

    if "is_system_seeded" not in ensemble_cols:
        cur.execute("ALTER TABLE global_country_ensemble_registry ADD COLUMN is_system_seeded INTEGER NOT NULL DEFAULT 0")

    if "is_active" not in ensemble_cols:
        cur.execute("ALTER TABLE global_country_ensemble_registry ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")

    if "created_at" not in ensemble_cols:
        cur.execute("ALTER TABLE global_country_ensemble_registry ADD COLUMN created_at TEXT")

    if "updated_at" not in ensemble_cols:
        cur.execute("ALTER TABLE global_country_ensemble_registry ADD COLUMN updated_at TEXT")

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_global_country_ensemble_registry_unique
    ON global_country_ensemble_registry(country_code, ensemble_key)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS librarian_created_concerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        librarian_email TEXT NOT NULL,
        librarian_country TEXT,
        concert_ref TEXT NOT NULL,
        concert_name TEXT NOT NULL,
        concert_date TEXT NOT NULL,
        ensemble_name TEXT NOT NULL,
        venue_name TEXT NOT NULL,
        conductor_name TEXT NOT NULL,
        concert_tier INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        identity_locked_at TEXT,
        operational_anchor_at TEXT,
        operational_expires_at TEXT,
        life_window_days INTEGER NOT NULL DEFAULT 30,
        life_window_state TEXT NOT NULL DEFAULT 'live',
        total_send_count INTEGER NOT NULL DEFAULT 0,
        revision_send_count INTEGER NOT NULL DEFAULT 0,
        billing_total_usd REAL NOT NULL DEFAULT 0,
        high_revision_triggered_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("PRAGMA table_info(librarian_created_concerts)")
    created_concert_cols = {row["name"] for row in cur.fetchall()}

    if "librarian_email" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN librarian_email TEXT")

    if "librarian_country" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN librarian_country TEXT")

    if "concert_ref" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN concert_ref TEXT")

    if "concert_name" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN concert_name TEXT")

    if "concert_date" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN concert_date TEXT")

    if "ensemble_name" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN ensemble_name TEXT")

    if "venue_name" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN venue_name TEXT")

    if "conductor_name" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN conductor_name TEXT")

    if "concert_tier" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN concert_tier INTEGER")

    if "status" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")

    if "identity_locked_at" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN identity_locked_at TEXT")

    if "operational_anchor_at" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN operational_anchor_at TEXT")

    if "operational_expires_at" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN operational_expires_at TEXT")

    if "life_window_days" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN life_window_days INTEGER NOT NULL DEFAULT 30")

    if "life_window_state" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN life_window_state TEXT NOT NULL DEFAULT 'live'")

    if "total_send_count" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN total_send_count INTEGER NOT NULL DEFAULT 0")

    if "revision_send_count" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN revision_send_count INTEGER NOT NULL DEFAULT 0")

    if "billing_total_usd" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN billing_total_usd REAL NOT NULL DEFAULT 0")

    if "high_revision_triggered_at" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN high_revision_triggered_at TEXT")

    if "created_at" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN created_at TEXT")

    if "updated_at" not in created_concert_cols:
        cur.execute("ALTER TABLE librarian_created_concerts ADD COLUMN updated_at TEXT")

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_librarian_created_concerts_ref
    ON librarian_created_concerts(concert_ref)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS global_country_conductor_registry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country_code TEXT NOT NULL,
        conductor_name TEXT NOT NULL,
        conductor_key TEXT NOT NULL,
        added_by_email TEXT,
        is_system_seeded INTEGER NOT NULL DEFAULT 0,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("PRAGMA table_info(global_country_conductor_registry)")
    conductor_cols = {row["name"] for row in cur.fetchall()}

    if "country_code" not in conductor_cols:
        cur.execute("ALTER TABLE global_country_conductor_registry ADD COLUMN country_code TEXT")

    if "conductor_name" not in conductor_cols:
        cur.execute("ALTER TABLE global_country_conductor_registry ADD COLUMN conductor_name TEXT")

    if "conductor_key" not in conductor_cols:
        cur.execute("ALTER TABLE global_country_conductor_registry ADD COLUMN conductor_key TEXT")

    if "added_by_email" not in conductor_cols:
        cur.execute("ALTER TABLE global_country_conductor_registry ADD COLUMN added_by_email TEXT")

    if "is_system_seeded" not in conductor_cols:
        cur.execute("ALTER TABLE global_country_conductor_registry ADD COLUMN is_system_seeded INTEGER NOT NULL DEFAULT 0")

    if "is_active" not in conductor_cols:
        cur.execute("ALTER TABLE global_country_conductor_registry ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")

    if "created_at" not in conductor_cols:
        cur.execute("ALTER TABLE global_country_conductor_registry ADD COLUMN created_at TEXT")

    if "updated_at" not in conductor_cols:
        cur.execute("ALTER TABLE global_country_conductor_registry ADD COLUMN updated_at TEXT")

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_global_country_conductor_registry_unique
    ON global_country_conductor_registry(country_code, conductor_key)
    """)

    now = datetime.utcnow().isoformat()

    seeded_nz_venues = [
        ("Aotea Centre", "Auckland", 2256),
        ("Auckland Town Hall", "Auckland", 1529),
        ("Bruce Mason Centre", "Takapuna", 1090),
        ("Kiri Te Kanawa Theatre", "Auckland", 2256),
        ("Spark Arena", "Auckland", 12000),
        ("The Civic", "Auckland", 2378),
        ("Due Drop Events Centre", "Manukau", 4000),
        ("Claudelands", "Hamilton", 6000),
        ("Globox Arena", "Claudelands", 6800),
        ("Clarence St Theatre", "Hamilton", 300),
        ("Baycourt Community and Arts Centre", "Tauranga", 500),
        ("ASB Baypark Arena", "Mount Maunganui", 3500),
        ("The Opera House", "Wellington", 1388),
        ("Michael Fowler Centre", "Wellington", 2200),
        ("St James Theatre", "Wellington", 1552),
        ("TSB Arena", "Wellington", 4750),
        ("Marlborough Events Centre", "Blenheim", 900),
        ("Nelson Centre of Musical Arts", "Nelson", 330),
        ("Isaac Theatre Royal", "Christchurch", 1289),
        ("Wolfbrook Arena", "Christchurch", 8888),
        ("Christchurch Town Hall", "Christchurch", 2500),
        ("James Hay Theatre", "Christchurch", 1000),
        ("Regent Theatre", "Dunedin", 1916),
        ("Dunedin Town Hall", "Dunedin", 1000),
        ("The Piano", "Christchurch", 350),
        ("Invercargill Civic Theatre", "Invercargill", 1015),
    ]

    seeded_nz_ensembles = [
        "Auckland Philharmonia",
        "New Zealand Symphony Orchestra",
        "Christchurch Symphony Orchestra",
        "Dunedin Symphony Orchestra",
        "Orchestra Wellington",
        "Opus Orchestra",
        "Bach Musica NZ",
        "NZ Barok",
        "New Zealand String Quartet",
        "St Matthew's Chamber Orchestra",
        "Auckland Chamber Orchestra",
        "Vector Wellington Orchestra",
        "Nelson Symphony Orchestra",
        "Manukau Symphony Orchestra",
        "Trust Waikato Symphony Orchestra",
        "Bay of Plenty Symphonia",
        "Hawke's Bay Orchestra",
        "Tasman Sinfonia",
        "Southern Sinfonia",
        "Arohanui Strings",
    ]

    seeded_nz_conductors = [
        "Gemma New",
        "Giordano Bellincampi",
        "Hamish McKeich",
        "Marc Taddei",
        "James Judd",
        "Uwe Grodd",
        "Peter Walls",
        "Sarah-Grace Williams",
        "Carlos Kalmar",
        "Benjamin Northey",
        "Kenneth Young",
        "Edo de Waart",
        "Holly Mathieson",
        "Brent Stewart",
        "Vicki Chow",
    ]

    for venue_name, city, capacity in seeded_nz_venues:
        venue_key = " ".join(str(venue_name or "").strip().lower().split())
        cur.execute("""
        INSERT OR IGNORE INTO global_country_venue_registry (
            country_code,
            venue_name,
            venue_key,
            city,
            capacity,
            added_by_user_id,
            is_system_seeded,
            is_active,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, NULL, 1, 1, ?, ?)
        """, ("NZ", venue_name, venue_key, city, capacity, now, now))

    for ensemble_name in seeded_nz_ensembles:
        ensemble_key = " ".join(str(ensemble_name or "").strip().lower().split())
        cur.execute("""
        INSERT OR IGNORE INTO global_country_ensemble_registry (
            country_code,
            ensemble_name,
            ensemble_key,
            added_by_email,
            is_system_seeded,
            is_active,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, NULL, 1, 1, ?, ?)
        """, ("NZ", ensemble_name, ensemble_key, now, now))

    for conductor_name in seeded_nz_conductors:
        conductor_key = " ".join(str(conductor_name or "").strip().lower().split())
        cur.execute("""
        INSERT OR IGNORE INTO global_country_conductor_registry (
            country_code,
            conductor_name,
            conductor_key,
            added_by_email,
            is_system_seeded,
            is_active,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, NULL, 1, 1, ?, ?)
        """, ("NZ", conductor_name, conductor_key, now, now))

    conn.commit()
    conn.close()


@app.on_event("startup")
def startup():
    init_db()


# =====================================================================
# LIBRARIAN DASHBOARD CONCERT SUMMARY — STANDALONE BLOCK
# PURPOSE: LIBRARIAN DASHBOARD ONLY
# =====================================================================

LIBRARIAN_DASHBOARD_CONCERT_SUMMARY_SECTIONS = (
    "strings",
    "winds",
    "brass",
    "percussion",
    "voice",
    "guests",
)


def librarian_dashboard_concert_summary_make_route_key(concert_name: str, concert_ref: str) -> str:
    clean_name = str(concert_name or "").strip()
    clean_ref = str(concert_ref or "").strip()
    return quote_plus(f"{clean_name}|||{clean_ref}")


def librarian_dashboard_concert_summary_normalize_concert_name(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def librarian_dashboard_concert_summary_normalize_concert_ref(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def librarian_dashboard_concert_summary_section_for_instrument(instrument: str) -> str:
    value = str(instrument or "").strip().lower()

    if not value:
        return "guests"

    if any(
        token in value
        for token in [
            "violin",
            "viola",
            "cello",
            "double bass",
            "bass ",
            "contrabass",
            "harp",
        ]
    ):
        return "strings"

    if any(
        token in value
        for token in [
            "flute",
            "oboe",
            "clarinet",
            "bassoon",
            "piccolo",
            "cor anglais",
            "english horn",
            "sax",
            "recorder",
        ]
    ):
        return "winds"

    if any(
        token in value
        for token in [
            "horn",
            "trumpet",
            "trombone",
            "tuba",
            "euphonium",
            "flugel",
            "cornet",
        ]
    ):
        return "brass"

    if any(
        token in value
        for token in [
            "timp",
            "percussion",
            "drum",
            "marimba",
            "xylophone",
            "vibraphone",
            "glock",
            "cymbal",
        ]
    ):
        return "percussion"

    if any(
        token in value
        for token in [
            "voice",
            "soprano",
            "mezzo",
            "alto",
            "tenor",
            "baritone",
            "bass vocalist",
            "choir",
            "chorus",
            "vocal",
        ]
    ):
        return "voice"

    return "guests"


def librarian_dashboard_concert_summary_seal_for_light(light: str) -> str:
    clean_light = str(light or "").strip().lower()
    if clean_light == "green":
        return "/static/seal_green.png"
    if clean_light == "grey":
        return "/static/seal_amber.png"
    return "/static/seal_red.png"


def librarian_dashboard_concert_summary_fetch_concert_index():
    refresh_keys: list[tuple[str, str]] = []
    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        concerts_by_key: dict[tuple[str, str], dict] = {}

        cur.execute(
            """
            SELECT
                id,
                COALESCE(concert_name, '') AS concert_name,
                COALESCE(concert_ref, '') AS concert_ref,
                COALESCE(concert_date, '') AS concert_date,
                COALESCE(ensemble_name, '') AS ensemble_name,
                COALESCE(venue_name, '') AS venue_name,
                COALESCE(conductor_name, '') AS conductor_name,
                COALESCE(status, 'active') AS status,
                COALESCE(updated_at, '') AS updated_at
            FROM librarian_created_concerts
            ORDER BY
                COALESCE(concert_date, '') DESC,
                COALESCE(updated_at, '') DESC,
                id DESC
            """
        )
        created_rows = cur.fetchall()

        for row in created_rows:
            concert_name = librarian_dashboard_concert_summary_normalize_concert_name(row["concert_name"])
            concert_ref = librarian_dashboard_concert_summary_normalize_concert_ref(row["concert_ref"])
            key = (concert_name.lower(), concert_ref.lower())

            if concert_name or concert_ref:
                refresh_keys.append((concert_name, concert_ref))

            raw_concert_date = str(row["concert_date"] or "").strip()
            display_concert_date = raw_concert_date or "—"
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
                try:
                    display_concert_date = datetime.strptime(raw_concert_date, fmt).strftime("%d %B %Y")
                    break
                except ValueError:
                    pass

            concerts_by_key[key] = {
                "id": librarian_dashboard_concert_summary_make_route_key(concert_name, concert_ref),
                "name": concert_name or "Concert pending issue",
                "concert_ref": concert_ref,
                "concert_date_nz": display_concert_date,
                "ensemble": str(row["ensemble_name"] or "").strip() or "Not yet set.",
                "venue": str(row["venue_name"] or "").strip() or "Not yet set.",
                "conductor": str(row["conductor_name"] or "").strip() or "Not yet set.",
                "status": str(row["status"] or "").strip() or "active",
            }

        cur.execute(
            """
            SELECT
                MAX(id) AS latest_id,
                COALESCE(concert_name, '') AS concert_name,
                COALESCE(concert_ref, '') AS concert_ref,
                COALESCE(MAX(concert_date), '') AS concert_date,
                COALESCE(MAX(updated_at), '') AS updated_at
            FROM musician_concert_receipts
            WHERE COALESCE(is_active, 0)=1
              AND (
                    COALESCE(concert_name, '') != ''
                 OR COALESCE(concert_ref, '') != ''
              )
            GROUP BY
                lower(trim(COALESCE(concert_name, ''))),
                lower(trim(COALESCE(concert_ref, '')))
            ORDER BY
                COALESCE(MAX(concert_date), '') DESC,
                COALESCE(MAX(updated_at), '') DESC,
                MAX(id) DESC
            """
        )
        receipt_rows = cur.fetchall()

        for row in receipt_rows:
            concert_name = librarian_dashboard_concert_summary_normalize_concert_name(row["concert_name"])
            concert_ref = librarian_dashboard_concert_summary_normalize_concert_ref(row["concert_ref"])
            key = (concert_name.lower(), concert_ref.lower())

            if concert_name or concert_ref:
                refresh_keys.append((concert_name, concert_ref))

            if key not in concerts_by_key:
                raw_concert_date = str(row["concert_date"] or "").strip()
                display_concert_date = raw_concert_date or "—"
                for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
                    try:
                        display_concert_date = datetime.strptime(raw_concert_date, fmt).strftime("%d %B %Y")
                        break
                    except ValueError:
                        pass

                concerts_by_key[key] = {
                    "id": librarian_dashboard_concert_summary_make_route_key(concert_name, concert_ref),
                    "name": concert_name or "Concert pending issue",
                    "concert_ref": concert_ref,
                    "concert_date_nz": display_concert_date,
                    "ensemble": "Not yet set.",
                    "venue": "Not yet set.",
                    "conductor": "Not yet set.",
                    "status": "active",
                }
            else:
                raw_concert_date = str(row["concert_date"] or "").strip()
                if raw_concert_date:
                    display_concert_date = raw_concert_date
                    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
                        try:
                            display_concert_date = datetime.strptime(raw_concert_date, fmt).strftime("%d %B %Y")
                            break
                        except ValueError:
                            pass
                    concerts_by_key[key]["concert_date_nz"] = display_concert_date

        cur.execute(
            """
            SELECT
                MAX(id) AS latest_id,
                COALESCE(concert_name, '') AS concert_name,
                COALESCE(concert_ref, '') AS concert_ref,
                COALESCE(MAX(upload_timestamp), '') AS upload_timestamp
            FROM conductor_upload_receipts
            WHERE lower(COALESCE(cycle_state, 'sent')) != 'superseded'
              AND (
                    COALESCE(concert_name, '') != ''
                 OR COALESCE(concert_ref, '') != ''
              )
            GROUP BY
                lower(trim(COALESCE(concert_name, ''))),
                lower(trim(COALESCE(concert_ref, '')))
            ORDER BY
                COALESCE(MAX(upload_timestamp), '') DESC,
                MAX(id) DESC
            """
        )
        conductor_rows = cur.fetchall()

        for row in conductor_rows:
            concert_name = librarian_dashboard_concert_summary_normalize_concert_name(row["concert_name"])
            concert_ref = librarian_dashboard_concert_summary_normalize_concert_ref(row["concert_ref"])
            key = (concert_name.lower(), concert_ref.lower())

            if concert_name or concert_ref:
                refresh_keys.append((concert_name, concert_ref))

            if key not in concerts_by_key:
                concerts_by_key[key] = {
                    "id": librarian_dashboard_concert_summary_make_route_key(concert_name, concert_ref),
                    "name": concert_name or "Concert pending issue",
                    "concert_ref": concert_ref,
                    "concert_date_nz": "—",
                    "ensemble": "Not yet set.",
                    "venue": "Not yet set.",
                    "conductor": "Conductor upload received",
                    "status": "active",
                }
            else:
                concerts_by_key[key]["conductor"] = "Conductor upload received"
                
    finally:
        if conn is not None:
            conn.close()

    seen_refresh_keys: set[tuple[str, str]] = set()
    for concert_name, concert_ref in refresh_keys:
        refresh_key = (concert_name.lower(), concert_ref.lower())
        if refresh_key in seen_refresh_keys:
            continue
        seen_refresh_keys.add(refresh_key)

        summary = concert_identity_lock_refresh(concert_name, concert_ref)
        if not summary:
            continue

        if refresh_key not in concerts_by_key:
            concerts_by_key[refresh_key] = {
                "id": librarian_dashboard_concert_summary_make_route_key(
                    str(summary.get("concert_name") or concert_name),
                    str(summary.get("concert_ref") or concert_ref),
                ),
                "name": str(summary.get("concert_name") or concert_name).strip() or "Concert pending issue",
                "concert_ref": str(summary.get("concert_ref") or concert_ref).strip(),
                "concert_date_nz": "—",
                "ensemble": "Not yet set.",
                "venue": "Not yet set.",
                "conductor": "Not yet set.",
                "status": "active",
            }

        concerts_by_key[refresh_key]["id"] = librarian_dashboard_concert_summary_make_route_key(
            str(summary.get("concert_name") or concert_name),
            str(summary.get("concert_ref") or concert_ref),
        )
        concerts_by_key[refresh_key]["name"] = (
            str(summary.get("concert_name") or concert_name).strip() or "Concert pending issue"
        )
        concerts_by_key[refresh_key]["concert_ref"] = str(summary.get("concert_ref") or concert_ref).strip()
        concerts_by_key[refresh_key]["status"] = (
            str(summary.get("concert_status") or concerts_by_key[refresh_key].get("status") or "active").strip()
            or "active"
        )
        concerts_by_key[refresh_key]["life_window_state"] = (
            str(summary.get("life_window_state") or "").strip() or "live"
        )
        concerts_by_key[refresh_key]["life_window_days"] = int(summary.get("life_window_days") or 30)
        concerts_by_key[refresh_key]["total_send_count"] = int(summary.get("total_send_count") or 0)
        concerts_by_key[refresh_key]["revision_send_count"] = int(summary.get("revision_send_count") or 0)
        concerts_by_key[refresh_key]["billing_total_usd"] = float(summary.get("billing_total_usd") or 0)
        concerts_by_key[refresh_key]["high_revision_active"] = bool(summary.get("high_revision_active"))
        concerts_by_key[refresh_key]["high_revision_triggered_at"] = (
            str(summary.get("high_revision_triggered_at") or "").strip()
        )

    concerts = list(concerts_by_key.values())
    concerts.sort(
        key=lambda item: (
            str(item.get("concert_date_nz") or ""),
            str(item.get("name") or "").lower(),
            str(item.get("concert_ref") or "").lower(),
        ),
        reverse=True,
    )
    return concerts


def librarian_dashboard_concert_summary_fetch_section_lights(
    concert_name: str,
    concert_ref: str,
) -> dict[str, dict]:
    result: dict[str, dict] = {
        section_key: {
            "light": "grey",
            "label": "No Invite Wave",
            "seal": librarian_dashboard_concert_summary_seal_for_light("grey"),
            "active": 0,
            "ready": 0,
            "confirmed": 0,
            "missing_current": 0,
            "pending": 0,
            "accepted": 0,
            "declined": 0,
            "away": 0,
            "unresolved": 0,
            "landed": 0,
        }
        for section_key in LIBRARIAN_DASHBOARD_CONCERT_SUMMARY_SECTIONS
    }

    clean_name = librarian_dashboard_concert_summary_normalize_concert_name(concert_name)
    clean_ref = librarian_dashboard_concert_summary_normalize_concert_ref(concert_ref)

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        if clean_name and clean_ref:
            cur.execute(
                """
                SELECT
                    COALESCE(i.target_section, m.primary_instrument, m.instrument, '') AS target_section,
                    lower(trim(COALESCE(r.invite_status, i.status, 'pending'))) AS invite_status,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM organisation_memberships om
                            WHERE om.organisation_id = r.organisation_id
                              AND om.musician_user_id = r.musician_user_id
                              AND lower(trim(COALESCE(om.status, ''))) = 'accepted'
                        ) THEN 1 ELSE 0
                    END AS has_membership,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM organisation_default_ensemble_members dem
                            WHERE dem.organisation_id = r.organisation_id
                              AND dem.musician_user_id = r.musician_user_id
                              AND lower(trim(COALESCE(dem.ensemble_name, ''))) = lower(trim(COALESCE(i.target_ensemble_name, '')))
                        ) THEN 1 ELSE 0
                    END AS has_default_ensemble,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM organisation_default_ensemble_members dem
                            WHERE dem.organisation_id = r.organisation_id
                              AND dem.musician_user_id = r.musician_user_id
                              AND lower(trim(COALESCE(dem.ensemble_name, ''))) = lower(trim(COALESCE(i.target_ensemble_name, '')))
                              AND lower(trim(COALESCE(dem.instrument, ''))) = lower(trim(COALESCE(i.target_section, m.primary_instrument, m.instrument, '')))
                        ) THEN 1 ELSE 0
                    END AS has_target_section
                FROM musician_concert_receipts r
                LEFT JOIN invites i ON i.id = r.invite_id
                LEFT JOIN musicians m ON m.user_id = r.musician_user_id
                WHERE lower(trim(COALESCE(r.concert_name, '')))=lower(?)
                  AND lower(trim(COALESCE(r.concert_ref, '')))=lower(?)
                ORDER BY r.id ASC
                """,
                (clean_name, clean_ref),
            )
        elif clean_ref:
            cur.execute(
                """
                SELECT
                    COALESCE(i.target_section, m.primary_instrument, m.instrument, '') AS target_section,
                    lower(trim(COALESCE(r.invite_status, i.status, 'pending'))) AS invite_status,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM organisation_memberships om
                            WHERE om.organisation_id = r.organisation_id
                              AND om.musician_user_id = r.musician_user_id
                              AND lower(trim(COALESCE(om.status, ''))) = 'accepted'
                        ) THEN 1 ELSE 0
                    END AS has_membership,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM organisation_default_ensemble_members dem
                            WHERE dem.organisation_id = r.organisation_id
                              AND dem.musician_user_id = r.musician_user_id
                              AND lower(trim(COALESCE(dem.ensemble_name, ''))) = lower(trim(COALESCE(i.target_ensemble_name, '')))
                        ) THEN 1 ELSE 0
                    END AS has_default_ensemble,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM organisation_default_ensemble_members dem
                            WHERE dem.organisation_id = r.organisation_id
                              AND dem.musician_user_id = r.musician_user_id
                              AND lower(trim(COALESCE(dem.ensemble_name, ''))) = lower(trim(COALESCE(i.target_ensemble_name, '')))
                              AND lower(trim(COALESCE(dem.instrument, ''))) = lower(trim(COALESCE(i.target_section, m.primary_instrument, m.instrument, '')))
                        ) THEN 1 ELSE 0
                    END AS has_target_section
                FROM musician_concert_receipts r
                LEFT JOIN invites i ON i.id = r.invite_id
                LEFT JOIN musicians m ON m.user_id = r.musician_user_id
                WHERE lower(trim(COALESCE(r.concert_ref, '')))=lower(?)
                ORDER BY r.id ASC
                """,
                (clean_ref,),
            )
        else:
            cur.execute(
                """
                SELECT
                    COALESCE(i.target_section, m.primary_instrument, m.instrument, '') AS target_section,
                    lower(trim(COALESCE(r.invite_status, i.status, 'pending'))) AS invite_status,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM organisation_memberships om
                            WHERE om.organisation_id = r.organisation_id
                              AND om.musician_user_id = r.musician_user_id
                              AND lower(trim(COALESCE(om.status, ''))) = 'accepted'
                        ) THEN 1 ELSE 0
                    END AS has_membership,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM organisation_default_ensemble_members dem
                            WHERE dem.organisation_id = r.organisation_id
                              AND dem.musician_user_id = r.musician_user_id
                              AND lower(trim(COALESCE(dem.ensemble_name, ''))) = lower(trim(COALESCE(i.target_ensemble_name, '')))
                        ) THEN 1 ELSE 0
                    END AS has_default_ensemble,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM organisation_default_ensemble_members dem
                            WHERE dem.organisation_id = r.organisation_id
                              AND dem.musician_user_id = r.musician_user_id
                              AND lower(trim(COALESCE(dem.ensemble_name, ''))) = lower(trim(COALESCE(i.target_ensemble_name, '')))
                              AND lower(trim(COALESCE(dem.instrument, ''))) = lower(trim(COALESCE(i.target_section, m.primary_instrument, m.instrument, '')))
                        ) THEN 1 ELSE 0
                    END AS has_target_section
                FROM musician_concert_receipts r
                LEFT JOIN invites i ON i.id = r.invite_id
                LEFT JOIN musicians m ON m.user_id = r.musician_user_id
                WHERE lower(trim(COALESCE(r.concert_name, '')))=lower(?)
                ORDER BY r.id ASC
                """,
                (clean_name,),
            )

        rows = cur.fetchall()

        for row in rows:
            section_key = librarian_dashboard_concert_summary_section_for_instrument(row["target_section"])
            bucket = result[section_key]
            invite_status = str(row["invite_status"] or "").strip().lower()
            landed_correctly = (
                int(row["has_membership"] or 0) == 1
                and int(row["has_default_ensemble"] or 0) == 1
                and int(row["has_target_section"] or 0) == 1
            )

            bucket["active"] += 1

            if invite_status == "accepted":
                bucket["accepted"] += 1
                if landed_correctly:
                    bucket["landed"] += 1
                else:
                    bucket["unresolved"] += 1
                continue

            if invite_status == "pending" or not invite_status:
                bucket["pending"] += 1
                continue

            if invite_status == "decline" or invite_status == "declined":
                bucket["declined"] += 1
                continue

            if invite_status == "away":
                bucket["away"] += 1
                continue

            bucket["unresolved"] += 1

        for section_key in LIBRARIAN_DASHBOARD_CONCERT_SUMMARY_SECTIONS:
            bucket = result[section_key]
            active = int(bucket["active"] or 0)
            pending = int(bucket["pending"] or 0)
            accepted = int(bucket["accepted"] or 0)
            declined = int(bucket["declined"] or 0)
            away = int(bucket["away"] or 0)
            unresolved = int(bucket["unresolved"] or 0)
            landed = int(bucket["landed"] or 0)

            if active <= 0:
                bucket["light"] = "grey"
                bucket["label"] = "No Invite Wave"
                bucket["seal"] = librarian_dashboard_concert_summary_seal_for_light("grey")
                continue

            if accepted == active and landed == active:
                bucket["light"] = "green"
                bucket["label"] = "Invite Wave Complete"
                bucket["seal"] = librarian_dashboard_concert_summary_seal_for_light("green")
                continue

            if declined > 0 or away > 0 or unresolved > 0:
                bucket["light"] = "red"
                bucket["label"] = "Action Required"
                bucket["seal"] = librarian_dashboard_concert_summary_seal_for_light("red")
                continue

            if pending > 0 or accepted < active:
                bucket["light"] = "grey"
                bucket["label"] = "Awaiting Responses"
                bucket["seal"] = librarian_dashboard_concert_summary_seal_for_light("grey")
                continue

            bucket["light"] = "red"
            bucket["label"] = "Action Required"
            bucket["seal"] = librarian_dashboard_concert_summary_seal_for_light("red")

        return result
    finally:
        if conn is not None:
            conn.close()


def librarian_dashboard_concert_summary_fetch_snapshot() -> dict:
    concerts = librarian_dashboard_concert_summary_fetch_concert_index()
    lights_by_concert: dict[str, dict[str, dict]] = {}
    fully_green_count = 0
    red_sections_count = 0

    for concert in concerts:
        section_lights = librarian_dashboard_concert_summary_fetch_section_lights(
            concert["name"],
            concert["concert_ref"],
        )
        lights_by_concert[concert["id"]] = section_lights

        visible_lights = [
            str(section_lights[section_key]["light"] or "").strip().lower()
            for section_key in LIBRARIAN_DASHBOARD_CONCERT_SUMMARY_SECTIONS
        ]
        non_grey_lights = [light for light in visible_lights if light != "grey"]

        if non_grey_lights and all(light == "green" for light in non_grey_lights):
            fully_green_count += 1

        red_sections_count += sum(1 for light in visible_lights if light == "red")

    return {
        "concerts": concerts,
        "fully_green_count": fully_green_count,
        "red_sections_count": red_sections_count,
        "lights_by_concert": lights_by_concert,
    }


# =====================================================================
# CONCERT CONTROL ROUTE KEY — STANDALONE BLOCK
# PURPOSE: CONCERT CONTROL REDIRECTS ONLY
# =====================================================================

def concert_control_route_key_parse(concert_key: str) -> tuple[str, str]:
    decoded = unquote_plus(str(concert_key or "").strip())
    if "|||" not in decoded:
        return decoded, ""
    clean_name, clean_ref = decoded.split("|||", 1)
    return str(clean_name or "").strip(), str(clean_ref or "").strip()


# =====================================================================
# CONCERT CONTROL CURRENT FILE SOURCE — STANDALONE BLOCK
# PURPOSE: CONCERT CONTROL CURRENT FILE ONLY
# =====================================================================

def concert_control_current_file_fetch(
    concert_name: str,
    concert_ref: str,
):
    clean_name = " ".join(str(concert_name or "").strip().split())
    clean_ref = " ".join(str(concert_ref or "").strip().split())

    if not clean_name:
        return None

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT
            id,
            COALESCE(original_filename, '') AS original_filename,
            COALESCE(stored_rel_path, '') AS stored_rel_path,
            COALESCE(uploaded_at, '') AS uploaded_at
        FROM concert_control_current_files
        WHERE lower(trim(COALESCE(concert_name, '')))=lower(?)
          AND lower(trim(COALESCE(concert_ref, '')))=lower(?)
        ORDER BY
            COALESCE(uploaded_at, '') DESC,
            id DESC
        LIMIT 1
        """, (
            clean_name,
            clean_ref,
        ))
        return cur.fetchone()
    finally:
        if conn is not None:
            conn.close()


def concert_control_current_file_list(
    concert_name: str,
    concert_ref: str,
) -> list[dict]:
    clean_name = " ".join(str(concert_name or "").strip().split())
    clean_ref = " ".join(str(concert_ref or "").strip().split())

    if not clean_name:
        return []

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT
            id,
            COALESCE(original_filename, '') AS original_filename,
            COALESCE(stored_rel_path, '') AS stored_rel_path,
            COALESCE(uploaded_at, '') AS uploaded_at
        FROM concert_control_current_files
        WHERE lower(trim(COALESCE(concert_name, '')))=lower(?)
          AND lower(trim(COALESCE(concert_ref, '')))=lower(?)
        ORDER BY
            COALESCE(uploaded_at, '') DESC,
            id DESC
        """, (
            clean_name,
            clean_ref,
        ))
        return [dict(row) for row in cur.fetchall()]
    finally:
        if conn is not None:
            conn.close()


def concert_control_current_file_store(
    librarian_email: str,
    concert_name: str,
    concert_ref: str,
    original_filename: str,
    file_bytes: bytes,
) -> bool:
    clean_email = str(librarian_email or "").strip().lower()
    clean_name = " ".join(str(concert_name or "").strip().split())
    clean_ref = " ".join(str(concert_ref or "").strip().split())
    clean_filename = Path(str(original_filename or "").strip()).name
    uploaded_at = datetime.utcnow().isoformat()

    if not clean_email or not clean_name or not clean_filename or not file_bytes:
        return False

    upload_dir = APP_DIR / "concert_control_uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    stamped_name = (
        f"{uploaded_at[:19].replace(':', '-').replace('T', '_')}"
        f"__concert_control__{clean_filename}"
    )
    stored_path = upload_dir / stamped_name
    stored_path.write_bytes(file_bytes)
    stored_rel_path = str(stored_path.relative_to(APP_DIR)).replace("\\", "/")

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO concert_control_current_files (
            librarian_email,
            librarian_country,
            concert_name,
            concert_ref,
            original_filename,
            stored_rel_path,
            uploaded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            clean_email,
            librarian_country_code_by_email(clean_email),
            clean_name,
            clean_ref,
            clean_filename,
            stored_rel_path,
            uploaded_at,
        ))
        conn.commit()
        return True
    finally:
        if conn is not None:
            conn.close()


# =====================================================================
# CONCERT CONTROL DETAIL HELPERS — STANDALONE BLOCK
# PURPOSE: CONCERT CONTROL DETAIL PAGE ONLY
# =====================================================================

def concert_control_detail_normalize_concert_name(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def concert_control_detail_normalize_concert_ref(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def concert_control_detail_section_for_instrument(instrument: str) -> str:
    value = str(instrument or "").strip().lower()

    if not value:
        return "guests"

    if any(
        token in value
        for token in [
            "violin",
            "viola",
            "cello",
            "double bass",
            "bass ",
            "contrabass",
            "harp",
        ]
    ):
        return "strings"

    if any(
        token in value
        for token in [
            "flute",
            "oboe",
            "clarinet",
            "bassoon",
            "piccolo",
            "cor anglais",
            "english horn",
            "sax",
            "recorder",
        ]
    ):
        return "winds"

    if any(
        token in value
        for token in [
            "horn",
            "trumpet",
            "trombone",
            "tuba",
            "euphonium",
            "flugel",
            "cornet",
        ]
    ):
        return "brass"

    if any(
        token in value
        for token in [
            "timp",
            "percussion",
            "drum",
            "marimba",
            "xylophone",
            "vibraphone",
            "glock",
            "cymbal",
        ]
    ):
        return "percussion"

    if any(
        token in value
        for token in [
            "voice",
            "soprano",
            "mezzo",
            "alto",
            "tenor",
            "baritone",
            "bass vocalist",
            "choir",
            "chorus",
            "vocal",
        ]
    ):
        return "voice"

    return "guests"


def concert_control_detail_fetch(
    concert_name: str,
    concert_ref: str,
) -> dict:
    clean_name = concert_control_detail_normalize_concert_name(concert_name)
    clean_ref = concert_control_detail_normalize_concert_ref(concert_ref)

    detail = {
        "concert_name": clean_name or "Not yet set.",
        "concert_ref": clean_ref or "Not yet set.",
        "concert_date": "Not yet set.",
        "conductor": "Not yet set.",
        "concert_tier": 1,
        "concert_status": "active",
        "identity_locked_at": "",
        "operational_anchor_at": "",
        "operational_expires_at": "",
        "life_window_days": 30,
        "life_window_state": "live",
        "total_send_count": 0,
        "revision_send_count": 0,
        "billing_total_usd": 0.0,
        "high_revision_active": False,
        "high_revision_triggered_at": "",
        "selected_musicians": 0,
        "available_musicians": 0,
        "seats_left": 0,
        "has_current_file": False,
        "current_file_label": "No current authorised file selected",
        "current_file_count": 0,
        "section_rows": {section_name: [] for section_name in CONCERT_CONTROL_SECTIONS},
        "section_counts": {
            section_name: {"selected": 0, "available": 0, "qty": 0}
            for section_name in CONCERT_CONTROL_SECTIONS
        },
    }

    if not clean_name and not clean_ref:
        return detail

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        if clean_name and clean_ref:
            cur.execute(
                """
                SELECT
                    COALESCE(concert_name, '') AS concert_name,
                    COALESCE(concert_ref, '') AS concert_ref,
                    COALESCE(concert_date, '') AS concert_date,
                    COALESCE(conductor_name, '') AS conductor_name,
                    COALESCE(concert_tier, 1) AS concert_tier,
                    COALESCE(status, 'active') AS status
                FROM librarian_created_concerts
                WHERE lower(trim(COALESCE(concert_name, '')))=lower(?)
                  AND lower(trim(COALESCE(concert_ref, '')))=lower(?)
                ORDER BY id DESC
                LIMIT 1
                """,
                (clean_name, clean_ref),
            )
        elif clean_ref:
            cur.execute(
                """
                SELECT
                    COALESCE(concert_name, '') AS concert_name,
                    COALESCE(concert_ref, '') AS concert_ref,
                    COALESCE(concert_date, '') AS concert_date,
                    COALESCE(conductor_name, '') AS conductor_name,
                    COALESCE(concert_tier, 1) AS concert_tier,
                    COALESCE(status, 'active') AS status
                FROM librarian_created_concerts
                WHERE lower(trim(COALESCE(concert_ref, '')))=lower(?)
                ORDER BY id DESC
                LIMIT 1
                """,
                (clean_ref,),
            )
        else:
            cur.execute(
                """
                SELECT
                    COALESCE(concert_name, '') AS concert_name,
                    COALESCE(concert_ref, '') AS concert_ref,
                    COALESCE(concert_date, '') AS concert_date,
                    COALESCE(conductor_name, '') AS conductor_name,
                    COALESCE(concert_tier, 1) AS concert_tier,
                    COALESCE(status, 'active') AS status
                FROM librarian_created_concerts
                WHERE lower(trim(COALESCE(concert_name, '')))=lower(?)
                ORDER BY id DESC
                LIMIT 1
                """,
                (clean_name,),
            )
        created_concert_row = cur.fetchone()

        if created_concert_row:
            detail["concert_name"] = (
                concert_control_detail_normalize_concert_name(created_concert_row["concert_name"])
                or detail["concert_name"]
            )
            detail["concert_ref"] = (
                concert_control_detail_normalize_concert_ref(created_concert_row["concert_ref"])
                or detail["concert_ref"]
            )
            raw_concert_date = str(created_concert_row["concert_date"] or "").strip()
            detail["concert_date"] = raw_concert_date or "Not yet set."
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
                try:
                    detail["concert_date"] = datetime.strptime(raw_concert_date, fmt).strftime("%d %B %Y")
                    break
                except ValueError:
                    pass
            detail["conductor"] = str(created_concert_row["conductor_name"] or "").strip() or "Not yet set."
            detail["concert_tier"] = int(created_concert_row["concert_tier"] or 1)
            detail["concert_status"] = str(created_concert_row["status"] or "active").strip() or "active"

        cur.execute(
            """
            SELECT
                COALESCE(r.concert_name, '') AS concert_name,
                COALESCE(r.concert_ref, '') AS concert_ref,
                COALESCE(r.concert_date, '') AS concert_date,
                COALESCE(r.current_file_label, '') AS current_file_label,
                COALESCE(r.current_file_token, '') AS current_file_token,
                COALESCE(r.current_file_sent_at, '') AS current_file_sent_at,
                COALESCE(r.invite_status, '') AS invite_status,
                COALESCE(r.confirmed_at, '') AS confirmed_at,
                COALESCE(r.is_active, 0) AS is_active,
                COALESCE(u.name, u.email, 'Unknown musician') AS musician_name,
                COALESCE(u.email, '') AS musician_email,
                COALESCE(m.instrument, '') AS instrument
            FROM musician_concert_receipts r
            LEFT JOIN users u ON u.id = r.musician_user_id
            LEFT JOIN musicians m ON m.user_id = r.musician_user_id
            WHERE lower(trim(COALESCE(r.concert_name, '')))=lower(?)
              AND lower(trim(COALESCE(r.concert_ref, '')))=lower(?)
            ORDER BY
                lower(COALESCE(m.instrument, '')),
                lower(COALESCE(u.name, u.email, ''))
            """,
            (clean_name, clean_ref),
        )
        receipt_rows = cur.fetchall()

        if receipt_rows:
            first_row = receipt_rows[0]
            detail["concert_name"] = (
                concert_control_detail_normalize_concert_name(first_row["concert_name"])
                or detail["concert_name"]
            )
            detail["concert_ref"] = (
                concert_control_detail_normalize_concert_ref(first_row["concert_ref"])
                or detail["concert_ref"]
            )
            raw_concert_date = str(first_row["concert_date"] or "").strip()
            if raw_concert_date:
                detail["concert_date"] = raw_concert_date
                for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
                    try:
                        detail["concert_date"] = datetime.strptime(raw_concert_date, fmt).strftime("%d %B %Y")
                        break
                    except ValueError:
                        pass

        cur.execute(
            """
            SELECT
                COALESCE(concert_name, '') AS concert_name,
                COALESCE(concert_ref, '') AS concert_ref,
                COALESCE(upload_filename, '') AS upload_filename,
                COALESCE(upload_timestamp, '') AS upload_timestamp
            FROM conductor_upload_receipts
            WHERE lower(COALESCE(cycle_state, 'sent'))!='superseded'
              AND lower(trim(COALESCE(concert_name, '')))=lower(?)
              AND lower(trim(COALESCE(concert_ref, '')))=lower(?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (clean_name, clean_ref),
        )
        conductor_row = cur.fetchone()

        if conductor_row:
            detail["conductor"] = "Conductor upload received"

        current_file_rows = concert_control_current_file_list(clean_name, clean_ref)
        if current_file_rows:
            detail["has_current_file"] = True
            detail["current_file_count"] = len(current_file_rows)

            current_file_names = []
            for current_file_row in current_file_rows:
                current_file_name = str(current_file_row.get("original_filename") or "").strip()
                if current_file_name:
                    current_file_names.append(current_file_name)

            if current_file_names:
                detail["current_file_label"] = f'{len(current_file_names)} file(s) in current update'
            else:
                detail["current_file_label"] = "Current file active"

        for row in receipt_rows:
            is_active = int(row["is_active"] or 0) == 1
            invite_status = str(row["invite_status"] or "").strip().lower()
            if not is_active or invite_status != "accepted":
                continue

            section_key = concert_control_detail_section_for_instrument(row["instrument"])
            section_name = {
                "strings": "Strings",
                "winds": "Winds",
                "brass": "Brass",
                "percussion": "Percussion",
                "voice": "Voice",
                "guests": "Guests",
            }.get(section_key, "Guests")

            has_current_file = any(
                str(row[column] or "").strip()
                for column in ["current_file_label", "current_file_token", "current_file_sent_at"]
            )
            confirmed = "Yes" if str(row["confirmed_at"] or "").strip() else "No"

            detail["selected_musicians"] += 1
            detail["section_counts"][section_name]["selected"] += 1
            detail["section_counts"][section_name]["qty"] += 1

            if has_current_file:
                detail["has_current_file"] = True
                if detail["current_file_label"] == "No current authorised file selected":
                    detail["current_file_label"] = (
                        str(row["current_file_label"] or "").strip()
                        or "Current file active"
                    )

            detail["section_rows"][section_name].append(
                {
                    "musician_name": str(row["musician_name"] or "").strip() or "Unknown musician",
                    "musician_email": str(row["musician_email"] or "").strip() or "—",
                    "instrument": str(row["instrument"] or "").strip() or section_name,
                    "confirmed": confirmed,
                    "has_current_file": "Yes" if has_current_file else "No",
                }
            )

        detail["available_musicians"] = detail["selected_musicians"]
        detail["seats_left"] = 0

        for section_name in CONCERT_CONTROL_SECTIONS:
            detail["section_counts"][section_name]["available"] = detail["section_counts"][section_name]["selected"]

        if not detail["has_current_file"]:
            detail["current_file_label"] = "No current authorised file selected"

        summary = concert_identity_lock_refresh(detail["concert_name"], detail["concert_ref"])
        if summary:
            detail["concert_tier"] = int(summary["concert_tier"] or detail["concert_tier"])
            detail["concert_status"] = str(summary["concert_status"] or detail["concert_status"]).strip() or detail["concert_status"]
            detail["identity_locked_at"] = str(summary["identity_locked_at"] or "").strip()
            detail["operational_anchor_at"] = str(summary["operational_anchor_at"] or "").strip()
            detail["operational_expires_at"] = str(summary["operational_expires_at"] or "").strip()
            detail["life_window_days"] = int(summary["life_window_days"] or detail["life_window_days"])
            detail["life_window_state"] = str(summary["life_window_state"] or detail["life_window_state"]).strip() or detail["life_window_state"]
            detail["total_send_count"] = int(summary["total_send_count"] or 0)
            detail["revision_send_count"] = int(summary["revision_send_count"] or 0)
            detail["billing_total_usd"] = float(summary["billing_total_usd"] or 0)
            detail["high_revision_active"] = bool(summary["high_revision_active"])
            detail["high_revision_triggered_at"] = str(summary["high_revision_triggered_at"] or "").strip()

        return detail
    finally:
        if conn is not None:
            conn.close()


# =====================================================================
# LIBRARIAN COUNTRY + ENSEMBLE SOURCE — STANDALONE BLOCK
# PURPOSE: CREATE CONCERT DROPDOWN ONLY
# =====================================================================

def librarian_country_code_by_email(librarian_email: str) -> str:
    clean_email = str(librarian_email or "").strip().lower()
    if not clean_email:
        return ""

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT COALESCE(o.country, '') AS country_code
        FROM users u
        LEFT JOIN organisations o ON o.id = u.organisation_id
        WHERE lower(u.email)=lower(?)
          AND u.role='librarian'
        LIMIT 1
        """, (clean_email,))
        row = cur.fetchone()
        if row and str(row["country_code"] or "").strip():
            return str(row["country_code"] or "").strip().upper()

        if clean_email == "librarian@local":
            return "NZ"

        return ""
    finally:
        if conn is not None:
            conn.close()


def librarian_dashboard_ensemble_names(librarian_email: str) -> list[str]:
    country_code = librarian_country_code_by_email(librarian_email)
    if not country_code:
        return []

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT DISTINCT COALESCE(ensemble_name, '') AS ensemble_name
        FROM global_country_ensemble_registry
        WHERE country_code=?
          AND COALESCE(is_active, 1)=1
          AND trim(COALESCE(ensemble_name, '')) != ''
        ORDER BY lower(trim(COALESCE(ensemble_name, '')))
        """, (country_code,))
        return [str(row["ensemble_name"] or "").strip() for row in cur.fetchall() if str(row["ensemble_name"] or "").strip()]
    finally:
        if conn is not None:
            conn.close()


def librarian_dashboard_conductor_names(librarian_email: str) -> list[str]:
    country_code = librarian_country_code_by_email(librarian_email)
    if not country_code:
        return []

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT DISTINCT COALESCE(conductor_name, '') AS conductor_name
        FROM global_country_conductor_registry
        WHERE country_code=?
          AND COALESCE(is_active, 1)=1
          AND trim(COALESCE(conductor_name, '')) != ''
        ORDER BY lower(trim(COALESCE(conductor_name, '')))
        """, (country_code,))
        return [str(row["conductor_name"] or "").strip() for row in cur.fetchall() if str(row["conductor_name"] or "").strip()]
    finally:
        if conn is not None:
            conn.close()


# =====================================================================
# LIBRARIAN CREATED CONCERTS — STANDALONE BLOCK
# PURPOSE: ZERO-MUSICIAN CONCERT CREATION ONLY
# =====================================================================

def librarian_created_concerts_clean_text(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def librarian_created_concerts_make_ref(
    librarian_email: str,
    concert_name: str,
    concert_date: str,
) -> str:
    clean_email = librarian_created_concerts_clean_text(librarian_email).lower().replace(" ", "")
    clean_name = librarian_created_concerts_clean_text(concert_name).lower().replace(" ", "-")
    clean_date = librarian_created_concerts_clean_text(concert_date).replace("/", "-")
    stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return f"{clean_email}__{clean_name}__{clean_date}__{stamp}"


def concert_identity_lock_normalize_tier(tier: int) -> int:
    clean_tier = int(tier or 1)
    if clean_tier < 1:
        clean_tier = 1
    if clean_tier > 10:
        clean_tier = 10
    return clean_tier


def concert_identity_lock_life_window_days(tier: int) -> int:
    clean_tier = concert_identity_lock_normalize_tier(tier)

    if clean_tier <= 3:
        return 30

    if clean_tier <= 6:
        return 60

    if clean_tier <= 8:
        return 90

    return 120


def concert_identity_lock_parse_anchor_datetime(
    concert_date: str,
    created_at: str,
) -> datetime:
    raw_concert_date = str(concert_date or "").strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw_concert_date, fmt)
        except ValueError:
            pass

    raw_created_at = str(created_at or "").strip()
    if raw_created_at:
        try:
            return datetime.fromisoformat(raw_created_at.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            pass

    return datetime.utcnow()


def concert_identity_lock_build_lifecycle(
    concert_date: str,
    created_at: str,
    concert_tier: int,
) -> dict:
    anchor_dt = concert_identity_lock_parse_anchor_datetime(concert_date, created_at)
    life_window_days = concert_identity_lock_life_window_days(concert_tier)
    expires_dt = anchor_dt + timedelta(days=life_window_days)
    now_dt = datetime.utcnow()

    return {
        "anchor_at": anchor_dt.isoformat(),
        "expires_at": expires_dt.isoformat(),
        "life_window_days": life_window_days,
        "life_window_state": "live" if now_dt <= expires_dt else "expired",
    }


def concert_identity_lock_high_revision_surcharge(tier: int) -> int:
    clean_tier = concert_identity_lock_normalize_tier(tier)

    if clean_tier <= 3:
        return 10

    if clean_tier <= 6:
        return 15

    if clean_tier <= 8:
        return 20

    return 25


def concert_identity_lock_total_send_count(
    cur: sqlite3.Cursor,
    concert_name: str,
    concert_ref: str,
) -> int:
    cur.execute(
        """
        SELECT
            id,
            COALESCE(current_file_token, '') AS current_file_token,
            COALESCE(current_file_sent_at, '') AS current_file_sent_at
        FROM musician_concert_receipts
        WHERE lower(trim(COALESCE(concert_name, '')))=lower(?)
          AND lower(trim(COALESCE(concert_ref, '')))=lower(?)
          AND COALESCE(is_active, 0)=1
          AND (
                trim(COALESCE(current_file_token, '')) != ''
             OR trim(COALESCE(current_file_sent_at, '')) != ''
          )
        ORDER BY
            COALESCE(current_file_sent_at, '') ASC,
            id ASC
        """,
        (concert_name, concert_ref),
    )
    rows = cur.fetchall()

    cycle_keys: list[str] = []
    seen_cycle_keys: set[str] = set()

    for row in rows:
        current_file_token = str(row["current_file_token"] or "").strip()
        current_file_sent_at = str(row["current_file_sent_at"] or "").strip()

        if current_file_token:
            cycle_key = f"token::{current_file_token}"
        elif current_file_sent_at:
            cycle_key = f"sent_at::{current_file_sent_at}"
        else:
            continue

        if cycle_key in seen_cycle_keys:
            continue

        seen_cycle_keys.add(cycle_key)
        cycle_keys.append(cycle_key)

    return len(cycle_keys)


def concert_identity_lock_pricing_summary(
    concert_tier: int,
    total_send_count: int,
) -> dict:
    clean_tier = concert_identity_lock_normalize_tier(concert_tier)
    pricing = concert_control_tier_pricing(clean_tier)
    repeat_surcharge = concert_identity_lock_high_revision_surcharge(clean_tier)

    first_send_count = 1 if int(total_send_count or 0) > 0 else 0
    revision_send_count = max(int(total_send_count or 0) - first_send_count, 0)
    billing_total_usd = float(pricing["first"]) if first_send_count else 0.0
    high_revision_active = False

    for revision_number in range(1, revision_send_count + 1):
        line_total = float(pricing["repeat"])
        if revision_number >= 3:
            line_total += float(repeat_surcharge)
            high_revision_active = True
        billing_total_usd += line_total

    return {
        "first_send_count": first_send_count,
        "revision_send_count": revision_send_count,
        "total_send_count": int(total_send_count or 0),
        "billing_total_usd": round(billing_total_usd, 2),
        "high_revision_active": high_revision_active,
    }


def concert_identity_lock_refresh(
    concert_name: str,
    concert_ref: str,
) -> dict:
    clean_name = librarian_created_concerts_clean_text(concert_name)
    clean_ref = librarian_created_concerts_clean_text(concert_ref)

    if not clean_name and not clean_ref:
        return {}

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        if clean_name and clean_ref:
            cur.execute(
                """
                SELECT
                    id,
                    COALESCE(librarian_email, '') AS librarian_email,
                    COALESCE(librarian_country, '') AS librarian_country,
                    COALESCE(concert_ref, '') AS concert_ref,
                    COALESCE(concert_name, '') AS concert_name,
                    COALESCE(concert_date, '') AS concert_date,
                    COALESCE(ensemble_name, '') AS ensemble_name,
                    COALESCE(venue_name, '') AS venue_name,
                    COALESCE(conductor_name, '') AS conductor_name,
                    COALESCE(concert_tier, 1) AS concert_tier,
                    COALESCE(status, 'active') AS status,
                    COALESCE(identity_locked_at, '') AS identity_locked_at,
                    COALESCE(high_revision_triggered_at, '') AS high_revision_triggered_at,
                    COALESCE(created_at, '') AS created_at,
                    COALESCE(updated_at, '') AS updated_at
                FROM librarian_created_concerts
                WHERE lower(trim(COALESCE(concert_name, '')))=lower(?)
                  AND lower(trim(COALESCE(concert_ref, '')))=lower(?)
                ORDER BY id DESC
                LIMIT 1
                """,
                (clean_name, clean_ref),
            )
        elif clean_ref:
            cur.execute(
                """
                SELECT
                    id,
                    COALESCE(librarian_email, '') AS librarian_email,
                    COALESCE(librarian_country, '') AS librarian_country,
                    COALESCE(concert_ref, '') AS concert_ref,
                    COALESCE(concert_name, '') AS concert_name,
                    COALESCE(concert_date, '') AS concert_date,
                    COALESCE(ensemble_name, '') AS ensemble_name,
                    COALESCE(venue_name, '') AS venue_name,
                    COALESCE(conductor_name, '') AS conductor_name,
                    COALESCE(concert_tier, 1) AS concert_tier,
                    COALESCE(status, 'active') AS status,
                    COALESCE(identity_locked_at, '') AS identity_locked_at,
                    COALESCE(high_revision_triggered_at, '') AS high_revision_triggered_at,
                    COALESCE(created_at, '') AS created_at,
                    COALESCE(updated_at, '') AS updated_at
                FROM librarian_created_concerts
                WHERE lower(trim(COALESCE(concert_ref, '')))=lower(?)
                ORDER BY id DESC
                LIMIT 1
                """,
                (clean_ref,),
            )
        else:
            cur.execute(
                """
                SELECT
                    id,
                    COALESCE(librarian_email, '') AS librarian_email,
                    COALESCE(librarian_country, '') AS librarian_country,
                    COALESCE(concert_ref, '') AS concert_ref,
                    COALESCE(concert_name, '') AS concert_name,
                    COALESCE(concert_date, '') AS concert_date,
                    COALESCE(ensemble_name, '') AS ensemble_name,
                    COALESCE(venue_name, '') AS venue_name,
                    COALESCE(conductor_name, '') AS conductor_name,
                    COALESCE(concert_tier, 1) AS concert_tier,
                    COALESCE(status, 'active') AS status,
                    COALESCE(identity_locked_at, '') AS identity_locked_at,
                    COALESCE(high_revision_triggered_at, '') AS high_revision_triggered_at,
                    COALESCE(created_at, '') AS created_at,
                    COALESCE(updated_at, '') AS updated_at
                FROM librarian_created_concerts
                WHERE lower(trim(COALESCE(concert_name, '')))=lower(?)
                ORDER BY id DESC
                LIMIT 1
                """,
                (clean_name,),
            )

        row = cur.fetchone()

        if not row:
            return {}

        now = datetime.utcnow().isoformat()
        lifecycle = concert_identity_lock_build_lifecycle(
            row["concert_date"],
            row["created_at"],
            int(row["concert_tier"] or 1),
        )
        total_send_count = concert_identity_lock_total_send_count(
            cur,
            str(row["concert_name"] or "").strip(),
            str(row["concert_ref"] or "").strip(),
        )
        pricing_summary = concert_identity_lock_pricing_summary(
            int(row["concert_tier"] or 1),
            total_send_count,
        )

        identity_locked_at = str(row["identity_locked_at"] or "").strip() or str(row["created_at"] or "").strip() or now
        high_revision_triggered_at = str(row["high_revision_triggered_at"] or "").strip()
        if pricing_summary["high_revision_active"]:
            if not high_revision_triggered_at:
                high_revision_triggered_at = now
        else:
            high_revision_triggered_at = ""

        cur.execute(
            """
            UPDATE librarian_created_concerts
            SET identity_locked_at=?,
                operational_anchor_at=?,
                operational_expires_at=?,
                life_window_days=?,
                life_window_state=?,
                total_send_count=?,
                revision_send_count=?,
                billing_total_usd=?,
                high_revision_triggered_at=?,
                updated_at=?
            WHERE id=?
            """,
            (
                identity_locked_at,
                lifecycle["anchor_at"],
                lifecycle["expires_at"],
                lifecycle["life_window_days"],
                lifecycle["life_window_state"],
                pricing_summary["total_send_count"],
                pricing_summary["revision_send_count"],
                pricing_summary["billing_total_usd"],
                high_revision_triggered_at or None,
                now,
                int(row["id"]),
            ),
        )
        conn.commit()

        return {
            "concert_ref": str(row["concert_ref"] or "").strip(),
            "concert_name": str(row["concert_name"] or "").strip(),
            "concert_tier": int(row["concert_tier"] or 1),
            "concert_status": str(row["status"] or "active").strip() or "active",
            "identity_locked_at": identity_locked_at,
            "operational_anchor_at": lifecycle["anchor_at"],
            "operational_expires_at": lifecycle["expires_at"],
            "life_window_days": lifecycle["life_window_days"],
            "life_window_state": lifecycle["life_window_state"],
            "total_send_count": pricing_summary["total_send_count"],
            "revision_send_count": pricing_summary["revision_send_count"],
            "billing_total_usd": pricing_summary["billing_total_usd"],
            "high_revision_active": pricing_summary["high_revision_active"],
            "high_revision_triggered_at": high_revision_triggered_at,
        }
    finally:
        if conn is not None:
            conn.close()


def librarian_created_concerts_store(
    librarian_email: str,
    concert_name: str,
    concert_date: str,
    ensemble_name: str,
    venue_name: str,
    conductor_name: str,
    concert_tier: int,
) -> str:
    clean_email = librarian_created_concerts_clean_text(librarian_email).lower()
    clean_name = librarian_created_concerts_clean_text(concert_name)
    clean_date = librarian_created_concerts_clean_text(concert_date)
    clean_ensemble = librarian_created_concerts_clean_text(ensemble_name)
    clean_venue = librarian_created_concerts_clean_text(venue_name)
    clean_conductor = librarian_created_concerts_clean_text(conductor_name)
    clean_country = librarian_country_code_by_email(clean_email)
    clean_tier = concert_identity_lock_normalize_tier(concert_tier)
    concert_ref = librarian_created_concerts_make_ref(clean_email, clean_name, clean_date)
    now = datetime.utcnow().isoformat()
    lifecycle = concert_identity_lock_build_lifecycle(clean_date, now, clean_tier)

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO librarian_created_concerts (
            librarian_email,
            librarian_country,
            concert_ref,
            concert_name,
            concert_date,
            ensemble_name,
            venue_name,
            conductor_name,
            concert_tier,
            status,
            identity_locked_at,
            operational_anchor_at,
            operational_expires_at,
            life_window_days,
            life_window_state,
            total_send_count,
            revision_send_count,
            billing_total_usd,
            high_revision_triggered_at,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, 0, 0, 0, NULL, ?, ?)
        """, (
            clean_email,
            clean_country,
            concert_ref,
            clean_name,
            clean_date,
            clean_ensemble,
            clean_venue,
            clean_conductor,
            clean_tier,
            now,
            lifecycle["anchor_at"],
            lifecycle["expires_at"],
            lifecycle["life_window_days"],
            lifecycle["life_window_state"],
            now,
            now,
        ))

        if clean_country and clean_ensemble:
            ensemble_key = " ".join(clean_ensemble.strip().lower().split())
            cur.execute("""
            INSERT OR IGNORE INTO global_country_ensemble_registry (
                country_code,
                ensemble_name,
                ensemble_key,
                added_by_email,
                is_system_seeded,
                is_active,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, 0, 1, ?, ?)
            """, (
                clean_country,
                clean_ensemble,
                ensemble_key,
                clean_email,
                now,
                now,
            ))

        conn.commit()
        return concert_ref
    finally:
        if conn is not None:
            conn.close()


@app.post("/librarian/production/create")
def librarian_created_concerts_create(
    request: Request,
    ensemble_select: str = Form(""),
    ensemble_new: str = Form(""),
    concert_date: str = Form(""),
    venue_select: str = Form(""),
    venue_new: str = Form(""),
    conductor_select: str = Form(""),
    conductor_new: str = Form(""),
    name: str = Form(""),
    concert_tier: int = Form(...),
):
    email = librarian_route_get_email_from_request(request) or "librarian@local"

    ensemble_name = librarian_created_concerts_clean_text(
        ensemble_new if str(ensemble_select or "").strip() == "__new__" else ensemble_select
    )
    venue_name = librarian_created_concerts_clean_text(
        venue_new if str(venue_select or "").strip() == "__new__" else venue_select
    )
    conductor_name = librarian_created_concerts_clean_text(
        conductor_new if str(conductor_select or "").strip() == "__new__" else conductor_select
    )
    concert_name = librarian_created_concerts_clean_text(name)
    clean_date = librarian_created_concerts_clean_text(concert_date)

    if not ensemble_name or not venue_name or not conductor_name or not concert_name or not clean_date:
        return RedirectResponse(f"/librarian?email={quote_plus(email)}", status_code=303)

    librarian_created_concerts_store(
        librarian_email=email,
        concert_name=concert_name,
        concert_date=clean_date,
        ensemble_name=ensemble_name,
        venue_name=venue_name,
        conductor_name=conductor_name,
        concert_tier=int(concert_tier),
    )

    return RedirectResponse(f"/librarian?email={quote_plus(email)}", status_code=303)


# =====================================================================
# LIBRARIAN HOME PAGE — NEW BUILD
# PURPOSE: CLEAN BACKEND FOR EXISTING librarian.html LAYOUT ONLY
# =====================================================================

@app.get("/", response_class=HTMLResponse)
def home_page(request: Request):
    librarian_file = TEMPLATES_DIR / "librarian.html"
    if not librarian_file.exists():
        return HTMLResponse(
            """
            <!doctype html>
            <html lang="en">
            <head>
                <meta charset="utf-8">
                <title>Annotatio — Librarian Dashboard</title>
                <meta name="viewport" content="width=device-width, initial-scale=1">
            </head>
            <body style="margin:0; background:#071018; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif;">
                <div style="max-width:1100px; margin:32px auto; padding:0 18px;">
                    <div style="background:#0b1622; border:1px solid #223548; border-radius:16px; padding:28px;">
                        <div style="font-size:34px; color:#74d3de; margin-bottom:10px;">Librarian Dashboard</div>
                        <div style="font-size:17px; color:#c8d0d8;">templates/librarian.html not found.</div>
                    </div>
                </div>
            </body>
            </html>
            """
        )

    librarian_email = "librarian@local"
    venue_names = get_librarian_dashboard_venues(librarian_email)
    ensemble_names = librarian_dashboard_ensemble_names(librarian_email)
    conductor_names = librarian_dashboard_conductor_names(librarian_email)
    traffic_snapshot = librarian_dashboard_concert_summary_fetch_snapshot()
    popup_payload = librarian_conductor_invite_popup_payload(request)

    return templates.TemplateResponse(
        "librarian.html",
        {
            "request": request,
            "user": {"email": librarian_email},
            "selected_view": "",
            "librarian_broadcast": "",
            "logo_exists": (STATIC_DIR / "annotatio_logo.png").exists(),
            "concerts": traffic_snapshot["concerts"],
            "fully_green_count": traffic_snapshot["fully_green_count"],
            "red_sections_count": traffic_snapshot["red_sections_count"],
            "connected_now_count": librarian_dashboard_connected_now_count(librarian_email),
            "notify_logs": [],
            "lights_by_concert": traffic_snapshot["lights_by_concert"],
            "ensembles": ensemble_names,
            "venues": venue_names,
             "conductors": conductor_names,
            "conductor_alerts": concert_control_conductor_alerts_list(limit=12),
            "conductor_alerts_latest": concert_control_conductor_alerts_latest(),
            "conductor_alerts_pending_review_count": concert_control_conductor_alerts_pending_review_count(),
            "conductor_alerts_forwarded_count": concert_control_conductor_alerts_forwarded_count(),
            "conductor_invite_popup_active": popup_payload["conductor_invite_popup_active"],
            "conductor_invite_popup_link": popup_payload["conductor_invite_popup_link"],
        },
    )

@app.get("/librarian/invite", response_class=HTMLResponse)
def librarian_invite_page(request: Request):
    template_file = TEMPLATES_DIR / "librarian_invitation.html"
    if not template_file.exists():
        return HTMLResponse("templates/librarian_invitation.html not found.", status_code=404)

    email = (request.query_params.get("email") or "").strip().lower()
    if not email:
        return HTMLResponse("Librarian email missing.", status_code=400)

    base_url = str(request.base_url).rstrip("/")
    invite_url = f"{base_url}/librarian_shortcut?email={quote_plus(email)}"
    static_base_url = f"{base_url}/static"

    return templates.TemplateResponse(
        "librarian_invitation.html",
        {
            "request": request,
            "user": {"email": email},
            "invite_url": invite_url,
            "static_base_url": static_base_url,
        },
    )


@app.get("/librarian_shortcut")
def librarian_shortcut(request: Request):
    email = (request.query_params.get("email") or "").strip().lower()
    if not email:
        return RedirectResponse("/librarian_setup", status_code=303)

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT id
        FROM users
        WHERE lower(email)=lower(?)
          AND role='librarian'
        LIMIT 1
        """, (email,))
        if cur.fetchone():
            return RedirectResponse(f"/librarian?email={quote_plus(email)}", status_code=303)
        return RedirectResponse(f"/librarian_setup?email={quote_plus(email)}", status_code=303)
    finally:
        if conn is not None:
            conn.close()


@app.get("/conductor_shortcut")
def conductor_shortcut(request: Request):
    email = (request.query_params.get("email") or "").strip().lower()
    if not email:
        return RedirectResponse("/conductor_setup", status_code=303)

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT id
        FROM users
        WHERE lower(email)=lower(?)
          AND role='conductor'
        LIMIT 1
        """, (email,))
        if cur.fetchone():
            return RedirectResponse(f"/conductor?email={quote_plus(email)}", status_code=303)
        return RedirectResponse(f"/conductor_setup?email={quote_plus(email)}", status_code=303)
    finally:
        if conn is not None:
            conn.close()


@app.get("/librarian_setup", response_class=HTMLResponse)
def librarian_setup_page(request: Request):
    invited_email = str(request.query_params.get("email") or "").strip().lower()
    error = str(request.query_params.get("error") or "").strip().lower()

    error_map = {
        "missing_email": "Email address is required.",
        "email_mismatch": "The confirmed email must match the invitation email.",
        "missing_name": "Full name is required.",
        "missing_organisation": "Organisation name is required.",
    }
    error_html = ""
    if error in error_map:
        error_html = f"""
        <div style="margin-bottom:18px; padding:12px 16px; border:1px solid #b15a5a; background:#2a1111; border-radius:10px; color:#f2d6d6;">
            {error_map[error]}
        </div>
        """

    page_html = f"""
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Annotatio — Librarian Setup</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
    </head>
    <body style="margin:0; background:#071018; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif;">
        <div style="max-width:920px; margin:26px auto; padding:0 18px;">
            <div style="background:#0b1622; border:1px solid #223548; border-radius:16px; overflow:hidden; box-shadow:0 10px 30px rgba(0,0,0,0.28);">
                <div style="text-align:center; padding:18px 20px 8px 20px;">
                    <img src="/static/annotatio_logo.png" alt="Annotatio" style="width:460px; max-width:85%; height:auto;">
                    <div style="height:1px; width:180px; background:#b89457; margin:10px auto 0 auto;"></div>
                </div>

                <div style="padding:22px 28px 30px 28px;">
                    <div style="font-size:30px; color:#74d3de; margin-bottom:10px;">Librarian Setup</div>
                    <div style="font-size:17px; line-height:1.7; color:#c8d0d8; margin-bottom:18px;">
                        Complete your details once. Your librarian record is created only when you save this setup.
                    </div>

                    {error_html}

                    <form method="post" action="/librarian_setup?email={quote_plus(invited_email)}">
                        <div style="display:grid; grid-template-columns:1fr 1fr; gap:14px 16px;">
                            <div>
                                <label for="full_name" style="display:block; margin-bottom:8px; color:#e5dccb;">Full name</label>
                                <input id="full_name" name="full_name" type="text" required style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="preferred_name" style="display:block; margin-bottom:8px; color:#e5dccb;">Preferred name</label>
                                <input id="preferred_name" name="preferred_name" type="text" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="confirm_email" style="display:block; margin-bottom:8px; color:#e5dccb;">Confirm email address</label>
                                <input id="confirm_email" name="confirm_email" type="email" value="{invited_email}" required style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="mobile" style="display:block; margin-bottom:8px; color:#e5dccb;">Mobile</label>
                                <input id="mobile" name="mobile" type="text" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div style="grid-column:1 / -1;">
                                <label for="organisation_name" style="display:block; margin-bottom:8px; color:#e5dccb;">Organisation name</label>
                                <input id="organisation_name" name="organisation_name" type="text" required style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="role_title" style="display:block; margin-bottom:8px; color:#e5dccb;">Role title</label>
                                <input id="role_title" name="role_title" type="text" value="Librarian" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="work_email" style="display:block; margin-bottom:8px; color:#e5dccb;">Work email</label>
                                <input id="work_email" name="work_email" type="email" value="{invited_email}" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="city" style="display:block; margin-bottom:8px; color:#e5dccb;">City</label>
                                <input id="city" name="city" type="text" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="state_region_territory" style="display:block; margin-bottom:8px; color:#e5dccb;">State / Region / Territory</label>
                                <input id="state_region_territory" name="state_region_territory" type="text" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div style="grid-column:1 / -1;">
                                <label for="country_name" style="display:block; margin-bottom:8px; color:#e5dccb;">Country</label>
                                <input id="country_name" name="country_name" type="text" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>
                        </div>

                        <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:22px;">
                            <button type="submit" style="display:inline-block; padding:12px 22px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; text-decoration:none; font-size:17px; font-family:Georgia, 'Times New Roman', serif; cursor:pointer;">Save Librarian Setup</button>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(page_html)


@app.post("/librarian_setup")
def librarian_setup_submit(
    request: Request,
    full_name: str = Form(...),
    preferred_name: str = Form(""),
    confirm_email: str = Form(...),
    mobile: str = Form(""),
    organisation_name: str = Form(...),
    role_title: str = Form("Librarian"),
    work_email: str = Form(""),
    city: str = Form(""),
    state_region_territory: str = Form(""),
    country_name: str = Form(""),
):
    invited_email = " ".join(str(request.query_params.get("email") or "").strip().split()).lower()
    submitted_email = " ".join(str(confirm_email or "").strip().split()).lower()
    clean_full_name = " ".join(str(full_name or "").strip().split())
    clean_preferred_name = " ".join(str(preferred_name or "").strip().split())
    clean_mobile = " ".join(str(mobile or "").strip().split())
    clean_organisation_name = " ".join(str(organisation_name or "").strip().split())
    clean_role_title = " ".join(str(role_title or "").strip().split()) or "Librarian"
    clean_work_email = " ".join(str(work_email or "").strip().split()) or submitted_email
    clean_city = " ".join(str(city or "").strip().split())
    clean_state = " ".join(str(state_region_territory or "").strip().split())
    clean_country_name = " ".join(str(country_name or "").strip().split())
    now = datetime.utcnow().isoformat()

    if not submitted_email:
        return RedirectResponse(
            f"/librarian_setup?email={quote_plus(invited_email)}&error=missing_email",
            status_code=303,
        )

    if invited_email and invited_email != submitted_email:
        return RedirectResponse(
            f"/librarian_setup?email={quote_plus(invited_email)}&error=email_mismatch",
            status_code=303,
        )

    if not clean_full_name:
        return RedirectResponse(
            f"/librarian_setup?email={quote_plus(submitted_email)}&error=missing_name",
            status_code=303,
        )

    if not clean_organisation_name:
        return RedirectResponse(
            f"/librarian_setup?email={quote_plus(submitted_email)}&error=missing_organisation",
            status_code=303,
        )

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        cur.execute("""
        SELECT id
        FROM organisations
        WHERE lower(name)=lower(?)
        LIMIT 1
        """, (clean_organisation_name,))
        organisation_row = cur.fetchone()

        if organisation_row:
            organisation_id = int(organisation_row["id"])
            cur.execute("""
            UPDATE organisations
            SET country=?, city=?
            WHERE id=?
            """, (
                clean_country_name,
                clean_city,
                organisation_id,
            ))
        else:
            cur.execute("""
            INSERT INTO organisations (
                name,
                country,
                city
            ) VALUES (?, ?, ?)
            """, (
                clean_organisation_name,
                clean_country_name,
                clean_city,
            ))
            organisation_id = int(cur.lastrowid)

        cur.execute("""
        SELECT id
        FROM users
        WHERE lower(email)=lower(?)
          AND role='librarian'
        LIMIT 1
        """, (submitted_email,))
        user_row = cur.fetchone()

        if user_row:
            user_id = int(user_row["id"])
            cur.execute("""
            UPDATE users
            SET
                name=?,
                organisation_id=?
            WHERE id=?
            """, (
                clean_preferred_name or clean_full_name or submitted_email,
                organisation_id,
                user_id,
            ))
        else:
            cur.execute("""
            INSERT INTO users (
                name,
                email,
                role,
                organisation_id
            ) VALUES (?, ?, 'librarian', ?)
            """, (
                clean_preferred_name or clean_full_name or submitted_email,
                submitted_email,
                organisation_id,
            ))
            user_id = int(cur.lastrowid)

        cur.execute("""
        INSERT INTO librarian_profiles (
            user_id,
            organisation_id,
            email,
            full_name,
            preferred_name,
            mobile,
            notes,
            role_title,
            organisation_name,
            ensemble_name,
            city,
            state_region_territory,
            country_code,
            country_name,
            work_email,
            global_search_visible,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, '', ?, ?, '', ?, ?, '', ?, ?, 'Yes', ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            organisation_id=excluded.organisation_id,
            email=excluded.email,
            full_name=excluded.full_name,
            preferred_name=excluded.preferred_name,
            mobile=excluded.mobile,
            role_title=excluded.role_title,
            organisation_name=excluded.organisation_name,
            city=excluded.city,
            state_region_territory=excluded.state_region_territory,
            country_name=excluded.country_name,
            work_email=excluded.work_email,
            updated_at=excluded.updated_at
        """, (
            user_id,
            organisation_id,
            submitted_email,
            clean_full_name,
            clean_preferred_name,
            clean_mobile,
            clean_role_title,
            clean_organisation_name,
            clean_city,
            clean_state,
            clean_country_name,
            clean_work_email,
            now,
            now,
        ))

        conn.commit()
    finally:
        if conn is not None:
            conn.close()

    return RedirectResponse(f"/librarian?email={quote_plus(submitted_email)}", status_code=303)


@app.get("/conductor_setup", response_class=HTMLResponse)
def conductor_setup_page(request: Request):
    invited_email = str(request.query_params.get("email") or "").strip().lower()
    invite_token = str(request.query_params.get("invite_token") or "").strip()
    error = str(request.query_params.get("error") or "").strip().lower()

    error_map = {
        "missing_email": "Email address is required.",
        "email_mismatch": "The confirmed email must match the invitation email.",
        "missing_name": "Full name is required.",
        "invalid_invite": "This invite is not valid for conductor setup.",
    }
    error_html = ""
    if error in error_map:
        error_html = f"""
        <div style="margin-bottom:18px; padding:12px 16px; border:1px solid #b15a5a; background:#2a1111; border-radius:10px; color:#f2d6d6;">
            {error_map[error]}
        </div>
        """

    page_html = f"""
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Annotatio — Conductor Setup</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
    </head>
    <body style="margin:0; background:#071018; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif;">
        <div style="max-width:920px; margin:26px auto; padding:0 18px;">
            <div style="background:#0b1622; border:1px solid #223548; border-radius:16px; overflow:hidden; box-shadow:0 10px 30px rgba(0,0,0,0.28);">
                <div style="text-align:center; padding:18px 20px 8px 20px;">
                    <img src="/static/annotatio_logo.png" alt="Annotatio" style="width:460px; max-width:85%; height:auto;">
                    <div style="height:1px; width:180px; background:#b89457; margin:10px auto 0 auto;"></div>
                </div>

                <div style="padding:22px 28px 30px 28px;">
                    <div style="font-size:30px; color:#74d3de; margin-bottom:10px;">Conductor Setup</div>
                    <div style="font-size:17px; line-height:1.7; color:#c8d0d8; margin-bottom:18px;">
                        Complete your details once. Your conductor record is created only when you save this setup.
                    </div>

                    {error_html}

                    <form method="post" action="/conductor_setup?email={quote_plus(invited_email)}{f'&invite_token={quote_plus(invite_token)}' if invite_token else ''}">
                        <div style="display:grid; grid-template-columns:1fr 1fr; gap:14px 16px;">
                            <div>
                                <label for="full_name" style="display:block; margin-bottom:8px; color:#e5dccb;">Full name</label>
                                <input id="full_name" name="full_name" type="text" required style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="preferred_name" style="display:block; margin-bottom:8px; color:#e5dccb;">Preferred name</label>
                                <input id="preferred_name" name="preferred_name" type="text" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="confirm_email" style="display:block; margin-bottom:8px; color:#e5dccb;">Confirm email address</label>
                                <input id="confirm_email" name="confirm_email" type="email" value="{invited_email}" required style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="mobile" style="display:block; margin-bottom:8px; color:#e5dccb;">Mobile</label>
                                <input id="mobile" name="mobile" type="text" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="work_email" style="display:block; margin-bottom:8px; color:#e5dccb;">Work email</label>
                                <input id="work_email" name="work_email" type="email" value="{invited_email}" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="working_area" style="display:block; margin-bottom:8px; color:#e5dccb;">Primary working area</label>
                                <input id="working_area" name="working_area" type="text" value="Conductor" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="city" style="display:block; margin-bottom:8px; color:#e5dccb;">City</label>
                                <input id="city" name="city" type="text" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div>
                                <label for="state_region_territory" style="display:block; margin-bottom:8px; color:#e5dccb;">State / Region / Territory</label>
                                <input id="state_region_territory" name="state_region_territory" type="text" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>

                            <div style="grid-column:1 / -1;">
                                <label for="country_name" style="display:block; margin-bottom:8px; color:#e5dccb;">Country</label>
                                <input id="country_name" name="country_name" type="text" style="width:100%; box-sizing:border-box; padding:12px 14px; border:1px solid #31455c; border-radius:10px; background:#f6f3ec; color:#1d2430; font-family:Georgia, 'Times New Roman', serif; font-size:16px;">
                            </div>
                        </div>

                        <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:22px;">
                            <button type="submit" style="display:inline-block; padding:12px 22px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; text-decoration:none; font-size:17px; font-family:Georgia, 'Times New Roman', serif; cursor:pointer;">Save Conductor Setup</button>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(page_html)


@app.post("/conductor_setup")
def conductor_setup_submit(
    request: Request,
    full_name: str = Form(...),
    preferred_name: str = Form(""),
    confirm_email: str = Form(...),
    mobile: str = Form(""),
    work_email: str = Form(""),
    working_area: str = Form("Conductor"),
    city: str = Form(""),
    state_region_territory: str = Form(""),
    country_name: str = Form(""),
):
    invited_email = " ".join(str(request.query_params.get("email") or "").strip().split()).lower()
    invite_token = " ".join(str(request.query_params.get("invite_token") or "").strip().split())
    submitted_email = " ".join(str(confirm_email or "").strip().split()).lower()
    clean_full_name = " ".join(str(full_name or "").strip().split())
    clean_preferred_name = " ".join(str(preferred_name or "").strip().split())
    clean_mobile = " ".join(str(mobile or "").strip().split())
    clean_work_email = " ".join(str(work_email or "").strip().split()) or submitted_email
    clean_working_area = " ".join(str(working_area or "").strip().split()) or "Conductor"
    clean_city = " ".join(str(city or "").strip().split())
    clean_state = " ".join(str(state_region_territory or "").strip().split())
    clean_country_name = " ".join(str(country_name or "").strip().split())
    now = datetime.utcnow().isoformat()
    
    if not submitted_email:
        return RedirectResponse(
            f"/conductor_setup?email={quote_plus(invited_email)}&error=missing_email",
            status_code=303,
        )

    if invited_email and invited_email != submitted_email:
        return RedirectResponse(
            f"/conductor_setup?email={quote_plus(invited_email)}&error=email_mismatch",
            status_code=303,
        )

    if not clean_full_name:
        return RedirectResponse(
            f"/conductor_setup?email={quote_plus(submitted_email)}{f'&invite_token={quote_plus(invite_token)}' if invite_token else ''}&error=missing_name",
            status_code=303,
        )

    if invite_token:
        invite_row = conductor_entry_invite_get_by_token(invite_token)
        if not invite_row:
            return RedirectResponse(
                f"/conductor_setup?email={quote_plus(invited_email or submitted_email)}&error=invalid_invite",
                status_code=303,
            )

        if str(invite_row["status"] or "").strip().lower() != "active":
            return RedirectResponse(
                f"/conductor_setup?email={quote_plus(invited_email or submitted_email)}&error=invalid_invite",
                status_code=303,
            )

        if str(invite_row["conductor_email"] or "").strip().lower() != submitted_email:
            return RedirectResponse(
                f"/conductor_setup?email={quote_plus(str(invite_row['conductor_email'] or '').strip().lower())}&error=email_mismatch",
                status_code=303,
            )

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        cur.execute("""
        SELECT id
        FROM users
        WHERE lower(email)=lower(?)
          AND role='conductor'
        LIMIT 1
        """, (submitted_email,))
        user_row = cur.fetchone()

        if user_row:
            user_id = int(user_row["id"])
            cur.execute("""
            UPDATE users
            SET name=?
            WHERE id=?
            """, (
                clean_preferred_name or clean_full_name or submitted_email,
                user_id,
            ))
        else:
            cur.execute("""
            INSERT INTO users (
                name,
                email,
                role,
                organisation_id
            ) VALUES (?, ?, 'conductor', NULL)
            """, (
                clean_preferred_name or clean_full_name or submitted_email,
                submitted_email,
            ))
            user_id = int(cur.lastrowid)

        cur.execute("""
        INSERT INTO conductor_profiles (
            user_id,
            email,
            full_name,
            preferred_name,
            mobile,
            notes,
            working_area,
            career_stage,
            production_types,
            known_for,
            city,
            state_region_territory,
            country_code,
            country_name,
            work_email,
            global_search_visible,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, '', ?, '', '', '', ?, ?, '', ?, ?, 'Yes', ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            email=excluded.email,
            full_name=excluded.full_name,
            preferred_name=excluded.preferred_name,
            mobile=excluded.mobile,
            working_area=excluded.working_area,
            city=excluded.city,
            state_region_territory=excluded.state_region_territory,
            country_name=excluded.country_name,
            work_email=excluded.work_email,
            updated_at=excluded.updated_at
        """, (
            user_id,
            submitted_email,
            clean_full_name,
            clean_preferred_name,
            clean_mobile,
            clean_working_area,
            clean_city,
            clean_state,
            clean_country_name,
            clean_work_email,
            now,
            now,
        ))

        if invite_token:
            cur.execute("""
            UPDATE conductor_entry_invites
            SET status='used',
                used_at=?
            WHERE invite_token=?
            """, (
                now,
                invite_token,
            ))

        conn.commit()
    finally:
        if conn is not None:
            conn.close()

    return RedirectResponse(f"/conductor?email={quote_plus(submitted_email)}", status_code=303)


# =====================================================================
# LOGOUT — STANDALONE BLOCK
# PURPOSE: LOGOUT REDIRECT ONLY
# =====================================================================

@app.get("/logout")
def logout_route():
    return RedirectResponse("/", status_code=303)


def annotatio_invite_host_base_url(request: Request) -> str:
    configured_host = str(APP_DIR.joinpath("invite_host.txt").read_text(encoding="utf-8")).strip() if APP_DIR.joinpath("invite_host.txt").exists() else ""
    query_host = str(request.query_params.get("host") or "").strip()
    forwarded_host = str(request.headers.get("x-forwarded-host") or "").strip()
    forwarded_proto = str(request.headers.get("x-forwarded-proto") or "").strip()
    request_base = str(request.base_url).rstrip("/")

    if query_host:
        return query_host.rstrip("/")

    if configured_host:
        return configured_host.rstrip("/")

    if forwarded_host:
        scheme = forwarded_proto or urlparse(request_base).scheme or "http"
        return f"{scheme}://{forwarded_host}".rstrip("/")

    return request_base.rstrip("/")


@app.get("/conductor/invite", response_class=HTMLResponse)
def conductor_invite_page(request: Request):
    template_file = TEMPLATES_DIR / "conductor_invitation.html"
    if not template_file.exists():
        return HTMLResponse("templates/conductor_invitation.html not found.", status_code=404)

    email = (request.query_params.get("email") or "").strip().lower()
    if not email:
        return HTMLResponse("Conductor email missing.", status_code=400)

    base_url = str(request.base_url).rstrip("/")
    invite_url = f"{base_url}/conductor_shortcut?email={quote_plus(email)}"
    static_base_url = f"{base_url}/static"

    return templates.TemplateResponse(
        "conductor_invitation.html",
        {
            "request": request,
            "user": {"email": email},
            "conductor_email": email,
            "invite_url": invite_url,
            "static_base_url": static_base_url,
        },
    )


def conductor_entry_invite_create(
    librarian_email: str,
    conductor_email: str,
) -> str:
    clean_librarian_email = str(librarian_email or "").strip().lower()
    clean_conductor_email = str(conductor_email or "").strip().lower()
    invite_token = secrets.token_urlsafe(24)
    now = datetime.utcnow().isoformat()

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO conductor_entry_invites (
            librarian_email,
            conductor_email,
            invite_token,
            status,
            created_at,
            used_at
        ) VALUES (?, ?, ?, 'active', ?, NULL)
        """, (
            clean_librarian_email,
            clean_conductor_email,
            invite_token,
            now,
        ))
        conn.commit()
        return invite_token
    finally:
        if conn is not None:
            conn.close()


def conductor_entry_invite_get_by_token(invite_token: str):
    clean_token = str(invite_token or "").strip()
    if not clean_token:
        return None

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT *
        FROM conductor_entry_invites
        WHERE invite_token=?
        LIMIT 1
        """, (clean_token,))
        return cur.fetchone()
    finally:
        if conn is not None:
            conn.close()


@app.get("/librarian/conductor_invite/draft")
def librarian_conductor_invite_draft(request: Request):
    invite_token = str(request.query_params.get("token") or "").strip()
    invite_row = conductor_entry_invite_get_by_token(invite_token)

    if not invite_row:
        return HTMLResponse("Invite not found.", status_code=404)

    if str(invite_row["status"] or "").strip().lower() != "active":
        return HTMLResponse("Invite is no longer active.", status_code=403)

    conductor_email = str(invite_row["conductor_email"] or "").strip().lower()
    if not conductor_email:
        return HTMLResponse("Invite email missing.", status_code=400)

    base_url = str(request.base_url).rstrip("/")
    entry_url = f"{base_url}/entry?token={quote_plus(invite_token)}"
    seal_url = f"{base_url}/static/conductor_invite_gold.png"

    html_body = f"""\
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Annotatio Conductor Invite</title>
</head>
<body style="margin:0; padding:0; background:#071018;">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%; background:#071018; margin:0; padding:0;">
  <tr>
    <td align="center" style="padding:40px 20px;">
      <table role="presentation" width="520" cellspacing="0" cellpadding="0" border="0" style="width:520px; max-width:520px; background:#071018; border-collapse:collapse;">
        <tr>
          <td align="center" style="padding:40px 0;">
            <a href="{entry_url}" style="display:inline-block; text-decoration:none;">
              <img src="{seal_url}" alt="Annotatio Conductor Invite" width="220" style="display:block; width:220px; max-width:220px; height:auto; border:0; margin:0 auto;">
            </a>
          </td>
        </tr>
      </table>
    </td>
  </tr>
</table>
</body>
</html>
"""

    message = EmailMessage()
    message["Subject"] = "Annotatio Conductor Invite"
    message["To"] = conductor_email
    message["X-Unsent"] = "1"
    message.set_content(entry_url)
    message.add_alternative(html_body, subtype="html")

    filename = f"annotatio_conductor_invite_{conductor_email.replace('@', '_at_')}.eml"
    return Response(
        content=message.as_bytes(),
        media_type="message/rfc822",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )


def librarian_conductor_invite_popup_payload(request: Request) -> dict:
    invite_token = str(request.query_params.get("conductor_invite_token") or "").strip()
    if not invite_token:
        return {
            "conductor_invite_popup_active": False,
            "conductor_invite_popup_link": "",
            "conductor_invite_popup_image_url": "",
            "conductor_invite_popup_draft_url": "",
        }

    invite_row = conductor_entry_invite_get_by_token(invite_token)
    if not invite_row:
        return {
            "conductor_invite_popup_active": False,
            "conductor_invite_popup_link": "",
            "conductor_invite_popup_image_url": "",
            "conductor_invite_popup_draft_url": "",
        }

    base_url = str(request.base_url).rstrip("/")
    return {
        "conductor_invite_popup_active": True,
        "conductor_invite_popup_link": f"{base_url}/entry?token={quote_plus(invite_token)}",
        "conductor_invite_popup_image_url": f"{base_url}/static/conductor_invite_gold.png",
        "conductor_invite_popup_draft_url": f"{base_url}/librarian/conductor_invite/draft?token={quote_plus(invite_token)}",
    }


@app.post("/librarian/conductor_invite/create")
def librarian_conductor_invite_create(
    request: Request,
    conductor_email: str = Form(...),
):
    librarian_email = librarian_route_get_email_from_request(request) or "librarian@local"
    clean_conductor_email = str(conductor_email or "").strip().lower()

    if not clean_conductor_email:
        return RedirectResponse(
            f"/librarian?email={quote_plus(librarian_email)}",
            status_code=303,
        )

    invite_token = conductor_entry_invite_create(
        librarian_email=librarian_email,
        conductor_email=clean_conductor_email,
    )

    return RedirectResponse(
        f"/librarian?email={quote_plus(librarian_email)}&conductor_invite_token={quote_plus(invite_token)}",
        status_code=303,
    )


@app.get("/entry")
def annotatio_global_entry(request: Request):
    invite_token = str(request.query_params.get("token") or "").strip()
    invite_row = conductor_entry_invite_get_by_token(invite_token)

    if not invite_row:
        return HTMLResponse("Invite not found.", status_code=404)

    if str(invite_row["status"] or "").strip().lower() != "active":
        return HTMLResponse("Invite is no longer active.", status_code=403)

    conductor_email = str(invite_row["conductor_email"] or "").strip().lower()
    if not conductor_email:
        return HTMLResponse("Invite email missing.", status_code=400)

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT id
        FROM users
        WHERE lower(email)=lower(?)
          AND role='conductor'
        LIMIT 1
        """, (conductor_email,))
        if cur.fetchone():
            return RedirectResponse(
                f"/conductor?email={quote_plus(conductor_email)}",
                status_code=303,
            )
    finally:
        if conn is not None:
            conn.close()

    return RedirectResponse(
        f"/conductor_setup?email={quote_plus(conductor_email)}&invite_token={quote_plus(invite_token)}",
        status_code=303,
    )


@app.get("/musician/invite", response_class=HTMLResponse)
def musician_invite_page(request: Request):
    template_file = TEMPLATES_DIR / "musician_invitation.html"
    if not template_file.exists():
        return HTMLResponse("templates/musician_invitation.html not found.", status_code=404)

    email = (request.query_params.get("email") or "").strip().lower()
    if not email:
        return HTMLResponse("Musician email missing.", status_code=400)

    base_url = str(request.base_url).rstrip("/")
    invite_url = f"{base_url}/musician_setup?email={quote_plus(email)}"
    static_base_url = f"{base_url}/static"

    return templates.TemplateResponse(
        "musician_invitation.html",
        {
            "request": request,
            "user": {"email": email},
            "invite_url": invite_url,
            "static_base_url": static_base_url,
        },
    )


@app.get("/librarian", response_class=HTMLResponse)
def librarian_home_page(request: Request):
    librarian_file = TEMPLATES_DIR / "librarian.html"
    if not librarian_file.exists():
        return HTMLResponse(
            """
            <!doctype html>
            <html lang="en">
            <head>
                <meta charset="utf-8">
                <title>Annotatio — Librarian Dashboard</title>
                <meta name="viewport" content="width=device-width, initial-scale=1">
            </head>
            <body style="margin:0; background:#071018; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif;">
                <div style="max-width:1100px; margin:32px auto; padding:0 18px;">
                    <div style="background:#0b1622; border:1px solid #223548; border-radius:16px; padding:28px;">
                        <div style="font-size:34px; color:#74d3de; margin-bottom:10px;">Librarian Dashboard</div>
                        <div style="font-size:17px; color:#c8d0d8;">templates/librarian.html not found.</div>
                    </div>
                </div>
            </body>
            </html>
            """
        )

    email = (request.query_params.get("email") or "").strip() or "librarian@local"
    venue_names = get_librarian_dashboard_venues(email)
    ensemble_names = librarian_dashboard_ensemble_names(email)
    conductor_names = librarian_dashboard_conductor_names(email)
    traffic_snapshot = librarian_dashboard_concert_summary_fetch_snapshot()
    popup_payload = librarian_conductor_invite_popup_payload(request)

    return templates.TemplateResponse(
        "librarian.html",
        {
            "request": request,
            "user": {"email": email},
            "selected_view": "",
            "librarian_broadcast": "",
            "logo_exists": (STATIC_DIR / "annotatio_logo.png").exists(),
            "concerts": traffic_snapshot["concerts"],
            "fully_green_count": traffic_snapshot["fully_green_count"],
            "red_sections_count": traffic_snapshot["red_sections_count"],
            "connected_now_count": librarian_dashboard_connected_now_count(email),
            "notify_logs": [],
            "lights_by_concert": traffic_snapshot["lights_by_concert"],
            "ensembles": ensemble_names,
            "venues": venue_names,
            "conductors": conductor_names,
            "conductor_alerts": concert_control_conductor_alerts_list(limit=12),
            "conductor_alerts_latest": concert_control_conductor_alerts_latest(),
            "conductor_alerts_pending_review_count": concert_control_conductor_alerts_pending_review_count(),
            "conductor_alerts_forwarded_count": concert_control_conductor_alerts_forwarded_count(),
            "conductor_invite_popup_active": popup_payload["conductor_invite_popup_active"],
            "conductor_invite_popup_link": popup_payload["conductor_invite_popup_link"],
            "conductor_invite_popup_image_url": popup_payload["conductor_invite_popup_image_url"],
            "conductor_invite_popup_draft_url": popup_payload["conductor_invite_popup_draft_url"],
        },
    )


# =====================================================================
# LIBRARIAN LINK ROUTES — STANDALONE BLOCK
# PURPOSE: MAKE LIBRARIAN DASHBOARD LINKS WORK ONLY
# =====================================================================

def librarian_route_get_email_from_request(request: Request) -> str:
    email = str(request.query_params.get("email") or "").strip().lower()
    if email:
        return email

    referer = str(request.headers.get("referer") or "").strip()
    if not referer:
        return ""

    try:
        parsed = urlparse(referer)
        return str(parse_qs(parsed.query).get("email", [""])[0] or "").strip().lower()
    except Exception:
        return ""


@app.get("/directory", response_class=HTMLResponse)
def librarian_directory_redirect():
    return RedirectResponse("/musicians", status_code=303)


@app.get("/profile", response_class=HTMLResponse)
def librarian_profile_shortcut(request: Request):
    email = librarian_route_get_email_from_request(request) or "librarian@local"
    return RedirectResponse(f"/librarian/profile?email={quote_plus(email)}", status_code=303)


def librarian_notes_clean_text(value: str) -> str:
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def librarian_notes_escape_html(value: str) -> str:
    return (
        str(value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def librarian_notes_get_country_code_by_email(librarian_email: str) -> str:
    clean_email = str(librarian_email or "").strip().lower()
    if not clean_email:
        return ""

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT COALESCE(o.country, '') AS country_code
        FROM users u
        LEFT JOIN organisations o ON o.id = u.organisation_id
        WHERE lower(u.email)=lower(?)
          AND u.role='librarian'
        LIMIT 1
        """, (clean_email,))
        row = cur.fetchone()
        return str(row["country_code"] or "").strip().upper() if row else ""
    finally:
        if conn is not None:
            conn.close()


def librarian_notes_format_timestamp_for_country(timestamp_value: str, country_code: str) -> str:
    raw_value = str(timestamp_value or "").strip()
    if not raw_value:
        return "—"

    parsed_value = None
    for candidate in (raw_value, raw_value.replace("Z", "+00:00")):
        try:
            parsed_value = datetime.fromisoformat(candidate)
            break
        except Exception:
            parsed_value = None

    if parsed_value is None:
        return raw_value

    month_name = parsed_value.strftime("%B")
    day_value = parsed_value.strftime("%d")
    year_value = parsed_value.strftime("%Y")
    time_value = parsed_value.strftime("%H:%M")

    clean_country = str(country_code or "").strip().upper()

    if clean_country in {"US"}:
        return f"{month_name} {day_value}, {year_value} {time_value}"

    return f"{day_value} {month_name} {year_value} {time_value}"


def librarian_notes_fetch_entries(librarian_email: str) -> list[dict]:
    clean_email = str(librarian_email or "").strip().lower()
    if not clean_email:
        return []

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT
            id,
            COALESCE(note_text, '') AS note_text,
            COALESCE(created_at, '') AS created_at
        FROM librarian_notes_entries
        WHERE lower(librarian_email)=lower(?)
        ORDER BY datetime(COALESCE(created_at, '')) DESC, id DESC
        """, (clean_email,))
        rows = cur.fetchall()

        entries = []
        for row in rows:
            entries.append(
                {
                    "id": int(row["id"]),
                    "note_text": str(row["note_text"] or "").strip(),
                    "created_at": str(row["created_at"] or "").strip(),
                }
            )
        return entries
    finally:
        if conn is not None:
            conn.close()


def librarian_notes_add_entry(librarian_email: str, note_text: str) -> None:
    clean_email = str(librarian_email or "").strip().lower()
    clean_text = librarian_notes_clean_text(note_text)
    now = datetime.utcnow().isoformat()

    if not clean_email or not clean_text:
        return

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO librarian_notes_entries (
            librarian_email,
            note_text,
            created_at
        ) VALUES (?, ?, ?)
        """, (
            clean_email,
            clean_text,
            now,
        ))
        conn.commit()
    finally:
        if conn is not None:
            conn.close()


def librarian_notes_cards_markup(entries: list[dict], country_code: str) -> str:
    if not entries:
        return """
        <div style="padding:18px; border:1px dashed #31455c; border-radius:12px; background:#101e2d; color:#c8d0d8; font-size:16px; line-height:1.7;">
            No notes saved yet.
        </div>
        """

    cards_html = ""
    for entry in entries:
        created_at = librarian_notes_escape_html(
            librarian_notes_format_timestamp_for_country(
                entry.get("created_at") or "",
                country_code,
            )
        )
        note_text = librarian_notes_escape_html(entry.get("note_text") or "")

        cards_html += f"""
        <div style="padding:16px 18px; border:1px solid #31455c; border-radius:12px; background:#101e2d;">
            <div style="font-size:13px; letter-spacing:0.06em; text-transform:uppercase; color:#d5b06a; margin-bottom:8px;">
                Entered {created_at}
            </div>
            <div style="color:#f2f0ea; font-size:16px; line-height:1.7; white-space:pre-wrap;">{note_text}</div>
        </div>
        """

    return cards_html


@app.get("/librarian/notes", response_class=HTMLResponse)
def librarian_notes_page(request: Request):
    email = librarian_route_get_email_from_request(request) or "librarian@local"
    entries = librarian_notes_fetch_entries(email)
    country_code = librarian_notes_get_country_code_by_email(email)
    note_cards = librarian_notes_cards_markup(entries, country_code)

    page_html = f"""
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Annotatio — Librarian Notes</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
    </head>
    <body style="margin:0; background:#071018; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif;">
        <div style="max-width:1200px; margin:32px auto; padding:0 18px;">
            <div style="background:#0b1622; border:1px solid #223548; border-radius:16px; padding:28px; box-shadow:0 10px 30px rgba(0,0,0,0.28);">
                <div style="font-size:34px; color:#74d3de; margin-bottom:10px;">Librarian Notes</div>
                <div style="width:190px; height:1px; background:#b89457; margin:0 0 18px 0;"></div>

                <div style="padding:18px; border:1px solid #31455c; border-radius:12px; background:#101e2d; color:#c8d0d8; font-size:16px; line-height:1.7; margin-bottom:18px;">
                    Enter one note at a time. Each saved note is added to the log with the date and time it was entered.
                </div>

                <form method="post" action="/librarian/notes/save?email={quote_plus(email)}" style="margin-bottom:22px;">
                    <label for="note_text" style="display:block; margin-bottom:8px; color:#d5b06a; font-size:16px;">New note</label>
                    <textarea id="note_text" name="note_text" style="width:100%; min-height:150px; padding:14px 16px; border-radius:12px; border:1px solid #31455c; background:#0d1927; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif; font-size:16px; line-height:1.7; box-sizing:border-box; resize:vertical;"></textarea>

                    <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:16px;">
                        <button type="submit" style="display:inline-block; padding:10px 18px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; text-decoration:none; font-size:16px; font-family:Georgia, 'Times New Roman', serif; cursor:pointer;">Add Note</button>
                        <a href="/librarian?email={quote_plus(email)}" style="display:inline-block; padding:10px 18px; border-radius:999px; border:1px solid #31455c; background:#101e2d; color:#f2f0ea; text-decoration:none; font-size:16px;">Back to Dashboard</a>
                    </div>
                </form>

                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Notes Log</div>

                <div style="display:grid; gap:12px;">
                    {note_cards}
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(page_html)


@app.post("/librarian/notes/save")
def librarian_notes_save(
    request: Request,
    note_text: str = Form(""),
):
    email = librarian_route_get_email_from_request(request) or "librarian@local"
    librarian_notes_add_entry(email, note_text)
    return RedirectResponse(f"/librarian/notes?email={quote_plus(email)}", status_code=303)


@app.get("/librarian/ensemble", response_class=HTMLResponse)
def librarian_ensemble_page(request: Request):
    template_file = TEMPLATES_DIR / "ensemble.html"
    if not template_file.exists():
        return HTMLResponse("templates/ensemble.html not found.", status_code=404)

    email = librarian_route_get_email_from_request(request) or "librarian@local"

    return templates.TemplateResponse(
        "ensemble.html",
        {
            "request": request,
            "user": {"email": email},
            "rows": [],
            "musicians": [],
            "invites": [],
            "ensembles": [],
            "selected_ensemble_id": "",
            "msg": "",
        },
    )


# =====================================================================
# LIBRARIAN PROFILE — STANDALONE BLOCK
# PURPOSE: LIBRARIAN PROFILE + GLOBAL LISTING ONLY
# =====================================================================

def librarian_profile_clean_text(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def librarian_profile_escape_html(value: str) -> str:
    return (
        str(value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def librarian_profile_get_user_row_by_email(email: str):
    clean_email = str(email or "").strip().lower()
    if not clean_email:
        return None

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT
            u.id,
            u.name,
            u.email,
            u.role,
            u.organisation_id,
            COALESCE(o.name, '') AS organisation_name,
            COALESCE(o.city, '') AS organisation_city,
            COALESCE(o.country, '') AS organisation_country
        FROM users u
        LEFT JOIN organisations o ON o.id = u.organisation_id
        WHERE lower(u.email)=lower(?)
          AND u.role='librarian'
        LIMIT 1
        """, (clean_email,))
        return cur.fetchone()
    finally:
        if conn is not None:
            conn.close()


def librarian_profile_get_profile_row(user_id: int):
    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT *
        FROM librarian_profiles
        WHERE user_id=?
        LIMIT 1
        """, (int(user_id),))
        return cur.fetchone()
    finally:
        if conn is not None:
            conn.close()


def librarian_profile_build_template_payload(user_row, profile_row) -> tuple[dict, dict]:
    profile = dict(profile_row) if profile_row else {}

    full_name = librarian_profile_clean_text(profile.get("full_name") or user_row["name"] or "")
    preferred_name = librarian_profile_clean_text(profile.get("preferred_name") or "")
    mobile = librarian_profile_clean_text(profile.get("mobile") or "")
    notes = str(profile.get("notes") or "").strip()
    city = librarian_profile_clean_text(profile.get("city") or user_row["organisation_city"] or "")
    country_name = librarian_profile_clean_text(profile.get("country_name") or user_row["organisation_country"] or "")
    organisation_name = librarian_profile_clean_text(profile.get("organisation_name") or user_row["organisation_name"] or "")
    ensemble_name = librarian_profile_clean_text(profile.get("ensemble_name") or "")
    global_search_visible = librarian_profile_clean_text(profile.get("global_search_visible") or "Yes")

    musician = {
        "name": full_name or user_row["email"],
        "preferred_name": preferred_name,
        "email": user_row["email"],
        "mobile": mobile,
        "city": city,
        "state_region_territory": librarian_profile_clean_text(profile.get("state_region_territory") or ""),
        "country_name": country_name,
        "global_search_visible": global_search_visible,
        "notes": notes,
        "primary_instrument": librarian_profile_clean_text(profile.get("role_title") or "Librarian"),
        "voice_type": organisation_name or "",
        "other_instruments": "",
        "ensembles": ensemble_name,
    }

    listing_profile = {
        "role_title": librarian_profile_clean_text(profile.get("role_title") or "Librarian"),
        "organisation_name": organisation_name,
        "ensemble_name": ensemble_name,
        "city": city,
        "state_region_territory": librarian_profile_clean_text(profile.get("state_region_territory") or ""),
        "country_name": country_name,
        "work_email": librarian_profile_clean_text(profile.get("work_email") or user_row["email"] or ""),
        "notes": str(profile.get("notes") or "").strip(),
    }

    return musician, listing_profile


def librarian_profile_upsert_listing(
    user_row,
    role_title: str,
    organisation_name: str,
    ensemble_name: str,
    city: str,
    state_region_territory: str,
    country_name: str,
    work_email: str,
    notes: str,
    global_search_visible: str,
) -> None:
    now = datetime.utcnow().isoformat()

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO librarian_profiles (
            user_id,
            organisation_id,
            email,
            full_name,
            preferred_name,
            mobile,
            notes,
            role_title,
            organisation_name,
            ensemble_name,
            city,
            state_region_territory,
            country_code,
            country_name,
            work_email,
            global_search_visible,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            organisation_id=excluded.organisation_id,
            email=excluded.email,
            notes=excluded.notes,
            role_title=excluded.role_title,
            organisation_name=excluded.organisation_name,
            ensemble_name=excluded.ensemble_name,
            city=excluded.city,
            state_region_territory=excluded.state_region_territory,
            country_name=excluded.country_name,
            work_email=excluded.work_email,
            global_search_visible=excluded.global_search_visible,
            updated_at=excluded.updated_at
        """, (
            int(user_row["id"]),
            user_row["organisation_id"],
            str(user_row["email"] or "").strip().lower(),
            librarian_profile_clean_text(user_row["name"] or ""),
            "",
            "",
            str(notes or "").strip(),
            librarian_profile_clean_text(role_title or "Librarian"),
            librarian_profile_clean_text(organisation_name or ""),
            librarian_profile_clean_text(ensemble_name or ""),
            librarian_profile_clean_text(city or ""),
            librarian_profile_clean_text(state_region_territory or ""),
            "",
            librarian_profile_clean_text(country_name or ""),
            librarian_profile_clean_text(work_email or ""),
            "No" if str(global_search_visible or "").strip().lower() == "no" else "Yes",
            now,
            now,
        ))
        conn.commit()
    finally:
        if conn is not None:
            conn.close()


@app.get("/librarian/profile", response_class=HTMLResponse)
def librarian_profile_page(request: Request):
    template_file = TEMPLATES_DIR / "librarian_profile.html"
    if not template_file.exists():
        return HTMLResponse("templates/librarian_profile.html not found.", status_code=404)

    email = librarian_route_get_email_from_request(request) or "librarian@local"
    user_row = librarian_profile_get_user_row_by_email(email)

    if not user_row:
        conn = None
        try:
            conn = db()
            cur = conn.cursor()
            cur.execute("""
            SELECT
                u.id,
                u.name,
                u.email,
                u.role,
                u.organisation_id,
                COALESCE(o.name, '') AS organisation_name,
                COALESCE(o.city, '') AS organisation_city,
                COALESCE(o.country, '') AS organisation_country
            FROM users u
            LEFT JOIN organisations o ON o.id = u.organisation_id
            WHERE u.role='librarian'
            ORDER BY u.id ASC
            LIMIT 1
            """)
            user_row = cur.fetchone()
        finally:
            if conn is not None:
                conn.close()

    if user_row:
        profile_row = librarian_profile_get_profile_row(int(user_row["id"]))
        musician, profile = librarian_profile_build_template_payload(user_row, profile_row)
        user_email = str(user_row["email"] or email).strip()
    else:
        musician = {
            "name": "Librarian",
            "preferred_name": "",
            "email": email,
            "mobile": "",
            "city": "",
            "state_region_territory": "",
            "country_name": "",
            "global_search_visible": "Yes",
            "notes": "",
            "primary_instrument": "Librarian",
            "voice_type": "",
            "other_instruments": "",
            "ensembles": "",
        }

        profile = {
            "role_title": "Librarian",
            "organisation_name": "",
            "ensemble_name": "",
            "city": "",
            "state_region_territory": "",
            "country_name": "",
            "work_email": email,
            "notes": "",
        }
        user_email = email

    return templates.TemplateResponse(
        "librarian_profile.html",
        {
            "request": request,
            "user": {"email": user_email},
            "musician": musician,
            "profile": profile,
        },
    )


@app.get("/librarian/profile/pdf")
def librarian_profile_export_pdf(request: Request):
    from io import BytesIO
    from fastapi.responses import StreamingResponse, PlainTextResponse

    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.pdfgen import canvas
    except Exception as e:
        return PlainTextResponse(f"PDF export is not available: {e}", status_code=500)

    email = librarian_route_get_email_from_request(request) or "librarian@local"
    user_row = librarian_profile_get_user_row_by_email(email)

    if not user_row:
        conn = None
        try:
            conn = db()
            cur = conn.cursor()
            cur.execute("""
            SELECT
                u.id,
                u.name,
                u.email,
                u.role,
                u.organisation_id,
                COALESCE(o.name, '') AS organisation_name,
                COALESCE(o.city, '') AS organisation_city,
                COALESCE(o.country, '') AS organisation_country
            FROM users u
            LEFT JOIN organisations o ON o.id = u.organisation_id
            WHERE u.role='librarian'
            ORDER BY u.id ASC
            LIMIT 1
            """)
            user_row = cur.fetchone()
        finally:
            if conn is not None:
                conn.close()

    profile_row = librarian_profile_get_profile_row(int(user_row["id"])) if user_row else None

    if user_row:
        musician, profile = librarian_profile_build_template_payload(user_row, profile_row)
        full_name = str(musician.get("name") or "Librarian").strip() or "Librarian"
        preferred_name = str(musician.get("preferred_name") or "").strip()
        email_value = str(musician.get("email") or user_row["email"] or "").strip()
        mobile = str(musician.get("mobile") or "").strip()
        city = str(musician.get("city") or "").strip()
        state_region_territory = str(musician.get("state_region_territory") or "").strip()
        country_name = str(musician.get("country_name") or "").strip()
        visible = str(musician.get("global_search_visible") or "Yes").strip()
        notes = str(musician.get("notes") or "").strip()
        role_title = str(profile.get("role_title") or "Librarian").strip()
        organisation_name = str(profile.get("organisation_name") or "").strip()
        ensemble_name = str(profile.get("ensemble_name") or "").strip()
        work_email = str(profile.get("work_email") or email_value).strip()
    else:
        full_name = "Librarian"
        preferred_name = ""
        email_value = email
        mobile = ""
        city = ""
        state_region_territory = ""
        country_name = ""
        visible = "Yes"
        notes = ""
        role_title = "Librarian"
        organisation_name = ""
        ensemble_name = ""
        work_email = email

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    page_width, page_height = A4

    left = 18 * mm
    top = page_height - (18 * mm)
    y = top

    def write_line(label: str, value: str):
        nonlocal y
        if y < 22 * mm:
            pdf.showPage()
            y = top

        pdf.setFont("Helvetica-Bold", 11)
        pdf.drawString(left, y, label)
        y -= 6 * mm

        text_value = str(value or "—")
        chunk_size = 92
        chunks = [text_value[i:i + chunk_size] for i in range(0, len(text_value), chunk_size)] or ["—"]

        pdf.setFont("Helvetica", 10)
        for chunk in chunks:
            pdf.drawString(left, y, chunk)
            y -= 5 * mm

        y -= 2 * mm

    pdf.setTitle("Annotatio Librarian Profile")
    pdf.setFont("Helvetica-Bold", 18)
    pdf.drawString(left, y, "Annotatio Librarian Profile")
    y -= 10 * mm

    write_line("Full name", full_name)
    write_line("Preferred name", preferred_name)
    write_line("Role title", role_title)
    write_line("Organisation", organisation_name)
    write_line("Ensemble", ensemble_name)
    write_line("Email", email_value)
    write_line("Work email", work_email)
    write_line("Mobile", mobile)
    write_line("City", city)
    write_line("State / Region / Territory", state_region_territory)
    write_line("Country", country_name)
    write_line("Visible in global search", visible)
    write_line("Notes", notes)

    pdf.save()
    buffer.seek(0)

    safe_name = "_".join((preferred_name or full_name or "librarian_profile").split()) or "librarian_profile"
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{safe_name}.pdf"'},
    )


@app.get("/librarian/global_listing", response_class=HTMLResponse)
def librarian_global_listing_page(request: Request):
    email = librarian_route_get_email_from_request(request) or "librarian@local"
    user_row = librarian_profile_get_user_row_by_email(email)

    if not user_row:
        conn = None
        try:
            conn = db()
            cur = conn.cursor()
            cur.execute("""
            SELECT
                u.id,
                u.name,
                u.email,
                u.role,
                u.organisation_id,
                COALESCE(o.name, '') AS organisation_name,
                COALESCE(o.city, '') AS organisation_city,
                COALESCE(o.country, '') AS organisation_country
            FROM users u
            LEFT JOIN organisations o ON o.id = u.organisation_id
            WHERE u.role='librarian'
            ORDER BY u.id ASC
            LIMIT 1
            """)
            user_row = cur.fetchone()
        finally:
            if conn is not None:
                conn.close()

    if not user_row:
        user_row = {
            "id": 0,
            "name": "Librarian",
            "email": email,
            "role": "librarian",
            "organisation_id": None,
            "organisation_name": "",
            "organisation_city": "",
            "organisation_country": "",
        }

    profile_row = librarian_profile_get_profile_row(int(user_row["id"]))
    profile = dict(profile_row) if profile_row else {}

    role_title = librarian_profile_escape_html(profile.get("role_title") or "Librarian")
    organisation_name = librarian_profile_escape_html(profile.get("organisation_name") or user_row["organisation_name"] or "")
    ensemble_name = librarian_profile_escape_html(profile.get("ensemble_name") or "")
    city = librarian_profile_escape_html(profile.get("city") or user_row["organisation_city"] or "")
    state_region_territory = librarian_profile_escape_html(profile.get("state_region_territory") or "")
    country_name = librarian_profile_escape_html(profile.get("country_name") or user_row["organisation_country"] or "")
    work_email = librarian_profile_escape_html(profile.get("work_email") or user_row["email"] or "")
    notes = librarian_profile_escape_html(profile.get("notes") or "")
    global_search_visible = librarian_profile_clean_text(profile.get("global_search_visible") or "Yes")

    page_html = f"""
    <!doctype html>
    <html>
    <head>
    <meta charset="utf-8">
    <title>Annotatio • Global Librarian Listing</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{
        margin:0;
        font-family: Georgia, "Times New Roman", serif;
        background:#071018;
        color:#f2f0ea;
    }}

    .page {{
        max-width:980px;
        margin:40px auto;
        padding:20px;
    }}

    .card {{
        background:#0b1622;
        border:1px solid #223548;
        border-radius:14px;
        padding:40px;
        box-shadow:0 10px 30px rgba(0,0,0,0.35);
    }}

    .header {{
        text-align:center;
        margin-bottom:30px;
    }}

    .header img {{
        width:420px;
        max-width:80%;
    }}

    .title {{
        font-size:34px;
        margin-top:10px;
        color:#74d3de;
    }}

    .rule {{
        height:1px;
        width:220px;
        background:#b89457;
        margin:18px auto 10px auto;
    }}

    .section-title {{
        font-size:22px;
        color:#74d3de;
        margin:0 0 18px 0;
    }}

    .grid {{
        display:grid;
        grid-template-columns:repeat(2, minmax(0, 1fr));
        gap:18px 20px;
    }}

    .field {{
        display:flex;
        flex-direction:column;
        gap:8px;
    }}

    .field.full {{
        grid-column:1 / -1;
    }}

    .label {{
        color:#d5b06a;
        font-size:15px;
    }}

    .input, .textarea, .select {{
        width:100%;
        padding:12px 14px;
        font-size:16px;
        color:#f2f0ea;
        background:#0d1b2b;
        border:1px solid #2d4257;
        border-radius:10px;
        line-height:1.5;
        box-sizing:border-box;
        font-family:Georgia, "Times New Roman", serif;
    }}

    .textarea {{
        min-height:130px;
        resize:vertical;
    }}

    .actions {{
        margin-top:22px;
        display:flex;
        gap:12px;
        flex-wrap:wrap;
    }}

    .button {{
        display:inline-block;
        padding:10px 18px;
        background:#173247;
        border:1px solid #b89457;
        border-radius:999px;
        text-decoration:none;
        color:#f2f0ea;
        font-family:Georgia, "Times New Roman", serif;
        font-size:16px;
        cursor:pointer;
    }}
    </style>
    </head>
    <body>
    <div class="page">
      <div class="card">
        <div class="header">
          <img src="/static/annotatio_logo.png">
          <div class="title">Global Librarian Listing</div>
          <div class="rule"></div>
        </div>

        <h2 class="section-title">Edit global search listing</h2>

        <form method="post" action="/librarian/global_listing/save?email={librarian_profile_escape_html(user_row['email'])}">
          <div class="grid">
            <div class="field">
              <div class="label">Role title</div>
              <input class="input" type="text" name="role_title" value="{role_title}">
            </div>

            <div class="field">
              <div class="label">Organisation name</div>
              <input class="input" type="text" name="organisation_name" value="{organisation_name}">
            </div>

            <div class="field">
              <div class="label">Ensemble name</div>
              <input class="input" type="text" name="ensemble_name" value="{ensemble_name}">
            </div>

            <div class="field">
              <div class="label">Work email</div>
              <input class="input" type="text" name="work_email" value="{work_email}">
            </div>

            <div class="field">
              <div class="label">City</div>
              <input class="input" type="text" name="city" value="{city}">
            </div>

            <div class="field">
              <div class="label">State / Region / Territory</div>
              <input class="input" type="text" name="state_region_territory" value="{state_region_territory}">
            </div>

            <div class="field">
              <div class="label">Country</div>
              <input class="input" type="text" name="country_name" value="{country_name}">
            </div>

            <div class="field">
              <div class="label">Visible in global search</div>
              <select class="select" name="global_search_visible">
                <option value="Yes"{" selected" if global_search_visible == "Yes" else ""}>Yes</option>
                <option value="No"{" selected" if global_search_visible == "No" else ""}>No</option>
              </select>
            </div>

            <div class="field full">
              <div class="label">Professional notes</div>
              <textarea class="textarea" name="notes">{notes}</textarea>
            </div>
          </div>

          <div class="actions">
            <button class="button" type="submit">Save Listing</button>
            <a class="button" href="/librarian/profile?email={librarian_profile_escape_html(user_row['email'])}">Back to Profile</a>
          </div>
        </form>
      </div>
    </div>
    </body>
    </html>
    """
    return HTMLResponse(page_html)


@app.post("/librarian/global_listing/save")
def librarian_global_listing_save(
    request: Request,
    role_title: str = Form(""),
    organisation_name: str = Form(""),
    ensemble_name: str = Form(""),
    city: str = Form(""),
    state_region_territory: str = Form(""),
    country_name: str = Form(""),
    work_email: str = Form(""),
    notes: str = Form(""),
    global_search_visible: str = Form("Yes"),
):
    email = librarian_route_get_email_from_request(request) or "librarian@local"
    user_row = librarian_profile_get_user_row_by_email(email)
    if not user_row:
        return HTMLResponse("Librarian not found.", status_code=404)

    librarian_profile_upsert_listing(
        user_row=user_row,
        role_title=role_title,
        organisation_name=organisation_name,
        ensemble_name=ensemble_name,
        city=city,
        state_region_territory=state_region_territory,
        country_name=country_name,
        work_email=work_email,
        notes=notes,
        global_search_visible=global_search_visible,
    )

    return RedirectResponse(f"/librarian/profile?email={quote_plus(user_row['email'])}&saved=1", status_code=303)


def librarian_profile_upsert_personal_details(
    user_row,
    full_name: str,
    preferred_name: str,
    mobile: str,
    city: str,
    state_region_territory: str,
    country_name: str,
    notes: str,
    global_search_visible: str,
) -> None:
    now = datetime.utcnow().isoformat()
    clean_full_name = librarian_profile_clean_text(full_name or user_row["name"] or user_row["email"] or "")
    clean_preferred_name = librarian_profile_clean_text(preferred_name or "")
    clean_mobile = librarian_profile_clean_text(mobile or "")
    clean_city = librarian_profile_clean_text(city or "")
    clean_state = librarian_profile_clean_text(state_region_territory or "")
    clean_country = librarian_profile_clean_text(country_name or "")
    clean_notes = str(notes or "").strip()
    clean_visible = "No" if str(global_search_visible or "").strip().lower() == "no" else "Yes"

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO librarian_profiles (
            user_id,
            organisation_id,
            email,
            full_name,
            preferred_name,
            mobile,
            notes,
            role_title,
            organisation_name,
            ensemble_name,
            city,
            state_region_territory,
            country_code,
            country_name,
            work_email,
            global_search_visible,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            organisation_id=excluded.organisation_id,
            email=excluded.email,
            full_name=excluded.full_name,
            preferred_name=excluded.preferred_name,
            mobile=excluded.mobile,
            notes=excluded.notes,
            city=excluded.city,
            state_region_territory=excluded.state_region_territory,
            country_name=excluded.country_name,
            global_search_visible=excluded.global_search_visible,
            updated_at=excluded.updated_at
        """, (
            int(user_row["id"]),
            user_row["organisation_id"],
            str(user_row["email"] or "").strip().lower(),
            clean_full_name,
            clean_preferred_name,
            clean_mobile,
            clean_notes,
            "",
            "",
            "",
            clean_city,
            clean_state,
            "",
            clean_country,
            str(user_row["email"] or "").strip().lower(),
            clean_visible,
            now,
            now,
        ))
        cur.execute("""
        UPDATE users
        SET name=?
        WHERE id=?
        """, (
            clean_preferred_name or clean_full_name,
            int(user_row["id"]),
        ))
        conn.commit()
    finally:
        if conn is not None:
            conn.close()


def librarian_profile_upsert_instrument_details(
    user_row,
    role_title: str,
    organisation_name: str,
) -> None:
    now = datetime.utcnow().isoformat()
    clean_role_title = librarian_profile_clean_text(role_title or "Librarian")
    clean_organisation_name = librarian_profile_clean_text(organisation_name or "")

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO librarian_profiles (
            user_id,
            organisation_id,
            email,
            full_name,
            preferred_name,
            mobile,
            notes,
            role_title,
            organisation_name,
            ensemble_name,
            city,
            state_region_territory,
            country_code,
            country_name,
            work_email,
            global_search_visible,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            organisation_id=excluded.organisation_id,
            email=excluded.email,
            role_title=excluded.role_title,
            organisation_name=excluded.organisation_name,
            updated_at=excluded.updated_at
        """, (
            int(user_row["id"]),
            user_row["organisation_id"],
            str(user_row["email"] or "").strip().lower(),
            "",
            "",
            "",
            "",
            clean_role_title,
            clean_organisation_name,
            "",
            "",
            "",
            "",
            "",
            str(user_row["email"] or "").strip().lower(),
            "Yes",
            now,
            now,
        ))
        conn.commit()
    finally:
        if conn is not None:
            conn.close()


def librarian_profile_upsert_ensemble_details(
    user_row,
    ensemble_name: str,
) -> None:
    now = datetime.utcnow().isoformat()
    clean_ensemble_name = ", ".join(
        [
            item
            for item in [librarian_profile_clean_text(part) for part in str(ensemble_name or "").split(",")]
            if item
        ]
    )

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO librarian_profiles (
            user_id,
            organisation_id,
            email,
            full_name,
            preferred_name,
            mobile,
            notes,
            role_title,
            organisation_name,
            ensemble_name,
            city,
            state_region_territory,
            country_code,
            country_name,
            work_email,
            global_search_visible,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            organisation_id=excluded.organisation_id,
            email=excluded.email,
            ensemble_name=excluded.ensemble_name,
            updated_at=excluded.updated_at
        """, (
            int(user_row["id"]),
            user_row["organisation_id"],
            str(user_row["email"] or "").strip().lower(),
            "",
            "",
            "",
            "",
            "",
            "",
            clean_ensemble_name,
            "",
            "",
            "",
            "",
            str(user_row["email"] or "").strip().lower(),
            "Yes",
            now,
            now,
        ))
        conn.commit()
    finally:
        if conn is not None:
            conn.close()


@app.get("/librarian/edit_details", response_class=HTMLResponse)
def librarian_edit_details_page(request: Request):
    email = librarian_route_get_email_from_request(request) or "librarian@local"
    user_row = librarian_profile_get_user_row_by_email(email)

    if not user_row:
        conn = None
        try:
            conn = db()
            cur = conn.cursor()
            cur.execute("""
            SELECT
                u.id,
                u.name,
                u.email,
                u.role,
                u.organisation_id,
                COALESCE(o.name, '') AS organisation_name,
                COALESCE(o.city, '') AS organisation_city,
                COALESCE(o.country, '') AS organisation_country
            FROM users u
            LEFT JOIN organisations o ON o.id = u.organisation_id
            WHERE u.role='librarian'
            ORDER BY u.id ASC
            LIMIT 1
            """)
            user_row = cur.fetchone()
        finally:
            if conn is not None:
                conn.close()

    if not user_row:
        user_row = {
            "id": 0,
            "name": "Librarian",
            "email": email,
            "role": "librarian",
            "organisation_id": None,
            "organisation_name": "",
            "organisation_city": "",
            "organisation_country": "",
        }

    profile_row = librarian_profile_get_profile_row(int(user_row["id"]))
    profile = dict(profile_row) if profile_row else {}

    full_name = librarian_profile_escape_html(profile.get("full_name") or user_row["name"] or "")
    preferred_name = librarian_profile_escape_html(profile.get("preferred_name") or "")
    mobile = librarian_profile_escape_html(profile.get("mobile") or "")
    city = librarian_profile_escape_html(profile.get("city") or user_row["organisation_city"] or "")
    state_region_territory = librarian_profile_escape_html(profile.get("state_region_territory") or "")
    country_name = librarian_profile_escape_html(profile.get("country_name") or user_row["organisation_country"] or "")
    notes = librarian_profile_escape_html(profile.get("notes") or "")
    global_search_visible = librarian_profile_clean_text(profile.get("global_search_visible") or "Yes")

    page_html = f"""
    <!doctype html>
    <html>
    <head>
    <meta charset="utf-8">
    <title>Annotatio • Edit Personal Details</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{
        margin:0;
        font-family: Georgia, "Times New Roman", serif;
        background:#071018;
        color:#f2f0ea;
    }}
    .page {{
        max-width:980px;
        margin:40px auto;
        padding:20px;
    }}
    .card {{
        background:#0b1622;
        border:1px solid #223548;
        border-radius:14px;
        padding:40px;
        box-shadow:0 10px 30px rgba(0,0,0,0.35);
    }}
    .header {{
        text-align:center;
        margin-bottom:30px;
    }}
    .header img {{
        width:420px;
        max-width:80%;
    }}
    .title {{
        font-size:34px;
        margin-top:10px;
        color:#74d3de;
    }}
    .rule {{
        height:1px;
        width:220px;
        background:#b89457;
        margin:18px auto 10px auto;
    }}
    .section-title {{
        font-size:22px;
        color:#74d3de;
        margin:0 0 18px 0;
    }}
    .grid {{
        display:grid;
        grid-template-columns:repeat(2, minmax(0, 1fr));
        gap:18px 20px;
    }}
    .field {{
        display:flex;
        flex-direction:column;
        gap:8px;
    }}
    .field.full {{
        grid-column:1 / -1;
    }}
    .label {{
        color:#d5b06a;
        font-size:15px;
    }}
    .input, .textarea, .select {{
        width:100%;
        padding:12px 14px;
        font-size:16px;
        color:#f2f0ea;
        background:#0d1b2b;
        border:1px solid #2d4257;
        border-radius:10px;
        line-height:1.5;
        box-sizing:border-box;
        font-family:Georgia, "Times New Roman", serif;
    }}
    .textarea {{
        min-height:130px;
        resize:vertical;
    }}
    .actions {{
        margin-top:22px;
        display:flex;
        gap:12px;
        flex-wrap:wrap;
    }}
    .button {{
        display:inline-block;
        padding:10px 18px;
        background:#173247;
        border:1px solid #b89457;
        border-radius:999px;
        text-decoration:none;
        color:#f2f0ea;
        font-family:Georgia, "Times New Roman", serif;
        font-size:16px;
        cursor:pointer;
    }}
    </style>
    </head>
    <body>
    <div class="page">
      <div class="card">
        <div class="header">
          <img src="/static/annotatio_logo.png">
          <div class="title">Edit Personal Details</div>
          <div class="rule"></div>
        </div>

        <h2 class="section-title">Librarian personal details</h2>

        <form method="post" action="/librarian/edit_details/save?email={librarian_profile_escape_html(user_row['email'])}">
          <div class="grid">
            <div class="field">
              <div class="label">Full name</div>
              <input class="input" type="text" name="full_name" value="{full_name}" required>
            </div>

            <div class="field">
              <div class="label">Preferred name</div>
              <input class="input" type="text" name="preferred_name" value="{preferred_name}">
            </div>

            <div class="field">
              <div class="label">Email</div>
              <input class="input" type="text" value="{librarian_profile_escape_html(user_row['email'])}" disabled>
            </div>

            <div class="field">
              <div class="label">Mobile</div>
              <input class="input" type="text" name="mobile" value="{mobile}">
            </div>

            <div class="field">
              <div class="label">City</div>
              <input class="input" type="text" name="city" value="{city}">
            </div>

            <div class="field">
              <div class="label">State / Region / Territory</div>
              <input class="input" type="text" name="state_region_territory" value="{state_region_territory}">
            </div>

            <div class="field">
              <div class="label">Country</div>
              <input class="input" type="text" name="country_name" value="{country_name}">
            </div>

            <div class="field">
              <div class="label">Visible in global search</div>
              <select class="select" name="global_search_visible">
                <option value="Yes"{" selected" if global_search_visible == "Yes" else ""}>Yes</option>
                <option value="No"{" selected" if global_search_visible == "No" else ""}>No</option>
              </select>
            </div>

            <div class="field full">
              <div class="label">Notes for the Librarian</div>
              <textarea class="textarea" name="notes">{notes}</textarea>
            </div>
          </div>

          <div class="actions">
            <button class="button" type="submit">Save Personal Details</button>
            <a class="button" href="/librarian/profile?email={librarian_profile_escape_html(user_row['email'])}">Back to Profile</a>
          </div>
        </form>
      </div>
    </div>
    </body>
    </html>
    """
    return HTMLResponse(page_html)


@app.post("/librarian/edit_details/save")
def librarian_edit_details_save(
    request: Request,
    full_name: str = Form(""),
    preferred_name: str = Form(""),
    mobile: str = Form(""),
    city: str = Form(""),
    state_region_territory: str = Form(""),
    country_name: str = Form(""),
    notes: str = Form(""),
    global_search_visible: str = Form("Yes"),
):
    email = librarian_route_get_email_from_request(request) or "librarian@local"
    user_row = librarian_profile_get_user_row_by_email(email)
    if not user_row:
        return HTMLResponse("Librarian not found.", status_code=404)

    librarian_profile_upsert_personal_details(
        user_row=user_row,
        full_name=full_name,
        preferred_name=preferred_name,
        mobile=mobile,
        city=city,
        state_region_territory=state_region_territory,
        country_name=country_name,
        notes=notes,
        global_search_visible=global_search_visible,
    )

    return RedirectResponse(f"/librarian/profile?email={quote_plus(user_row['email'])}&saved=1", status_code=303)


@app.get("/librarian/update_instruments", response_class=HTMLResponse)
def librarian_update_instruments_page(request: Request):
    email = librarian_route_get_email_from_request(request) or "librarian@local"
    user_row = librarian_profile_get_user_row_by_email(email)

    if not user_row:
        conn = None
        try:
            conn = db()
            cur = conn.cursor()
            cur.execute("""
            SELECT
                u.id,
                u.name,
                u.email,
                u.role,
                u.organisation_id,
                COALESCE(o.name, '') AS organisation_name,
                COALESCE(o.city, '') AS organisation_city,
                COALESCE(o.country, '') AS organisation_country
            FROM users u
            LEFT JOIN organisations o ON o.id = u.organisation_id
            WHERE u.role='librarian'
            ORDER BY u.id ASC
            LIMIT 1
            """)
            user_row = cur.fetchone()
        finally:
            if conn is not None:
                conn.close()

    if not user_row:
        user_row = {
            "id": 0,
            "name": "Librarian",
            "email": email,
            "role": "librarian",
            "organisation_id": None,
            "organisation_name": "",
            "organisation_city": "",
            "organisation_country": "",
        }

    profile_row = librarian_profile_get_profile_row(int(user_row["id"]))
    profile = dict(profile_row) if profile_row else {}

    role_title = librarian_profile_escape_html(profile.get("role_title") or "Librarian")
    organisation_name = librarian_profile_escape_html(profile.get("organisation_name") or user_row["organisation_name"] or "")

    page_html = f"""
    <!doctype html>
    <html>
    <head>
    <meta charset="utf-8">
    <title>Annotatio • Edit Librarian Role</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{
        margin:0;
        font-family: Georgia, "Times New Roman", serif;
        background:#071018;
        color:#f2f0ea;
    }}
    .page {{
        max-width:980px;
        margin:40px auto;
        padding:20px;
    }}
    .card {{
        background:#0b1622;
        border:1px solid #223548;
        border-radius:14px;
        padding:40px;
        box-shadow:0 10px 30px rgba(0,0,0,0.35);
    }}
    .header {{
        text-align:center;
        margin-bottom:30px;
    }}
    .header img {{
        width:420px;
        max-width:80%;
    }}
    .title {{
        font-size:34px;
        margin-top:10px;
        color:#74d3de;
    }}
    .rule {{
        height:1px;
        width:220px;
        background:#b89457;
        margin:18px auto 10px auto;
    }}
    .section-title {{
        font-size:22px;
        color:#74d3de;
        margin:0 0 18px 0;
    }}
    .grid {{
        display:grid;
        grid-template-columns:repeat(2, minmax(0, 1fr));
        gap:18px 20px;
    }}
    .field {{
        display:flex;
        flex-direction:column;
        gap:8px;
    }}
    .label {{
        color:#d5b06a;
        font-size:15px;
    }}
    .input {{
        width:100%;
        padding:12px 14px;
        font-size:16px;
        color:#f2f0ea;
        background:#0d1b2b;
        border:1px solid #2d4257;
        border-radius:10px;
        line-height:1.5;
        box-sizing:border-box;
        font-family:Georgia, "Times New Roman", serif;
    }}
    .actions {{
        margin-top:22px;
        display:flex;
        gap:12px;
        flex-wrap:wrap;
    }}
    .button {{
        display:inline-block;
        padding:10px 18px;
        background:#173247;
        border:1px solid #b89457;
        border-radius:999px;
        text-decoration:none;
        color:#f2f0ea;
        font-family:Georgia, "Times New Roman", serif;
        font-size:16px;
        cursor:pointer;
    }}
    </style>
    </head>
    <body>
    <div class="page">
      <div class="card">
        <div class="header">
          <img src="/static/annotatio_logo.png">
          <div class="title">Edit Instruments</div>
          <div class="rule"></div>
        </div>

        <h2 class="section-title">Librarian role display</h2>

        <form method="post" action="/librarian/update_instruments/save?email={librarian_profile_escape_html(user_row['email'])}">
          <div class="grid">
            <div class="field">
              <div class="label">Primary display title</div>
              <input class="input" type="text" name="role_title" value="{role_title}" required>
            </div>

            <div class="field">
              <div class="label">Secondary display line</div>
              <input class="input" type="text" name="organisation_name" value="{organisation_name}">
            </div>
          </div>

          <div class="actions">
            <button class="button" type="submit">Save Instruments</button>
            <a class="button" href="/librarian/profile?email={librarian_profile_escape_html(user_row['email'])}">Back to Profile</a>
          </div>
        </form>
      </div>
    </div>
    </body>
    </html>
    """
    return HTMLResponse(page_html)


@app.post("/librarian/update_instruments/save")
def librarian_update_instruments_save(
    request: Request,
    role_title: str = Form(""),
    organisation_name: str = Form(""),
):
    email = librarian_route_get_email_from_request(request) or "librarian@local"
    user_row = librarian_profile_get_user_row_by_email(email)
    if not user_row:
        return HTMLResponse("Librarian not found.", status_code=404)

    librarian_profile_upsert_instrument_details(
        user_row=user_row,
        role_title=role_title,
        organisation_name=organisation_name,
    )

    return RedirectResponse(f"/librarian/profile?email={quote_plus(user_row['email'])}&saved=1", status_code=303)


@app.get("/librarian/add_ensemble", response_class=HTMLResponse)
def librarian_add_ensemble_page(request: Request):
    email = librarian_route_get_email_from_request(request) or "librarian@local"
    user_row = librarian_profile_get_user_row_by_email(email)

    if not user_row:
        conn = None
        try:
            conn = db()
            cur = conn.cursor()
            cur.execute("""
            SELECT
                u.id,
                u.name,
                u.email,
                u.role,
                u.organisation_id,
                COALESCE(o.name, '') AS organisation_name,
                COALESCE(o.city, '') AS organisation_city,
                COALESCE(o.country, '') AS organisation_country
            FROM users u
            LEFT JOIN organisations o ON o.id = u.organisation_id
            WHERE u.role='librarian'
            ORDER BY u.id ASC
            LIMIT 1
            """)
            user_row = cur.fetchone()
        finally:
            if conn is not None:
                conn.close()

    if not user_row:
        user_row = {
            "id": 0,
            "name": "Librarian",
            "email": email,
            "role": "librarian",
            "organisation_id": None,
            "organisation_name": "",
            "organisation_city": "",
            "organisation_country": "",
        }

    profile_row = librarian_profile_get_profile_row(int(user_row["id"]))
    profile = dict(profile_row) if profile_row else {}
    ensemble_name = librarian_profile_escape_html(profile.get("ensemble_name") or "")

    page_html = f"""
    <!doctype html>
    <html>
    <head>
    <meta charset="utf-8">
    <title>Annotatio • Edit Ensembles</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{
        margin:0;
        font-family: Georgia, "Times New Roman", serif;
        background:#071018;
        color:#f2f0ea;
    }}
    .page {{
        max-width:980px;
        margin:40px auto;
        padding:20px;
    }}
    .card {{
        background:#0b1622;
        border:1px solid #223548;
        border-radius:14px;
        padding:40px;
        box-shadow:0 10px 30px rgba(0,0,0,0.35);
    }}
    .header {{
        text-align:center;
        margin-bottom:30px;
    }}
    .header img {{
        width:420px;
        max-width:80%;
    }}
    .title {{
        font-size:34px;
        margin-top:10px;
        color:#74d3de;
    }}
    .rule {{
        height:1px;
        width:220px;
        background:#b89457;
        margin:18px auto 10px auto;
    }}
    .section-title {{
        font-size:22px;
        color:#74d3de;
        margin:0 0 18px 0;
    }}
    .label {{
        color:#d5b06a;
        font-size:15px;
        margin-bottom:8px;
    }}
    .textarea {{
        width:100%;
        min-height:180px;
        padding:12px 14px;
        font-size:16px;
        color:#f2f0ea;
        background:#0d1b2b;
        border:1px solid #2d4257;
        border-radius:10px;
        line-height:1.7;
        box-sizing:border-box;
        resize:vertical;
        font-family:Georgia, "Times New Roman", serif;
    }}
    .actions {{
        margin-top:22px;
        display:flex;
        gap:12px;
        flex-wrap:wrap;
    }}
    .button {{
        display:inline-block;
        padding:10px 18px;
        background:#173247;
        border:1px solid #b89457;
        border-radius:999px;
        text-decoration:none;
        color:#f2f0ea;
        font-family:Georgia, "Times New Roman", serif;
        font-size:16px;
        cursor:pointer;
    }}
    .help {{
        color:#c8d0d8;
        font-size:15px;
        line-height:1.6;
        margin-top:10px;
    }}
    </style>
    </head>
    <body>
    <div class="page">
      <div class="card">
        <div class="header">
          <img src="/static/annotatio_logo.png">
          <div class="title">Edit Ensembles</div>
          <div class="rule"></div>
        </div>

        <h2 class="section-title">Orchestras and ensembles</h2>

        <form method="post" action="/librarian/add_ensemble/save?email={librarian_profile_escape_html(user_row['email'])}">
          <div class="label">Comma separated ensembles</div>
          <textarea class="textarea" name="ensemble_name">{ensemble_name}</textarea>
          <div class="help">Enter each orchestra or ensemble separated by commas.</div>

          <div class="actions">
            <button class="button" type="submit">Save Ensembles</button>
            <a class="button" href="/librarian/profile?email={librarian_profile_escape_html(user_row['email'])}">Back to Profile</a>
          </div>
        </form>
      </div>
    </div>
    </body>
    </html>
    """
    return HTMLResponse(page_html)


@app.post("/librarian/add_ensemble/save")
def librarian_add_ensemble_save(
    request: Request,
    ensemble_name: str = Form(""),
):
    email = librarian_route_get_email_from_request(request) or "librarian@local"
    user_row = librarian_profile_get_user_row_by_email(email)
    if not user_row:
        return HTMLResponse("Librarian not found.", status_code=404)

    librarian_profile_upsert_ensemble_details(
        user_row=user_row,
        ensemble_name=ensemble_name,
    )

    return RedirectResponse(f"/librarian/profile?email={quote_plus(user_row['email'])}&saved=1", status_code=303)


# =====================================================================
# GLOBAL SEARCH (DISCOVERY ONLY)
# =====================================================================

@app.get("/musicians", response_class=HTMLResponse)
def global_musicians():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
        SELECT
            u.id,
            u.name,
            m.instrument,
            m.city,
            m.country
        FROM users u
        LEFT JOIN musicians m ON m.user_id = u.id
        WHERE u.role='musician'
        ORDER BY u.name
        """)

        rows = cur.fetchall()

        html_out = """
        <!doctype html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <title>Annotatio — Global Musicians</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
        </head>
        <body style="margin:0; background:#071018; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif;">
            <div style="max-width:1200px; margin:18px auto; padding:0 18px;">
                <div style="background:#0b1622; border:1px solid #223548; border-radius:18px; overflow:hidden;">
                    <div style="padding:18px 24px; text-align:center; border-bottom:1px solid #223548;">
                        <div style="font-size:34px; color:#74d3de;">Global Musicians</div>
                    </div>
                    <div style="padding:20px 24px 28px 24px;">
        """

        if rows:
            html_out += """
            <div style="display:grid; gap:12px;">
            """
            for row in rows:
                html_out += f"""
                <div style="background:#101e2d; border:1px solid #223548; border-radius:14px; padding:14px 16px;">
                    <div style="font-size:24px; color:#74d3de; margin-bottom:6px;">{row['name']}</div>
                    <div style="font-size:16px; color:#c8d0d8;">
                        Instrument: {row['instrument'] or '-'}<br>
                        City: {row['city'] or '-'}<br>
                        Country: {row['country'] or '-'}
                    </div>
                </div>
                """
            html_out += "</div>"
        else:
            html_out += """
            <div style="font-size:17px; color:#c8d0d8;">Global musician listing will appear here as the network expands..</div>
            """

        html_out += """
                        <div style="margin-top:18px;">
                            <a href="/librarian" style="display:inline-block; padding:10px 18px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; text-decoration:none; font-size:16px;">
                                Return to Librarian Page
                            </a>
                        </div>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """

        return HTMLResponse(html_out)
    finally:
        if conn is not None:
            conn.close()


# =====================================================================
# INVITE
# =====================================================================

@app.post("/invite")
def send_invite(
    organisation_id: int = Form(...),
    musician_user_id: int = Form(...),
    invited_by_user_id: int = Form(...)
):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
        SELECT
            COALESCE(country, '') AS country,
            COALESCE(name, '') AS name
        FROM organisations
        WHERE id=?
        """, (organisation_id,))
        organisation = cur.fetchone()

        if not organisation:
            return HTMLResponse("Organisation not found", status_code=404)

        organisation_country = (organisation["country"] or "").strip()
        if not organisation_country:
            return HTMLResponse("Invite blocked. No librarian country exists for this organisation yet.", status_code=400)

        organisation_name = (organisation["name"] or "").strip()
        default_ensemble_name = f"{organisation_name} Default Ensemble" if organisation_name else "Default Ensemble"

        cur.execute("""
        SELECT
            COALESCE(country, '') AS country,
            COALESCE(email, '') AS email,
            COALESCE(primary_instrument, instrument, '') AS primary_instrument
        FROM musicians
        WHERE user_id=?
        """, (musician_user_id,))
        musician = cur.fetchone()
        musician_country = ((musician["country"] if musician else "") or "").strip()
        invite_email = ((musician["email"] if musician else "") or "").strip().lower()
        target_section = ((musician["primary_instrument"] if musician else "") or "").strip()

        now = datetime.utcnow().isoformat()

        cur.execute("""
        INSERT INTO invites (
            organisation_id,
            musician_user_id,
            invited_by_user_id,
            organisation_country,
            musician_country,
            invite_email,
            target_ensemble_name,
            target_is_default_ensemble,
            target_section,
            status,
            created_at,
            invite_sent_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, 'pending', ?, ?)
        """, (
            organisation_id,
            musician_user_id,
            invited_by_user_id,
            organisation_country,
            musician_country,
            invite_email,
            default_ensemble_name,
            target_section,
            now,
            now,
        ))

        conn.commit()
    finally:
        if conn is not None:
            conn.close()

    return RedirectResponse("/musicians", status_code=303)


# =====================================================================
# ACCEPT INVITE
# =====================================================================

@app.post("/accept")
def accept_invite(invite_id: int = Form(...)):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("SELECT * FROM invites WHERE id=?", (invite_id,))
        invite = cur.fetchone()

        if not invite:
            return HTMLResponse("Invite not found", status_code=404)

        organisation_country = (invite["organisation_country"] or "").strip()
        if not organisation_country:
            return HTMLResponse("Invite blocked. No librarian country exists for this organisation yet.", status_code=400)

        musician_country = (invite["musician_country"] or "").strip()
        membership_type = "local_pool" if musician_country and musician_country == organisation_country else "international_invite"

        now = datetime.utcnow().isoformat()

        cur.execute("""
        INSERT OR IGNORE INTO organisation_memberships (
            organisation_id,
            musician_user_id,
            invited_by_user_id,
            organisation_country,
            musician_country,
            membership_type,
            status,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'accepted', ?)
        """, (
            invite["organisation_id"],
            invite["musician_user_id"],
            invite["invited_by_user_id"],
            organisation_country,
            musician_country,
            membership_type,
            now
        ))

        cur.execute("""
        UPDATE invites
        SET status='accepted', responded_at=?
        WHERE id=?
        """, (now, invite_id))

        cur.execute("""
        SELECT *
        FROM invites
        WHERE id=?
        LIMIT 1
        """, (invite_id,))
        updated_invite = cur.fetchone()

        if updated_invite:
            concert_receipt_upsert_from_invite(
                cur,
                updated_invite,
                accepted_at=now,
                confirmed_at=None,
            )

        conn.commit()
        return HTMLResponse("Accepted")
    finally:
        if conn is not None:
            conn.close()


# =====================================================================
# READY TO RECEIVE CONCERT LAYER — STANDALONE HELPERS
# ADD BELOW THE MUSICIAN INVITATION RESPONSE HELPERS
# =====================================================================

def concert_receipt_upsert_from_invite(
    cur: sqlite3.Cursor,
    invite_row: sqlite3.Row,
    accepted_at: str | None = None,
    confirmed_at: str | None = None,
):
    now = datetime.utcnow().isoformat()

    invite_id = int(invite_row["id"])
    invite_sent_at = (invite_row["invite_sent_at"] or invite_row["created_at"] or "").strip()
    invite_status = str(invite_row["status"] or "pending").strip().lower()

    cur.execute("""
    SELECT id
    FROM musician_concert_receipts
    WHERE invite_id=?
    LIMIT 1
    """, (invite_id,))
    existing = cur.fetchone()

    if existing:
        cur.execute("""
        UPDATE musician_concert_receipts
        SET organisation_id=?,
            musician_user_id=?,
            invited_by_user_id=?,
            invite_status=?,
            invite_sent_at=?,
            accepted_at=COALESCE(?, accepted_at),
            confirmed_at=COALESCE(?, confirmed_at),
            is_active=?,
            updated_at=?
        WHERE invite_id=?
        """, (
            invite_row["organisation_id"],
            invite_row["musician_user_id"],
            invite_row["invited_by_user_id"],
            invite_status,
            invite_sent_at or None,
            accepted_at,
            confirmed_at,
            1 if invite_status == "accepted" else 0,
            now,
            invite_id,
        ))
        return

    cur.execute("""
    INSERT INTO musician_concert_receipts (
        invite_id,
        organisation_id,
        musician_user_id,
        invited_by_user_id,
        concert_ref,
        concert_name,
        concert_date,
        current_file_label,
        current_file_token,
        current_file_sent_at,
        access_status,
        invite_status,
        invite_sent_at,
        accepted_at,
        confirmed_at,
        intro_knock_enabled,
        intro_knock_heard_at,
        is_active,
        created_at,
        updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        invite_id,
        invite_row["organisation_id"],
        invite_row["musician_user_id"],
        invite_row["invited_by_user_id"],
        None,
        None,
        None,
        None,
        None,
        None,
        "ready",
        invite_status,
        invite_sent_at or None,
        accepted_at,
        confirmed_at,
        1 if invite_status == "accepted" else 0,
        None,
        1 if invite_status == "accepted" else 0,
        now,
        now,
    ))


def concert_receipt_list_for_musician(cur: sqlite3.Cursor, musician_user_id: int):
    cur.execute("""
    SELECT
        r.id,
        r.invite_id,
        r.organisation_id,
        r.musician_user_id,
        r.invited_by_user_id,
        COALESCE(r.concert_ref, '') AS concert_ref,
        COALESCE(r.concert_name, 'Concert pending issue') AS concert_name,
        COALESCE(r.concert_date, '') AS concert_date,
        COALESCE(r.current_file_label, '') AS current_file_label,
        COALESCE(r.current_file_token, '') AS current_file_token,
        COALESCE(r.current_file_sent_at, '') AS current_file_sent_at,
        COALESCE(r.access_status, 'ready') AS access_status,
        COALESCE(r.invite_status, 'pending') AS invite_status,
        COALESCE(r.invite_sent_at, '') AS invite_sent_at,
        COALESCE(r.accepted_at, '') AS accepted_at,
        COALESCE(r.confirmed_at, '') AS confirmed_at,
        COALESCE(r.intro_knock_enabled, 0) AS intro_knock_enabled,
        COALESCE(r.intro_knock_heard_at, '') AS intro_knock_heard_at,
        COALESCE(r.is_active, 0) AS is_active,
        COALESCE(i.musician_comment, '') AS musician_comment,
        COALESCE(o.name, 'Unknown organisation') AS organisation_name
    FROM musician_concert_receipts r
    LEFT JOIN invites i ON i.id = r.invite_id
    LEFT JOIN organisations o ON o.id = r.organisation_id
    WHERE r.musician_user_id=?
    ORDER BY
        CASE
            WHEN COALESCE(r.is_active, 0) = 1 THEN 0
            ELSE 1
        END,
        COALESCE(r.concert_date, '') DESC,
        r.id DESC
    """, (musician_user_id,))
    return cur.fetchall()


def musician_online_activity_mark_seen(musician_user_id: int) -> None:
    now = datetime.utcnow().isoformat()

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO musician_online_activity (
            musician_user_id,
            last_seen_at
        ) VALUES (?, ?)
        ON CONFLICT(musician_user_id)
        DO UPDATE SET
            last_seen_at=excluded.last_seen_at
        """, (
            int(musician_user_id),
            now,
        ))
        conn.commit()
    finally:
        if conn is not None:
            conn.close()


def librarian_dashboard_connected_now_count(librarian_email: str, active_window_minutes: int = 15) -> int:
    email = str(librarian_email or "").strip().lower()
    if not email:
        return 0

    try:
        clean_window_minutes = int(active_window_minutes)
    except Exception:
        clean_window_minutes = 15

    if clean_window_minutes < 1:
        clean_window_minutes = 1

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        cur.execute("""
        SELECT u.organisation_id
        FROM users u
        WHERE lower(u.email)=lower(?)
          AND u.role='librarian'
        LIMIT 1
        """, (email,))
        librarian_row = cur.fetchone()

        if not librarian_row:
            return 0

        organisation_id = librarian_row["organisation_id"]
        if organisation_id is None:
            return 0

        cur.execute("""
        SELECT COUNT(DISTINCT moa.musician_user_id) AS total
        FROM musician_online_activity moa
        JOIN organisation_memberships om
          ON om.musician_user_id = moa.musician_user_id
        WHERE om.organisation_id=?
          AND lower(COALESCE(om.status, ''))='accepted'
          AND datetime(COALESCE(moa.last_seen_at, '')) >= datetime('now', ?)
        """, (
            int(organisation_id),
            f"-{clean_window_minutes} minutes",
        ))
        row = cur.fetchone()
        return int(row["total"] or 0) if row else 0
    finally:
        if conn is not None:
            conn.close()


# =====================================================================
# MUSICIAN SETUP — STANDALONE BLOCK
# PURPOSE: FIRST-TIME MUSICIAN FRONT DOOR ONLY
# =====================================================================

MUSICIAN_SETUP_COUNTRY_CHOICES = [
    ("AR", "Argentina"),
    ("AU", "Australia"),
    ("AT", "Austria"),
    ("BE", "Belgium"),
    ("BR", "Brazil"),
    ("BG", "Bulgaria"),
    ("CA", "Canada"),
    ("CL", "Chile"),
    ("CN", "China"),
    ("CO", "Colombia"),
    ("HR", "Croatia"),
    ("CY", "Cyprus"),
    ("CZ", "Czech Republic"),
    ("DK", "Denmark"),
    ("EE", "Estonia"),
    ("FI", "Finland"),
    ("FR", "France"),
    ("DE", "Germany"),
    ("GR", "Greece"),
    ("HK", "Hong Kong"),
    ("HU", "Hungary"),
    ("IS", "Iceland"),
    ("IN", "India"),
    ("ID", "Indonesia"),
    ("IE", "Ireland"),
    ("IL", "Israel"),
    ("IT", "Italy"),
    ("JP", "Japan"),
    ("LV", "Latvia"),
    ("LT", "Lithuania"),
    ("LU", "Luxembourg"),
    ("MY", "Malaysia"),
    ("MT", "Malta"),
    ("MX", "Mexico"),
    ("NL", "Netherlands"),
    ("NZ", "New Zealand"),
    ("NO", "Norway"),
    ("PH", "Philippines"),
    ("PL", "Poland"),
    ("PT", "Portugal"),
    ("RO", "Romania"),
    ("RS", "Serbia"),
    ("SG", "Singapore"),
    ("SK", "Slovakia"),
    ("SI", "Slovenia"),
    ("ZA", "South Africa"),
    ("KR", "South Korea"),
    ("ES", "Spain"),
    ("SE", "Sweden"),
    ("CH", "Switzerland"),
    ("TW", "Taiwan"),
    ("TH", "Thailand"),
    ("TR", "Turkey"),
    ("UA", "Ukraine"),
    ("AE", "United Arab Emirates"),
    ("GB", "United Kingdom"),
    ("US", "United States"),
    ("UY", "Uruguay"),
    ("VN", "Vietnam"),
]

MUSICIAN_SETUP_COUNTRY_CODE_TO_NAME = {
    code: name for code, name in MUSICIAN_SETUP_COUNTRY_CHOICES
}

MUSICIAN_SETUP_COUNTRY_NAME_TO_CODE = {
    name.lower(): code for code, name in MUSICIAN_SETUP_COUNTRY_CHOICES
}


def musician_setup_clean_text(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def musician_setup_clean_multiline(value: str) -> str:
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def musician_setup_parse_ensembles(value: str) -> str:
    cleaned = []
    for part in str(value or "").split(","):
        item = musician_setup_clean_text(part)
        if item and item not in cleaned:
            cleaned.append(item)
    return ", ".join(cleaned)


def musician_setup_ensure_musicians_columns(cur: sqlite3.Cursor) -> None:
    cur.execute("PRAGMA table_info(musicians)")
    musician_cols = {row["name"] for row in cur.fetchall()}

    if "email" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN email TEXT")

    if "name" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN name TEXT")

    if "preferred_name" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN preferred_name TEXT")

    if "mobile" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN mobile TEXT")

    if "state_region_territory" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN state_region_territory TEXT")

    if "country_code" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN country_code TEXT")

    if "country_name" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN country_name TEXT")

    if "ensembles" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN ensembles TEXT")

    if "primary_instrument" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN primary_instrument TEXT")

    if "voice_type" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN voice_type TEXT")

    if "other_instruments" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN other_instruments TEXT")

    if "notes" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN notes TEXT")

    if "password_hash" not in musician_cols:
        cur.execute("ALTER TABLE musicians ADD COLUMN password_hash TEXT")


@app.get("/musician_setup", response_class=HTMLResponse)
def musician_setup_page(request: Request):
    template_file = TEMPLATES_DIR / "musician_setup.html"
    if not template_file.exists():
        return HTMLResponse("templates/musician_setup.html not found.", status_code=404)

    return templates.TemplateResponse(
        "musician_setup.html",
        {
            "request": request,
            "country_choices": MUSICIAN_SETUP_COUNTRY_CHOICES,
        },
    )


@app.post("/musician_setup")
def musician_setup_submit(
    request: Request,
    full_name: str = Form(...),
    preferred_name: str = Form(""),
    confirm_email: str = Form(...),
    mobile: str = Form(""),
    country_code: str = Form(""),
    country_name: str = Form(""),
    state_region_territory: str = Form(""),
    city: str = Form(""),
    ensembles: str = Form(""),
    primary_instrument: str = Form(...),
    primary_other: str = Form(""),
    voice_type: str = Form(""),
    other_instruments: list[str] = Form([], alias="other_instruments[]"),
    other_instruments_other: str = Form(""),
    ensemble_notes: str = Form(""),
    password: str = Form(...),
    confirm_password: str = Form(...),
):
    invited_email = musician_setup_clean_text(request.query_params.get("email") or "").lower()
    submitted_email = musician_setup_clean_text(confirm_email or "").lower()

    if not submitted_email:
        return RedirectResponse(
            f"/musician_setup?email={quote_plus(invited_email)}&error=missing_email",
            status_code=303,
        )

    if invited_email and submitted_email != invited_email:
        return RedirectResponse(
            f"/musician_setup?email={quote_plus(invited_email)}&error=email_mismatch",
            status_code=303,
        )

    if str(password or "") != str(confirm_password or ""):
        return RedirectResponse(
            f"/musician_setup?email={quote_plus(submitted_email)}&error=password_mismatch",
            status_code=303,
        )

    clean_full_name = musician_setup_clean_text(full_name or "")
    clean_preferred_name = musician_setup_clean_text(preferred_name or "")
    clean_mobile = musician_setup_clean_text(mobile or "")
    clean_country_code = musician_setup_clean_text(country_code or "").upper()
    clean_country_name = musician_setup_clean_text(country_name or "")
    clean_state = musician_setup_clean_text(state_region_territory or "")
    clean_city = musician_setup_clean_text(city or "")
    clean_ensembles = musician_setup_parse_ensembles(ensembles or "")
    clean_voice_type = musician_setup_clean_text(voice_type or "")
    clean_primary_instrument = musician_setup_clean_text(primary_instrument or "")
    clean_primary_other = musician_setup_clean_text(primary_other or "")
    clean_other_instruments_other = musician_setup_clean_text(other_instruments_other or "")
    clean_notes = musician_setup_clean_multiline(ensemble_notes or "")

    if clean_primary_instrument == "Other":
        clean_primary_instrument = clean_primary_other

    if clean_country_name and not clean_country_code:
        clean_country_code = MUSICIAN_SETUP_COUNTRY_NAME_TO_CODE.get(clean_country_name.lower(), "")

    if clean_country_code and not clean_country_name:
        clean_country_name = MUSICIAN_SETUP_COUNTRY_CODE_TO_NAME.get(clean_country_code, "")

    canonical_country_name = MUSICIAN_SETUP_COUNTRY_CODE_TO_NAME.get(clean_country_code, "")
    if canonical_country_name:
        clean_country_name = canonical_country_name

    cleaned_other_instruments = []
    for item in other_instruments:
        clean_item = musician_setup_clean_text(item or "")
        if not clean_item:
            continue
        if clean_item == "Other":
            continue
        if clean_item == clean_primary_instrument:
            continue
        if clean_item not in cleaned_other_instruments:
            cleaned_other_instruments.append(clean_item)

    if (
        clean_other_instruments_other
        and clean_other_instruments_other != clean_primary_instrument
        and clean_other_instruments_other not in cleaned_other_instruments
    ):
        cleaned_other_instruments.append(clean_other_instruments_other)

    password_hash = __import__("hashlib").sha256(str(password or "").encode("utf-8")).hexdigest()
    musician_country_value = clean_country_code or clean_country_name
    saved_ensemble_names = []

    if clean_ensembles:
        for part in clean_ensembles.split(","):
            item = musician_setup_clean_text(part)
            if item and item not in saved_ensemble_names:
                saved_ensemble_names.append(item)

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        musician_setup_ensure_musicians_columns(cur)

        cur.execute(
            """
            SELECT id, organisation_id
            FROM users
            WHERE lower(email)=lower(?)
              AND role='musician'
            LIMIT 1
            """,
            (submitted_email,),
        )
        user_row = cur.fetchone()

        if user_row:
            user_id = int(user_row["id"])
            cur.execute(
                """
                UPDATE users
                SET name=?
                WHERE id=?
                """,
                (
                    clean_preferred_name or clean_full_name or submitted_email,
                    user_id,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO users (
                    name,
                    email,
                    role,
                    organisation_id
                ) VALUES (?, ?, 'musician', NULL)
                """,
                (
                    clean_preferred_name or clean_full_name or submitted_email,
                    submitted_email,
                ),
            )
            user_id = int(cur.lastrowid)

        cur.execute(
            """
            SELECT id
            FROM musicians
            WHERE user_id=?
               OR lower(trim(COALESCE(email, '')))=lower(?)
            ORDER BY
                CASE WHEN user_id=? THEN 0 ELSE 1 END,
                id ASC
            LIMIT 1
            """,
            (user_id, submitted_email, user_id),
        )
        musician_row = cur.fetchone()

        if musician_row:
            musician_record_id = int(musician_row["id"])

            cur.execute(
                """
                SELECT id, COALESCE(user_id, 0) AS linked_user_id
                FROM musicians
                WHERE lower(trim(COALESCE(email, '')))=lower(?)
                ORDER BY
                    CASE WHEN id=? THEN 0 ELSE 1 END,
                    id ASC
                """,
                (
                    submitted_email,
                    musician_record_id,
                ),
            )
            duplicate_musician_rows = cur.fetchall()

            duplicate_musician_ids = []
            duplicate_user_ids = []

            for duplicate_row in duplicate_musician_rows:
                duplicate_id = int(duplicate_row["id"] or 0)
                duplicate_linked_user_id = int(duplicate_row["linked_user_id"] or 0)

                if duplicate_id and duplicate_id != musician_record_id:
                    duplicate_musician_ids.append(duplicate_id)

                if (
                    duplicate_linked_user_id
                    and duplicate_linked_user_id != user_id
                    and duplicate_linked_user_id not in duplicate_user_ids
                ):
                    duplicate_user_ids.append(duplicate_linked_user_id)

            cur.execute(
                """
                UPDATE invites
                SET musician_user_id=?,
                    invite_email=?
                WHERE lower(trim(COALESCE(invite_email, '')))=lower(?)
                   OR musician_user_id=?
                """,
                (
                    user_id,
                    submitted_email,
                    submitted_email,
                    user_id,
                ),
            )

            for duplicate_user_id in duplicate_user_ids:
                cur.execute(
                    """
                    UPDATE invites
                    SET musician_user_id=?,
                        invite_email=?
                    WHERE musician_user_id=?
                    """,
                    (
                        user_id,
                        submitted_email,
                        duplicate_user_id,
                    ),
                )

                cur.execute(
                    """
                    UPDATE organisation_memberships
                    SET musician_user_id=?
                    WHERE musician_user_id=?
                    """,
                    (
                        user_id,
                        duplicate_user_id,
                    ),
                )

                cur.execute(
                    """
                    UPDATE organisation_default_ensemble_members
                    SET musician_user_id=?
                    WHERE musician_user_id=?
                    """,
                    (
                        user_id,
                        duplicate_user_id,
                    ),
                )

            for duplicate_musician_id in duplicate_musician_ids:
                cur.execute(
                    """
                    DELETE FROM musicians
                    WHERE id=?
                    """,
                    (duplicate_musician_id,),
                )

            cur.execute(
                """
                UPDATE musicians
                SET
                    user_id=?,
                    email=?,
                    name=?,
                    preferred_name=?,
                    mobile=?,
                    city=?,
                    state_region_territory=?,
                    country=?,
                    country_code=?,
                    country_name=?,
                    ensembles=?,
                    instrument=?,
                    primary_instrument=?,
                    voice_type=?,
                    other_instruments=?,
                    notes=?,
                    password_hash=?
                WHERE id=?
                """,
                (
                    user_id,
                    submitted_email,
                    clean_full_name or submitted_email,
                    clean_preferred_name,
                    clean_mobile,
                    clean_city,
                    clean_state,
                    clean_country_name,
                    clean_country_code,
                    clean_country_name,
                    clean_ensembles,
                    clean_primary_instrument,
                    clean_primary_instrument,
                    clean_voice_type,
                    ", ".join(cleaned_other_instruments),
                    clean_notes,
                    password_hash,
                    musician_record_id,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO musicians (
                    user_id,
                    email,
                    name,
                    preferred_name,
                    mobile,
                    city,
                    state_region_territory,
                    country,
                    country_code,
                    country_name,
                    ensembles,
                    instrument,
                    primary_instrument,
                    voice_type,
                    other_instruments,
                    notes,
                    password_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    submitted_email,
                    clean_full_name or submitted_email,
                    clean_preferred_name,
                    clean_mobile,
                    clean_city,
                    clean_state,
                    clean_country_name,
                    clean_country_code,
                    clean_country_name,
                    clean_ensembles,
                    clean_primary_instrument,
                    clean_primary_instrument,
                    clean_voice_type,
                    ", ".join(cleaned_other_instruments),
                    clean_notes,
                    password_hash,
                ),
            )
            musician_record_id = int(cur.lastrowid)

        cur.execute(
            """
            SELECT
                i.*,
                COALESCE(o.name, '') AS organisation_name
            FROM invites i
            LEFT JOIN organisations o ON o.id = i.organisation_id
            WHERE (
                    lower(trim(COALESCE(i.invite_email, '')))=lower(?)
                 OR i.musician_user_id=?
            )
              AND lower(trim(COALESCE(i.status, 'pending')))='pending'
            ORDER BY i.id ASC
            """,
            (submitted_email, user_id),
        )
        pending_invites = cur.fetchall()

        for invite in pending_invites:
            organisation_name = str(invite["organisation_name"] or "").strip()
            organisation_country = str(invite["organisation_country"] or "").strip()
            default_ensemble_name = str(invite["target_ensemble_name"] or "").strip()
            if not default_ensemble_name:
                if organisation_name:
                    default_ensemble_name = f"{organisation_name} Default Ensemble"
                else:
                    default_ensemble_name = "Default Ensemble"

            target_section = str(invite["target_section"] or "").strip() or clean_primary_instrument

            membership_type = "international_invite"
            if musician_country_value and organisation_country and musician_country_value == organisation_country:
                membership_type = "local_pool"

            cur.execute(
                """
                INSERT OR IGNORE INTO organisation_memberships (
                    organisation_id,
                    musician_user_id,
                    invited_by_user_id,
                    organisation_country,
                    musician_country,
                    membership_type,
                    status,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'accepted', ?)
                """,
                (
                    invite["organisation_id"],
                    user_id,
                    invite["invited_by_user_id"],
                    organisation_country,
                    musician_country_value,
                    membership_type,
                    datetime.utcnow().isoformat(),
                ),
            )

            cur.execute(
                """
                UPDATE organisation_memberships
                SET
                    invited_by_user_id=?,
                    organisation_country=?,
                    musician_country=?,
                    membership_type=?,
                    status='accepted'
                WHERE organisation_id=?
                  AND musician_user_id=?
                """,
                (
                    invite["invited_by_user_id"],
                    organisation_country,
                    musician_country_value,
                    membership_type,
                    invite["organisation_id"],
                    user_id,
                ),
            )

            cur.execute(
                """
                INSERT OR IGNORE INTO organisation_default_ensemble_members (
                    organisation_id,
                    ensemble_name,
                    musician_user_id,
                    instrument,
                    role_title,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    invite["organisation_id"],
                    default_ensemble_name,
                    user_id,
                    target_section,
                    target_section,
                    datetime.utcnow().isoformat(),
                ),
            )

            cur.execute(
                """
                UPDATE organisation_default_ensemble_members
                SET instrument=?,
                    role_title=?
                WHERE organisation_id=?
                  AND musician_user_id=?
                  AND lower(trim(COALESCE(ensemble_name, '')))=lower(?)
                """,
                (
                    target_section,
                    target_section,
                    invite["organisation_id"],
                    user_id,
                    default_ensemble_name,
                ),
            )

            cur.execute(
                """
                UPDATE invites
                SET
                    musician_user_id=?,
                    musician_country=?,
                    invite_email=?,
                    target_ensemble_name=?,
                    target_is_default_ensemble=1,
                    target_section=?,
                    status='accepted',
                    responded_at=?,
                    confirmed_at=NULL
                WHERE id=?
                """,
                (
                    user_id,
                    musician_country_value,
                    submitted_email,
                    default_ensemble_name,
                    target_section,
                    datetime.utcnow().isoformat(),
                    invite["id"],
                ),
            )

            cur.execute(
                """
                SELECT *
                FROM invites
                WHERE id=?
                LIMIT 1
                """,
                (invite["id"],),
            )
            updated_invite = cur.fetchone()

            if updated_invite:
                concert_receipt_upsert_from_invite(
                    cur,
                    updated_invite,
                    accepted_at=datetime.utcnow().isoformat(),
                    confirmed_at=None,
                )

            if default_ensemble_name and default_ensemble_name not in saved_ensemble_names:
                saved_ensemble_names.append(default_ensemble_name)

        final_ensembles = ", ".join(saved_ensemble_names)

        cur.execute(
            """
            UPDATE musicians
            SET ensembles=?
            WHERE id=?
            """,
            (
                final_ensembles,
                musician_record_id,
            ),
        )

        conn.commit()
    finally:
        if conn is not None:
            conn.close()

    return RedirectResponse(f"/musician?email={quote_plus(submitted_email)}", status_code=303)


# =====================================================================
# MUSICIAN PROFILE — STANDALONE BLOCK
# PURPOSE: MUSICIAN PROFILE PAGE ONLY
# =====================================================================

@app.get("/musician/profile", response_class=HTMLResponse)
def musician_profile_page(request: Request):
    template_file = TEMPLATES_DIR / "musician_profile.html"
    if not template_file.exists():
        return HTMLResponse("templates/musician_profile.html not found.", status_code=404)

    email = str(request.query_params.get("email") or "").strip().lower()

    if not email:
        referer = str(request.headers.get("referer") or "").strip()
        if referer:
            try:
                parsed = urlparse(referer)
                email = str(parse_qs(parsed.query).get("email", [""])[0] or "").strip().lower()
            except Exception:
                email = ""

    if not email:
        email = "musician@local"

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        cur.execute("""
        PRAGMA table_info(musicians)
        """)
        musician_cols = {row["name"] for row in cur.fetchall()}

        select_parts = [
            "m.id",
            "m.user_id",
            "COALESCE(m.name, u.name, '') AS name",
            "COALESCE(m.email, u.email, '') AS email",
            "COALESCE(m.city, '') AS city",
            "COALESCE(m.country, '') AS country",
            "COALESCE(m.instrument, '') AS instrument",
        ]

        optional_columns = {
            "preferred_name": "COALESCE(m.preferred_name, '') AS preferred_name",
            "mobile": "COALESCE(m.mobile, '') AS mobile",
            "state_region_territory": "COALESCE(m.state_region_territory, '') AS state_region_territory",
            "country_code": "COALESCE(m.country_code, '') AS country_code",
            "country_name": "COALESCE(m.country_name, '') AS country_name",
            "ensembles": "COALESCE(m.ensembles, '') AS ensembles",
            "primary_instrument": "COALESCE(m.primary_instrument, '') AS primary_instrument",
            "voice_type": "COALESCE(m.voice_type, '') AS voice_type",
            "other_instruments": "COALESCE(m.other_instruments, '') AS other_instruments",
            "notes": "COALESCE(m.notes, '') AS notes",
        }

        for column_name, expression in optional_columns.items():
            if column_name in musician_cols:
                select_parts.append(expression)
            else:
                select_parts.append(f"'' AS {column_name}")

        cur.execute(f"""
        SELECT
            {", ".join(select_parts)}
        FROM users u
        LEFT JOIN musicians m ON m.user_id = u.id
        WHERE lower(u.email)=lower(?)
          AND u.role='musician'
        LIMIT 1
        """, (email,))
        row = cur.fetchone()

        if row:
            musician = dict(row)
            if not str(musician.get("primary_instrument") or "").strip():
                musician["primary_instrument"] = str(musician.get("instrument") or "").strip()
            user_email = str(musician.get("email") or email).strip() or email
        else:
            musician = {
                "name": "Musician",
                "preferred_name": "",
                "email": email,
                "mobile": "",
                "city": "",
                "state_region_territory": "",
                "country": "",
                "country_code": "",
                "country_name": "",
                "ensembles": "",
                "instrument": "",
                "primary_instrument": "",
                "voice_type": "",
                "other_instruments": "",
                "notes": "",
            }
            user_email = email

        return templates.TemplateResponse(
            "musician_profile.html",
            {
                "request": request,
                "user": {"email": user_email},
                "musician": musician,
            },
        )
    finally:
        if conn is not None:
            conn.close()


# =====================================================================
# CONDUCTOR PROFILE — STANDALONE BLOCK
# PURPOSE: CONDUCTOR GLOBAL PROFILE PAGE ONLY
# =====================================================================

def conductor_profile_clean_text(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def conductor_profile_get_user_row_by_email(email: str):
    clean_email = str(email or "").strip().lower()
    if not clean_email:
        return None

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT
            u.id,
            COALESCE(u.name, '') AS name,
            COALESCE(u.email, '') AS email,
            COALESCE(u.role, '') AS role
        FROM users u
        WHERE lower(u.email)=lower(?)
          AND u.role='conductor'
        LIMIT 1
        """, (clean_email,))
        return cur.fetchone()
    finally:
        if conn is not None:
            conn.close()


def conductor_profile_get_profile_row(user_id: int):
    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT *
        FROM conductor_profiles
        WHERE user_id=?
        LIMIT 1
        """, (int(user_id),))
        return cur.fetchone()
    finally:
        if conn is not None:
            conn.close()


def conductor_profile_build_template_payload(user_row, profile_row) -> tuple[dict, dict]:
    profile = dict(profile_row) if profile_row else {}

    full_name = conductor_profile_clean_text(profile.get("full_name") or user_row["name"] or "")
    preferred_name = conductor_profile_clean_text(profile.get("preferred_name") or "")
    mobile = conductor_profile_clean_text(profile.get("mobile") or "")
    notes = str(profile.get("notes") or "").strip()
    city = conductor_profile_clean_text(profile.get("city") or "")
    state_region_territory = conductor_profile_clean_text(profile.get("state_region_territory") or "")
    country_name = conductor_profile_clean_text(profile.get("country_name") or "")
    work_email = conductor_profile_clean_text(profile.get("work_email") or user_row["email"] or "")
    working_area = conductor_profile_clean_text(profile.get("working_area") or "")
    career_stage = conductor_profile_clean_text(profile.get("career_stage") or "")
    known_for = conductor_profile_clean_text(profile.get("known_for") or "")
    production_types = conductor_profile_clean_text(profile.get("production_types") or "")
    global_search_visible = conductor_profile_clean_text(profile.get("global_search_visible") or "Yes")

    conductor = {
        "name": full_name or user_row["email"],
        "preferred_name": preferred_name,
        "email": str(user_row["email"] or "").strip(),
        "mobile": mobile,
        "city": city,
        "state_region_territory": state_region_territory,
        "country_name": country_name,
        "notes": notes,
        "primary_instrument": "Conductor",
    }

    world_profile = {
        "working_area": working_area,
        "career_stage": career_stage,
        "production_types": production_types,
        "known_for": known_for,
        "work_email": work_email,
        "global_search_visible": global_search_visible,
    }

    return conductor, world_profile


@app.get("/conductor/profile", response_class=HTMLResponse)
def conductor_profile_page(request: Request):
    template_file = TEMPLATES_DIR / "conductor_profile.html"
    if not template_file.exists():
        return HTMLResponse("templates/conductor_profile.html not found.", status_code=404)

    email = str(request.query_params.get("email") or "").strip().lower()

    if not email:
        referer = str(request.headers.get("referer") or "").strip()
        if referer:
            try:
                parsed = urlparse(referer)
                email = str(parse_qs(parsed.query).get("email", [""])[0] or "").strip().lower()
            except Exception:
                email = ""

    if not email:
        email = "conductor@local"

    user_row = conductor_profile_get_user_row_by_email(email)

    if user_row:
        profile_row = conductor_profile_get_profile_row(int(user_row["id"]))
        conductor, profile = conductor_profile_build_template_payload(user_row, profile_row)
        user_email = str(user_row["email"] or email).strip()
    else:
        conductor = {
            "name": "Conductor",
            "preferred_name": "",
            "email": email,
            "mobile": "",
            "city": "",
            "state_region_territory": "",
            "country_name": "",
            "notes": "",
            "primary_instrument": "Conductor",
        }
        profile = {
            "working_area": "",
            "career_stage": "",
            "production_types": "",
            "known_for": "",
            "work_email": email,
            "global_search_visible": "Yes",
        }
        user_email = email

    return templates.TemplateResponse(
        "conductor_profile.html",
        {
            "request": request,
            "user": {"email": user_email},
            "conductor": conductor,
            "profile": profile,
        },
    )

# =====================================================================
# MUSICIAN INVITATION RESPONSE SYSTEM — STANDALONE BLOCK
# =====================================================================

def musician_response_get_email_from_request(request: Request) -> str:
    email = str(request.query_params.get("email") or "").strip().lower()
    if email:
        return email

    referer = str(request.headers.get("referer") or "").strip()
    if not referer:
        return ""

    try:
        parsed = urlparse(referer)
        return str(parse_qs(parsed.query).get("email", [""])[0] or "").strip().lower()
    except Exception:
        return ""


def musician_response_get_user_by_email(email: str):
    email = str(email or "").strip().lower()
    if not email:
        return None

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT *
        FROM users
        WHERE lower(email)=lower(?)
          AND role='musician'
        LIMIT 1
        """, (email,))
        return cur.fetchone()
    finally:
        if conn is not None:
            conn.close()


def musician_response_get_latest_invite(cur: sqlite3.Cursor, musician_user_id: int):
    cur.execute("""
    SELECT *
    FROM invites
    WHERE musician_user_id=?
    ORDER BY
        COALESCE(invite_sent_at, created_at) DESC,
        id DESC
    LIMIT 1
    """, (musician_user_id,))
    return cur.fetchone()


def musician_response_get_invite_by_id(
    cur: sqlite3.Cursor,
    musician_user_id: int,
    invite_id: int,
):
    cur.execute("""
    SELECT *
    FROM invites
    WHERE id=?
      AND musician_user_id=?
    LIMIT 1
    """, (
        invite_id,
        musician_user_id,
    ))
    return cur.fetchone()


def musician_response_redirect(email: str):
    safe_email = quote_plus(str(email or "").strip())
    return RedirectResponse(f"/musician?email={safe_email}", status_code=303)


@app.get("/musician", response_class=HTMLResponse)
def musician_hub(request: Request):
    musician_file = TEMPLATES_DIR / "musician.html"
    if not musician_file.exists():
        return HTMLResponse("templates/musician.html not found.", status_code=404)

    email = musician_response_get_email_from_request(request)
    if not email:
        return HTMLResponse("Musician email missing.", status_code=400)

    user = musician_response_get_user_by_email(email)
    if not user:
        return HTMLResponse("Musician not found.", status_code=404)

    musician_online_activity_mark_seen(int(user["id"]))

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        concert_receipts = concert_receipt_list_for_musician(cur, int(user["id"]))
        intro_knock_receipt_ids = musician_intro_knock_receipt_ids(concert_receipts)

        return templates.TemplateResponse(
            "musician.html",
            {
                "request": request,
                "user": {"email": user["email"]},
                "concert_receipts": concert_receipts,
                "intro_knock_receipt_ids": intro_knock_receipt_ids,
                "intro_knock_audio_url": "/static/3_knocks.mp3",
            },
        )
    finally:
        if conn is not None:
            conn.close()


# =====================================================================
# REPLACE musician_invitation_respond WITH THIS VERSION
# =====================================================================

@app.post("/musician/invitation/respond")
def musician_invitation_respond(
    request: Request,
    invite_id: int = Form(...),
    response_action: str = Form(...),
    comment: str = Form(""),
):
    email = musician_response_get_email_from_request(request)
    if not email:
        return HTMLResponse("Musician email missing.", status_code=400)

    user = musician_response_get_user_by_email(email)
    if not user:
        return HTMLResponse("Musician not found.", status_code=404)

    action = str(response_action or "").strip().lower()
    if action not in {"accept", "decline", "away"}:
        return HTMLResponse("Invalid response action.", status_code=400)

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        invite = musician_response_get_invite_by_id(
            cur,
            int(user["id"]),
            int(invite_id),
        )

        if not invite:
            return HTMLResponse("Invite not found.", status_code=404)

        if action == "accept" and str(invite["status"] or "").strip().lower() == "accepted":
            return musician_response_redirect(email)

        now = datetime.utcnow().isoformat()

        if action == "accept":
            organisation_country = (invite["organisation_country"] or "").strip()
            musician_country = (invite["musician_country"] or "").strip()
            membership_type = "local_pool" if musician_country and musician_country == organisation_country else "international_invite"

            cur.execute("""
            INSERT OR IGNORE INTO organisation_memberships (
                organisation_id,
                musician_user_id,
                invited_by_user_id,
                organisation_country,
                musician_country,
                membership_type,
                status,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'accepted', ?)
            """, (
                invite["organisation_id"],
                invite["musician_user_id"],
                invite["invited_by_user_id"],
                organisation_country,
                musician_country,
                membership_type,
                now
            ))

        cur.execute("""
        UPDATE invites
        SET status=?,
            musician_comment=?,
            responded_at=?,
            confirmed_at=NULL
        WHERE id=?
        """, (
            action,
            str(comment or "").strip(),
            now,
            invite["id"],
        ))

        cur.execute("""
        SELECT *
        FROM invites
        WHERE id=?
        LIMIT 1
        """, (invite["id"],))
        updated_invite = cur.fetchone()

        if updated_invite and action == "accept":
            concert_receipt_upsert_from_invite(
                cur,
                updated_invite,
                accepted_at=now,
                confirmed_at=None,
            )

        if updated_invite and action in {"decline", "away"}:
            concert_receipt_upsert_from_invite(
                cur,
                updated_invite,
                accepted_at=None,
                confirmed_at=None,
            )

        conn.commit()
    finally:
        if conn is not None:
            conn.close()

    return musician_response_redirect(email)


# =====================================================================
# REPLACE musician_confirm WITH THIS VERSION
# =====================================================================

@app.post("/musician/confirm")
def musician_confirm(
    request: Request,
    invite_id: int = Form(...),
):
    email = musician_response_get_email_from_request(request)
    if not email:
        return HTMLResponse("Musician email missing.", status_code=400)

    user = musician_response_get_user_by_email(email)
    if not user:
        return HTMLResponse("Musician not found.", status_code=404)

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        invite = musician_response_get_invite_by_id(
            cur,
            int(user["id"]),
            int(invite_id),
        )

        if not invite:
            return HTMLResponse("Invite not found.", status_code=404)

        now = datetime.utcnow().isoformat()

        cur.execute("""
        UPDATE invites
        SET confirmed_at=?
        WHERE id=?
        """, (
            now,
            invite["id"],
        ))

        cur.execute("""
        SELECT *
        FROM invites
        WHERE id=?
        LIMIT 1
        """, (invite["id"],))
        updated_invite = cur.fetchone()

        if updated_invite:
            concert_receipt_upsert_from_invite(
                cur,
                updated_invite,
                accepted_at=(updated_invite["responded_at"] or "").strip() or None,
                confirmed_at=now,
            )

        conn.commit()
    finally:
        if conn is not None:
            conn.close()

    return musician_response_redirect(email)


@app.post("/musician/intro_knock/heard")
def musician_intro_knock_heard(
    request: Request,
    receipt_id: int = Form(...),
):
    email = musician_response_get_email_from_request(request)
    if not email:
        return HTMLResponse("Musician email missing.", status_code=400)

    user = musician_response_get_user_by_email(email)
    if not user:
        return HTMLResponse("Musician not found.", status_code=404)

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        heard_at = datetime.utcnow().isoformat()

        cur.execute("""
        UPDATE musician_concert_receipts
        SET intro_knock_enabled=0,
            intro_knock_heard_at=?,
            updated_at=?
        WHERE id=?
          AND musician_user_id=?
          AND COALESCE(intro_knock_enabled, 0)=1
          AND COALESCE(intro_knock_heard_at, '')=''
        """, (
            heard_at,
            heard_at,
            int(receipt_id),
            int(user["id"]),
        ))
        conn.commit()
    finally:
        if conn is not None:
            conn.close()

    return HTMLResponse("OK")


def conductor_draft_files_list(
    email: str,
    concert_name: str = "",
    concert_ref: str = "",
):
    clean_email = str(email or "").strip().lower()
    clean_concert_name = str(concert_name or "").strip()
    clean_concert_ref = str(concert_ref or "").strip()

    if not clean_email or not clean_concert_name:
        return []

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT
            id,
            COALESCE(concert_name, '') AS concert_name,
            COALESCE(concert_ref, '') AS concert_ref,
            COALESCE(upload_filename, '') AS upload_filename,
            COALESCE(upload_stored_path, '') AS upload_stored_path,
            COALESCE(upload_timestamp, '') AS upload_timestamp,
            COALESCE(submit_mode, '') AS submit_mode,
            COALESCE(receipt_status, 'draft bundle') AS receipt_status,
            COALESCE(forward_status, 'idle') AS forward_status,
            COALESCE(cycle_state, 'draft') AS cycle_state,
            COALESCE(message_note, '') AS message_note,
            COALESCE(score_note, '') AS score_note,
            COALESCE(batch_token, '') AS batch_token,
            COALESCE(created_at, '') AS created_at,
            COALESCE(updated_at, '') AS updated_at
        FROM conductor_upload_receipts
        WHERE lower(conductor_email)=lower(?)
          AND lower(COALESCE(concert_name, ''))=lower(?)
          AND lower(COALESCE(concert_ref, ''))=lower(?)
          AND lower(COALESCE(cycle_state, 'draft'))='draft'
        ORDER BY id DESC
        """, (
            clean_email,
            clean_concert_name,
            clean_concert_ref,
        ))
        return cur.fetchall()
    finally:
        if conn is not None:
            conn.close()


def conductor_cycle_refresh_latest(
    email: str,
    concert_name: str = "",
    concert_ref: str = "",
    skip_sent_promotion: bool = False,
):
    clean_email = str(email or "").strip().lower()
    clean_concert_name = str(concert_name or "").strip()
    clean_concert_ref = str(concert_ref or "").strip()

    if not clean_email or not clean_concert_name:
        return None

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        cur.execute("""
        SELECT
            id,
            COALESCE(concert_name, '') AS concert_name,
            COALESCE(concert_ref, '') AS concert_ref,
            COALESCE(cycle_state, 'sent') AS cycle_state,
            COALESCE(receipt_status, 'idle') AS receipt_status,
            COALESCE(forward_status, 'idle') AS forward_status,
            COALESCE(batch_token, '') AS batch_token
        FROM conductor_upload_receipts
        WHERE lower(conductor_email)=lower(?)
          AND lower(COALESCE(concert_name, ''))=lower(?)
          AND lower(COALESCE(concert_ref, ''))=lower(?)
          AND lower(COALESCE(cycle_state, 'sent')) NOT IN ('superseded', 'draft')
        ORDER BY
            COALESCE(upload_timestamp, created_at) DESC,
            id DESC
        LIMIT 1
        """, (
            clean_email,
            clean_concert_name,
            clean_concert_ref,
        ))
        latest = cur.fetchone()

        if not latest:
            return None

        now = datetime.utcnow().isoformat()
        batch_token = str(latest["batch_token"] or "").strip()

        if latest["cycle_state"] == "sent" and not skip_sent_promotion:
            if batch_token:
                cur.execute("""
                UPDATE conductor_upload_receipts
                SET cycle_state='received',
                    receipt_status='received',
                    updated_at=?
                WHERE lower(conductor_email)=lower(?)
                  AND lower(COALESCE(concert_name, ''))=lower(?)
                  AND lower(COALESCE(concert_ref, ''))=lower(?)
                  AND COALESCE(batch_token, '')=?
                  AND lower(COALESCE(cycle_state, 'sent'))='sent'
                """, (
                    now,
                    clean_email,
                    clean_concert_name,
                    clean_concert_ref,
                    batch_token,
                ))
            else:
                cur.execute("""
                UPDATE conductor_upload_receipts
                SET cycle_state='received',
                    receipt_status='received',
                    updated_at=?
                WHERE id=?
                """, (
                    now,
                    int(latest["id"]),
                ))
            conn.commit()

        cur.execute("""
        SELECT
            id,
            COALESCE(concert_name, '') AS concert_name,
            COALESCE(concert_ref, '') AS concert_ref,
            COALESCE(cycle_state, 'sent') AS cycle_state,
            COALESCE(receipt_status, 'idle') AS receipt_status,
            COALESCE(forward_status, 'idle') AS forward_status,
            COALESCE(batch_token, '') AS batch_token
        FROM conductor_upload_receipts
        WHERE lower(conductor_email)=lower(?)
          AND lower(COALESCE(concert_name, ''))=lower(?)
          AND lower(COALESCE(concert_ref, ''))=lower(?)
          AND lower(COALESCE(cycle_state, 'sent')) NOT IN ('superseded', 'draft')
        ORDER BY
            COALESCE(upload_timestamp, created_at) DESC,
            id DESC
        LIMIT 1
        """, (
            clean_email,
            clean_concert_name,
            clean_concert_ref,
        ))
        latest = cur.fetchone()

        if (
            latest
            and latest["cycle_state"] == "received"
            and concert_control_forwarding_enabled_for_concert(latest["concert_name"], latest["concert_ref"])
            and latest["forward_status"] != "forwarded"
        ):
            batch_token = str(latest["batch_token"] or "").strip()

            if batch_token:
                cur.execute("""
                UPDATE conductor_upload_receipts
                SET cycle_state='sent_to_all',
                    forward_status='forwarded',
                    updated_at=?
                WHERE lower(conductor_email)=lower(?)
                  AND lower(COALESCE(concert_name, ''))=lower(?)
                  AND lower(COALESCE(concert_ref, ''))=lower(?)
                  AND COALESCE(batch_token, '')=?
                  AND lower(COALESCE(cycle_state, 'received'))='received'
                """, (
                    now,
                    clean_email,
                    clean_concert_name,
                    clean_concert_ref,
                    batch_token,
                ))
            else:
                cur.execute("""
                UPDATE conductor_upload_receipts
                SET cycle_state='sent_to_all',
                    forward_status='forwarded',
                    updated_at=?
                WHERE id=?
                """, (
                    now,
                    int(latest["id"]),
                ))
            conn.commit()

            cur.execute("""
            SELECT
                id,
                COALESCE(concert_name, '') AS concert_name,
                COALESCE(concert_ref, '') AS concert_ref,
                COALESCE(cycle_state, 'sent') AS cycle_state,
                COALESCE(receipt_status, 'idle') AS receipt_status,
                COALESCE(forward_status, 'idle') AS forward_status,
                COALESCE(batch_token, '') AS batch_token
            FROM conductor_upload_receipts
            WHERE lower(conductor_email)=lower(?)
              AND lower(COALESCE(concert_name, ''))=lower(?)
              AND lower(COALESCE(concert_ref, ''))=lower(?)
              AND lower(COALESCE(cycle_state, 'sent')) NOT IN ('superseded', 'draft')
            ORDER BY
                COALESCE(upload_timestamp, created_at) DESC,
                id DESC
            LIMIT 1
            """, (
                clean_email,
                clean_concert_name,
                clean_concert_ref,
            ))
            latest = cur.fetchone()

        return latest
    finally:
        if conn is not None:
            conn.close()


def conductor_cycle_reset_latest(
    email: str,
    concert_name: str = "",
    concert_ref: str = "",
) -> bool:
    clean_email = str(email or "").strip().lower()
    clean_concert_name = str(concert_name or "").strip()
    clean_concert_ref = str(concert_ref or "").strip()

    if not clean_email or not clean_concert_name:
        return False

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        cur.execute("""
        SELECT
            id,
            COALESCE(cycle_state, 'sent') AS cycle_state,
            COALESCE(batch_token, '') AS batch_token
        FROM conductor_upload_receipts
        WHERE lower(conductor_email)=lower(?)
          AND lower(COALESCE(concert_name, ''))=lower(?)
          AND lower(COALESCE(concert_ref, ''))=lower(?)
          AND lower(COALESCE(cycle_state, 'sent')) NOT IN ('superseded', 'draft')
        ORDER BY
            COALESCE(upload_timestamp, created_at) DESC,
            id DESC
        LIMIT 1
        """, (
            clean_email,
            clean_concert_name,
            clean_concert_ref,
        ))
        latest = cur.fetchone()

        if not latest:
            return False

        if latest["cycle_state"] not in {"received", "sent_to_all"}:
            return False

        now = datetime.utcnow().isoformat()
        batch_token = str(latest["batch_token"] or "").strip()

        if batch_token:
            cur.execute("""
            UPDATE conductor_upload_receipts
            SET cycle_state='superseded',
                superseded_at=?,
                updated_at=?
            WHERE lower(conductor_email)=lower(?)
              AND lower(COALESCE(concert_name, ''))=lower(?)
              AND lower(COALESCE(concert_ref, ''))=lower(?)
              AND COALESCE(batch_token, '')=?
              AND lower(COALESCE(cycle_state, 'sent')) IN ('received', 'sent_to_all')
            """, (
                now,
                now,
                clean_email,
                clean_concert_name,
                clean_concert_ref,
                batch_token,
            ))
        else:
            cur.execute("""
            UPDATE conductor_upload_receipts
            SET cycle_state='superseded',
                superseded_at=?,
                updated_at=?
            WHERE id=?
            """, (
                now,
                now,
                int(latest["id"]),
            ))

        conn.commit()
        return cur.rowcount > 0
    finally:
        if conn is not None:
            conn.close()


@app.get("/conductor", response_class=HTMLResponse)
def conductor_hub(request: Request):
    conductor_file = TEMPLATES_DIR / "conductor.html"
    if not conductor_file.exists():
        return HTMLResponse("templates/conductor.html not found.", status_code=404)

    email = str(request.query_params.get("email") or "").strip().lower()
    if not email:
        return HTMLResponse("Conductor email missing.", status_code=400)

    upload_result = str(request.query_params.get("upload_result") or "").strip().lower()
    active_concert_name = str(request.query_params.get("concert_name") or "").strip()
    active_concert_ref = str(request.query_params.get("concert_ref") or "").strip()

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT *
        FROM users
        WHERE lower(email)=lower(?)
          AND role='conductor'
        LIMIT 1
        """, (email,))
        user = cur.fetchone()

        if not user:
            return HTMLResponse("Conductor not found.", status_code=404)

        if not active_concert_name:
            cur.execute("""
            SELECT
                COALESCE(concert_name, '') AS concert_name,
                COALESCE(concert_ref, '') AS concert_ref
            FROM conductor_upload_receipts
            WHERE lower(conductor_email)=lower(?)
              AND lower(COALESCE(cycle_state, 'draft'))='draft'
            ORDER BY id DESC
            LIMIT 1
            """, (email,))
            latest_draft = cur.fetchone()

            if latest_draft:
                active_concert_name = str(latest_draft["concert_name"] or "").strip()
                active_concert_ref = str(latest_draft["concert_ref"] or "").strip()

        if active_concert_name:
            conductor_cycle_refresh_latest(
                email=email,
                concert_name=active_concert_name,
                concert_ref=active_concert_ref,
                skip_sent_promotion=(upload_result == "sent"),
            )

        cur.execute("""
        SELECT
            id,
            concert_name,
            concert_ref,
            upload_filename,
            submit_mode,
            receipt_status,
            forward_status,
            COALESCE(cycle_state, 'sent') AS cycle_state,
            COALESCE(message_note, '') AS message_note,
            COALESCE(score_note, '') AS score_note,
            COALESCE(batch_token, '') AS batch_token,
            created_at,
            updated_at
        FROM conductor_upload_receipts
        WHERE lower(conductor_email)=lower(?)
          AND lower(COALESCE(cycle_state, 'sent'))!='draft'
        ORDER BY
            COALESCE(upload_timestamp, created_at) DESC,
            id DESC
        LIMIT 20
        """, (email,))
        conductor_updates = cur.fetchall()

        if not active_concert_name and conductor_updates:
            latest_visible = conductor_updates[0]
            active_concert_name = str(latest_visible["concert_name"] or "").strip()
            active_concert_ref = str(latest_visible["concert_ref"] or "").strip()

            if active_concert_name:
                conductor_cycle_refresh_latest(
                    email=email,
                    concert_name=active_concert_name,
                    concert_ref=active_concert_ref,
                    skip_sent_promotion=(upload_result == "sent"),
                )

                cur.execute("""
                SELECT
                    id,
                    concert_name,
                    concert_ref,
                    upload_filename,
                    submit_mode,
                    receipt_status,
                    forward_status,
                    COALESCE(cycle_state, 'sent') AS cycle_state,
                    COALESCE(message_note, '') AS message_note,
                    COALESCE(score_note, '') AS score_note,
                    COALESCE(batch_token, '') AS batch_token,
                    created_at,
                    updated_at
                FROM conductor_upload_receipts
                WHERE lower(conductor_email)=lower(?)
                  AND lower(COALESCE(cycle_state, 'sent'))!='draft'
                ORDER BY
                    COALESCE(upload_timestamp, created_at) DESC,
                    id DESC
                LIMIT 20
                """, (email,))
                conductor_updates = cur.fetchall()

        conductor_draft_files = []
        if active_concert_name:
            conductor_draft_files = conductor_draft_files_list(
                email=email,
                concert_name=active_concert_name,
                concert_ref=active_concert_ref,
            )

        conductor_button_state = "send"
        if not conductor_draft_files and active_concert_name:
            cur.execute("""
            SELECT
                COALESCE(cycle_state, 'sent') AS cycle_state
            FROM conductor_upload_receipts
            WHERE lower(conductor_email)=lower(?)
              AND lower(COALESCE(concert_name, ''))=lower(?)
              AND lower(COALESCE(concert_ref, ''))=lower(?)
              AND lower(COALESCE(cycle_state, 'sent')) NOT IN ('superseded', 'draft')
            ORDER BY
                COALESCE(upload_timestamp, created_at) DESC,
                id DESC
            LIMIT 1
            """, (
                email,
                active_concert_name,
                active_concert_ref,
            ))
            latest_cycle = cur.fetchone()

            if latest_cycle and latest_cycle["cycle_state"] in {"sent", "received", "sent_to_all"}:
                conductor_button_state = str(latest_cycle["cycle_state"] or "send").strip().lower()

        return templates.TemplateResponse(
            "conductor.html",
            {
                "request": request,
                "user": {"email": user["email"]},
                "concert_cards": [],
                "upload_result": upload_result,
                "conductor_button_state": conductor_button_state,
                "conductor_updates": conductor_updates,
                "conductor_draft_files": conductor_draft_files,
                "active_concert_name": active_concert_name,
                "active_concert_ref": active_concert_ref,
            },
        )
    finally:
        if conn is not None:
            conn.close()


@app.post("/conductor/upload_files")
async def conductor_upload_files(
    request: Request,
    concert_name: str = Form(""),
    concert_ref: str = Form(""),
    score_uploads: list[UploadFile] = File(...),
):
    email = str(request.query_params.get("email") or "").strip().lower()
    safe_email = quote_plus(email)
    clean_concert_name = str(concert_name or "").strip()
    clean_concert_ref = str(concert_ref or "").strip()

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        cur.execute("""
        SELECT *
        FROM users
        WHERE lower(email)=lower(?)
          AND role='conductor'
        LIMIT 1
        """, (email,))
        user = cur.fetchone()

        if not user or not clean_concert_name or not score_uploads:
            return RedirectResponse(
                f"/conductor?email={safe_email}&concert_name={quote_plus(clean_concert_name)}&concert_ref={quote_plus(clean_concert_ref)}&upload_result=failed",
                status_code=303,
            )

        upload_dir = APP_DIR / "conductor_uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)

        inserted_any = False

        for score_upload in score_uploads:
            file_name = Path(str(score_upload.filename or "").strip()).name
            if not file_name:
                continue

            file_bytes = await score_upload.read()
            if not file_bytes:
                continue

            upload_timestamp = datetime.utcnow().isoformat()
            stamped_name = (
                f"{upload_timestamp[:19].replace(':', '-').replace('T', '_')}"
                f"__conductor_{int(user['id'])}"
                f"__{file_name}"
            )
            stored_path = upload_dir / stamped_name
            stored_path.write_bytes(file_bytes)
            stored_rel_path = str(stored_path.relative_to(APP_DIR)).replace("\\", "/")

            cur.execute("""
            INSERT INTO conductor_upload_receipts (
                conductor_user_id,
                conductor_email,
                concert_name,
                concert_ref,
                upload_filename,
                upload_stored_path,
                upload_timestamp,
                submit_mode,
                receipt_status,
                forward_status,
                cycle_state,
                message_note,
                score_note,
                batch_token,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                int(user["id"]),
                str(user["email"] or "").strip().lower(),
                clean_concert_name,
                clean_concert_ref,
                file_name,
                stored_rel_path,
                upload_timestamp,
                "conductor_draft",
                "draft bundle",
                "idle",
                "draft",
                "",
                "",
                "draft",
                upload_timestamp,
                upload_timestamp,
            ))
            inserted_any = True

        conn.commit()

        return RedirectResponse(
            f"/conductor?email={safe_email}&concert_name={quote_plus(clean_concert_name)}&concert_ref={quote_plus(clean_concert_ref)}&upload_result={'drafted' if inserted_any else 'failed'}",
            status_code=303,
        )
    finally:
        if conn is not None:
            conn.close()


@app.post("/conductor/draft/{draft_id}/delete")
def conductor_delete_draft_file(
    request: Request,
    draft_id: int,
    concert_name: str = Form(""),
    concert_ref: str = Form(""),
):
    email = str(request.query_params.get("email") or "").strip().lower()
    safe_email = quote_plus(email)
    clean_concert_name = str(concert_name or "").strip()
    clean_concert_ref = str(concert_ref or "").strip()

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT
            id,
            COALESCE(upload_stored_path, '') AS upload_stored_path
        FROM conductor_upload_receipts
        WHERE id=?
          AND lower(conductor_email)=lower(?)
          AND lower(COALESCE(cycle_state, 'draft'))='draft'
        LIMIT 1
        """, (
            int(draft_id),
            email,
        ))
        row = cur.fetchone()

        if row:
            stored_rel_path = str(row["upload_stored_path"] or "").strip()
            if stored_rel_path:
                stored_path = APP_DIR / stored_rel_path
                if stored_path.exists():
                    try:
                        stored_path.unlink()
                    except Exception:
                        pass

            cur.execute("""
            DELETE FROM conductor_upload_receipts
            WHERE id=?
            """, (int(draft_id),))
            conn.commit()
    finally:
        if conn is not None:
            conn.close()

    return RedirectResponse(
        f"/conductor?email={safe_email}&concert_name={quote_plus(clean_concert_name)}&concert_ref={quote_plus(clean_concert_ref)}",
        status_code=303,
    )


@app.post("/conductor/draft/{draft_id}/score_note")
def conductor_save_draft_score_note(
    request: Request,
    draft_id: int,
    concert_name: str = Form(""),
    concert_ref: str = Form(""),
    score_note: str = Form(""),
):
    email = str(request.query_params.get("email") or "").strip().lower()
    safe_email = quote_plus(email)
    clean_concert_name = str(concert_name or "").strip()
    clean_concert_ref = str(concert_ref or "").strip()
    clean_score_note = str(score_note or "").strip()

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        UPDATE conductor_upload_receipts
        SET score_note=?,
            updated_at=?
        WHERE id=?
          AND lower(conductor_email)=lower(?)
          AND lower(COALESCE(cycle_state, 'draft'))='draft'
        """, (
            clean_score_note,
            datetime.utcnow().isoformat(),
            int(draft_id),
            email,
        ))
        conn.commit()
    finally:
        if conn is not None:
            conn.close()

    return RedirectResponse(
        f"/conductor?email={safe_email}&concert_name={quote_plus(clean_concert_name)}&concert_ref={quote_plus(clean_concert_ref)}",
        status_code=303,
    )


@app.post("/conductor/send_update")
async def conductor_send_update(
    request: Request,
    concert_name: str = Form(""),
    concert_ref: str = Form(""),
    message: str = Form(""),
    submit_mode: str = Form(""),
):
    email = str(request.query_params.get("email") or "").strip().lower()
    safe_email = quote_plus(email)

    clean_concert_name = str(concert_name or "").strip()
    clean_concert_ref = str(concert_ref or "").strip()
    clean_message = str(message or "").strip()
    submit_mode = "conductor_send"
    conn = None

    try:
        conn = db()
        cur = conn.cursor()

        cur.execute("""
        SELECT *
        FROM users
        WHERE lower(email)=lower(?)
          AND role='conductor'
        LIMIT 1
        """, (email,))
        user = cur.fetchone()

        if not user:
            return RedirectResponse(
                f"/conductor?email={safe_email}&upload_result=failed",
                status_code=303,
            )

        cur.execute("""
        SELECT COUNT(*) AS total
        FROM conductor_upload_receipts
        WHERE lower(conductor_email)=lower(?)
          AND lower(COALESCE(concert_name, ''))=lower(?)
          AND lower(COALESCE(concert_ref, ''))=lower(?)
          AND lower(COALESCE(cycle_state, 'draft'))='draft'
        """, (
            email,
            clean_concert_name,
            clean_concert_ref,
        ))
        draft_count_row = cur.fetchone()
        draft_count = int(draft_count_row["total"] or 0) if draft_count_row else 0

        if not clean_concert_name or draft_count <= 0:
            return RedirectResponse(
                f"/conductor?email={safe_email}&concert_name={quote_plus(clean_concert_name)}&concert_ref={quote_plus(clean_concert_ref)}&upload_result=failed",
                status_code=303,
            )

        send_timestamp = datetime.utcnow().isoformat()
        send_batch_token = f"send::{int(user['id'])}::{send_timestamp}"

        cur.execute("""
        UPDATE conductor_upload_receipts
        SET cycle_state='superseded',
            superseded_at=?,
            updated_at=?
        WHERE lower(conductor_email)=lower(?)
          AND lower(COALESCE(cycle_state, 'sent')) IN ('sent', 'received', 'sent_to_all')
          AND lower(COALESCE(concert_name, ''))=lower(?)
          AND lower(COALESCE(concert_ref, ''))=lower(?)
        """, (
            send_timestamp,
            send_timestamp,
            str(user["email"] or "").strip().lower(),
            clean_concert_name,
            clean_concert_ref,
        ))

        cur.execute("""
        UPDATE conductor_upload_receipts
        SET upload_timestamp=?,
            submit_mode=?,
            receipt_status='pending librarian review',
            forward_status='idle',
            cycle_state='sent',
            message_note=?,
            batch_token=?,
            updated_at=?
        WHERE lower(conductor_email)=lower(?)
          AND lower(COALESCE(concert_name, ''))=lower(?)
          AND lower(COALESCE(concert_ref, ''))=lower(?)
          AND lower(COALESCE(cycle_state, 'draft'))='draft'
        """, (
            send_timestamp,
            submit_mode,
            clean_message,
            send_batch_token,
            send_timestamp,
            str(user["email"] or "").strip().lower(),
            clean_concert_name,
            clean_concert_ref,
        ))
        conn.commit()

        return RedirectResponse(
            f"/conductor?email={safe_email}&concert_name={quote_plus(clean_concert_name)}&concert_ref={quote_plus(clean_concert_ref)}&upload_result=sent",
            status_code=303,
        )
    finally:
        if conn is not None:
            conn.close()


@app.post("/conductor/cycle/reset")
def conductor_cycle_reset(
    request: Request,
    concert_name: str = Form(""),
    concert_ref: str = Form(""),
):
    email = str(request.query_params.get("email") or "").strip().lower()
    safe_email = quote_plus(email)
    clean_concert_name = str(concert_name or "").strip()
    clean_concert_ref = str(concert_ref or "").strip()

    conductor_cycle_reset_latest(
        email=email,
        concert_name=clean_concert_name,
        concert_ref=clean_concert_ref,
    )

    return RedirectResponse(
        f"/conductor?email={safe_email}&concert_name={quote_plus(clean_concert_name)}&concert_ref={quote_plus(clean_concert_ref)}",
        status_code=303,
    )


@app.get("/librarian/invite_log", response_class=HTMLResponse)
def librarian_invite_log_page():
    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT
            i.id,
            COALESCE(o.name, 'Unknown organisation') AS organisation_name,
            COALESCE(mu.name, mu.email, 'Unknown musician') AS musician_name,
            COALESCE(mu.email, '') AS musician_email,
            COALESCE(lu.name, lu.email, 'Unknown librarian') AS invited_by_name,
            COALESCE(i.status, 'pending') AS status,
            COALESCE(i.invite_sent_at, i.created_at, '') AS invite_sent_at,
            COALESCE(i.responded_at, '') AS replied_at,
            COALESCE(i.musician_comment, '') AS musician_comment,
            CASE
                WHEN COALESCE(i.confirmed_at, '') != '' THEN 'Yes'
                ELSE 'No'
            END AS confirmed
        FROM invites i
        LEFT JOIN organisations o ON o.id = i.organisation_id
        LEFT JOIN users mu ON mu.id = i.musician_user_id
        LEFT JOIN users lu ON lu.id = i.invited_by_user_id
        ORDER BY
            COALESCE(i.invite_sent_at, i.created_at) DESC,
            i.id DESC
        """)
        rows = cur.fetchall()

        rows_html = ""
        for row in rows:
            rows_html += f"""
            <tr>
                <td style="padding:10px 12px; border-bottom:1px solid #223548;">{row["organisation_name"]}</td>
                <td style="padding:10px 12px; border-bottom:1px solid #223548;">{row["musician_name"]}<br><span style="color:#c8d0d8; font-size:13px;">{row["musician_email"]}</span></td>
                <td style="padding:10px 12px; border-bottom:1px solid #223548;">{row["invited_by_name"]}</td>
                <td style="padding:10px 12px; border-bottom:1px solid #223548; text-transform:capitalize;">{row["status"]}</td>
                <td style="padding:10px 12px; border-bottom:1px solid #223548;">{row["invite_sent_at"] or '—'}</td>
                <td style="padding:10px 12px; border-bottom:1px solid #223548;">{row["replied_at"] or '—'}</td>
                <td style="padding:10px 12px; border-bottom:1px solid #223548;">{row["confirmed"]}</td>
                <td style="padding:10px 12px; border-bottom:1px solid #223548;">{row["musician_comment"] or '—'}</td>
            </tr>
            """

        if not rows_html:
            rows_html = """
            <tr>
                <td colspan="8" style="padding:18px 12px; color:#c8d0d8;">No invite records found.</td>
            </tr>
            """

        return HTMLResponse(f"""
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Annotatio — Invite Log</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
</head>
<body style="margin:0; background:#071018; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif;">
    <div style="max-width:1500px; margin:24px auto; padding:0 18px;">
        <div style="background:#0b1622; border:1px solid #223548; border-radius:16px; overflow:hidden;">
            <div style="padding:18px 24px; border-bottom:1px solid #223548; text-align:center;">
                <div style="font-size:34px; color:#74d3de;">Librarian Invite Log</div>
            </div>
            <div style="padding:20px 24px 28px 24px;">
                <div style="overflow:auto;">
                    <table style="width:100%; border-collapse:collapse; background:#101e2d; border:1px solid #223548;">
                        <thead>
                            <tr style="background:#0d1927; color:#e5dccb; text-align:left;">
                                <th style="padding:12px;">Organisation</th>
                                <th style="padding:12px;">Musician</th>
                                <th style="padding:12px;">Invited by</th>
                                <th style="padding:12px;">Response</th>
                                <th style="padding:12px;">Invite sent</th>
                                <th style="padding:12px;">Invite replied</th>
                                <th style="padding:12px;">Confirmed</th>
                                <th style="padding:12px;">Musician comment</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows_html}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
""")
    finally:
        if conn is not None:
            conn.close()

# =====================================================================
# ORGANISATION POOL
# =====================================================================

@app.get("/organisation/{org_id}/pool", response_class=HTMLResponse)
def organisation_pool(org_id: int):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
        SELECT
            u.name,
            m.instrument,
            m.city,
            COALESCE(m.country, '') AS musician_country,
            COALESCE(om.organisation_country, '') AS organisation_country,
            COALESCE(om.membership_type, '') AS membership_type
        FROM organisation_memberships om
        JOIN users u ON u.id = om.musician_user_id
        LEFT JOIN musicians m ON m.user_id = u.id
        WHERE om.organisation_id=?
          AND om.status='accepted'
        ORDER BY
            CASE COALESCE(om.membership_type, '')
                WHEN 'local_pool' THEN 0
                WHEN 'international_invite' THEN 1
                ELSE 2
            END,
            u.name
        """, (org_id,))

        rows = cur.fetchall()

        html_out = f"""
        <!doctype html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <title>Annotatio — Organisation Pool</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
        </head>
        <body style="margin:0; background:#071018; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif;">
            <div style="max-width:1200px; margin:18px auto; padding:0 18px;">
                <div style="background:#0b1622; border:1px solid #223548; border-radius:18px; overflow:hidden;">
                    <div style="padding:18px 24px; text-align:center; border-bottom:1px solid #223548;">
                        <div style="font-size:34px; color:#74d3de;">Organisation {org_id} Pool</div>
                    </div>
                    <div style="padding:20px 24px 28px 24px;">
        """

        if rows:
            for row in rows:
                membership_type = row["membership_type"] or "unclassified"
                html_out += f"""
                        <div style="padding:14px 16px; margin-bottom:12px; border:1px solid #223548; border-radius:12px; background:#101e2d;">
                            <div style="font-size:20px; color:#74d3de; margin-bottom:6px;">{row['name']}</div>
                            <div style="font-size:15px; color:#d8e1e8;">
                                Instrument: {row['instrument'] or 'Not set'}<br>
                                City: {row['city'] or 'Not set'}<br>
                                Musician country: {row['musician_country'] or 'Not set'}<br>
                                Librarian country: {row['organisation_country'] or 'Not set'}<br>
                                Membership type: {membership_type}
                            </div>
                        </div>
                """
        else:
            html_out += """
                        <div style="padding:16px; border:1px dashed #31455c; border-radius:12px; background:#101e2d; color:#c8d0d8;">
                            No accepted musicians in this organisation pool.
                        </div>
            """

        html_out += """
                    </div>
                </div>
            </div>
        </body>
        </html>
        """

        return HTMLResponse(html_out)
    finally:
        if conn is not None:
            conn.close()


# =====================================================================
# CONCERT CONTROL FORWARDING STATE SOURCE
# =====================================================================

def concert_control_forwarding_state_normalize(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def concert_control_forwarding_state_get(
    concert_name: str,
    concert_ref: str = "",
):
    clean_concert_name = concert_control_forwarding_state_normalize(concert_name)
    clean_concert_ref = concert_control_forwarding_state_normalize(concert_ref)

    if not clean_concert_name:
        return None

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT
            id,
            concert_name,
            concert_ref,
            automatic_forwarding_enabled,
            created_at,
            updated_at
        FROM concert_control_forwarding_state
        WHERE concert_name=?
          AND COALESCE(concert_ref, '')=?
        LIMIT 1
        """, (
            clean_concert_name,
            clean_concert_ref,
        ))
        return cur.fetchone()
    finally:
        if conn is not None:
            conn.close()


def concert_control_forwarding_state_upsert(
    concert_name: str,
    concert_ref: str = "",
    automatic_forwarding_enabled: int = 0,
):
    clean_concert_name = concert_control_forwarding_state_normalize(concert_name)
    clean_concert_ref = concert_control_forwarding_state_normalize(concert_ref)

    if not clean_concert_name:
        return False

    now = datetime.utcnow().isoformat()

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT id
        FROM concert_control_forwarding_state
        WHERE concert_name=?
          AND COALESCE(concert_ref, '')=?
        LIMIT 1
        """, (
            clean_concert_name,
            clean_concert_ref,
        ))
        existing = cur.fetchone()

        if existing:
            cur.execute("""
            UPDATE concert_control_forwarding_state
            SET automatic_forwarding_enabled=?,
                updated_at=?
            WHERE id=?
            """, (
                1 if int(automatic_forwarding_enabled or 0) == 1 else 0,
                now,
                int(existing["id"]),
            ))
        else:
            cur.execute("""
            INSERT INTO concert_control_forwarding_state (
                concert_name,
                concert_ref,
                automatic_forwarding_enabled,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """, (
                clean_concert_name,
                clean_concert_ref,
                1 if int(automatic_forwarding_enabled or 0) == 1 else 0,
                now,
                now,
            ))

        conn.commit()
        return True
    finally:
        if conn is not None:
            conn.close()


def concert_control_forwarding_enabled_for_concert(
    concert_name: str,
    concert_ref: str = "",
) -> bool:
    row = concert_control_forwarding_state_get(
        concert_name=concert_name,
        concert_ref=concert_ref,
    )
    if not row:
        return False
    return int(row["automatic_forwarding_enabled"] or 0) == 1


# =====================================================================
# CONCERT CONTROL CONDUCTOR ALERTS — STANDALONE BLOCK
# PURPOSE: CONCERT CONTROL CONDUCTOR ALERT DATA ONLY
# =====================================================================

def concert_control_conductor_alerts_normalize_limit(limit: int) -> int:
    try:
        clean_limit = int(limit)
    except Exception:
        clean_limit = 20

    if clean_limit < 1:
        return 1
    if clean_limit > 200:
        return 200
    return clean_limit


def concert_control_conductor_alerts_fetch_rows(limit: int = 20):
    clean_limit = concert_control_conductor_alerts_normalize_limit(limit)

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT
            id,
            COALESCE(concert_name, '') AS concert_name,
            COALESCE(concert_ref, '') AS concert_ref,
            COALESCE(upload_filename, '') AS upload_filename,
            COALESCE(upload_timestamp, '') AS upload_timestamp,
            COALESCE(submit_mode, '') AS submit_mode,
            COALESCE(receipt_status, 'idle') AS receipt_status,
            COALESCE(forward_status, 'idle') AS forward_status,
            COALESCE(message_note, '') AS message_note,
            COALESCE(score_note, '') AS score_note,
            COALESCE(created_at, '') AS created_at,
            COALESCE(updated_at, '') AS updated_at
        FROM conductor_upload_receipts
        ORDER BY
            COALESCE(upload_timestamp, created_at) DESC,
            id DESC
        LIMIT ?
        """, (clean_limit,))
        return cur.fetchall()
    finally:
        if conn is not None:
            conn.close()


def concert_control_conductor_alerts_list(limit: int = 20):
    return concert_control_conductor_alerts_fetch_rows(limit)


def concert_control_conductor_alerts_latest():
    rows = concert_control_conductor_alerts_fetch_rows(1)
    return rows[0] if rows else None


def concert_control_conductor_alerts_pending_review_count() -> int:
    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT COUNT(*) AS total
        FROM conductor_upload_receipts
        WHERE lower(COALESCE(receipt_status, 'idle'))='pending librarian review'
        """)
        row = cur.fetchone()
        return int(row["total"] or 0) if row else 0
    finally:
        if conn is not None:
            conn.close()


def concert_control_conductor_alerts_forwarded_count() -> int:
    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT COUNT(*) AS total
        FROM conductor_upload_receipts
        WHERE lower(COALESCE(forward_status, 'idle'))='forwarded'
        """)
        row = cur.fetchone()
        return int(row["total"] or 0) if row else 0
    finally:
        if conn is not None:
            conn.close()


def concert_control_conductor_alerts_mark_reviewed(alert_id: int) -> bool:
    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        now = datetime.utcnow().isoformat()

        cur.execute("""
        SELECT
            id,
            COALESCE(conductor_email, '') AS conductor_email,
            COALESCE(concert_name, '') AS concert_name,
            COALESCE(concert_ref, '') AS concert_ref,
            COALESCE(batch_token, '') AS batch_token
        FROM conductor_upload_receipts
        WHERE id=?
        LIMIT 1
        """, (int(alert_id),))
        row = cur.fetchone()

        if not row:
            return False

        batch_token = str(row["batch_token"] or "").strip()

        if batch_token:
            cur.execute("""
            UPDATE conductor_upload_receipts
            SET receipt_status='received',
                cycle_state='received',
                updated_at=?
            WHERE lower(conductor_email)=lower(?)
              AND lower(COALESCE(concert_name, ''))=lower(?)
              AND lower(COALESCE(concert_ref, ''))=lower(?)
              AND COALESCE(batch_token, '')=?
              AND lower(COALESCE(receipt_status, 'idle'))='pending librarian review'
            """, (
                now,
                str(row["conductor_email"] or "").strip().lower(),
                str(row["concert_name"] or "").strip(),
                str(row["concert_ref"] or "").strip(),
                batch_token,
            ))
        else:
            cur.execute("""
            UPDATE conductor_upload_receipts
            SET receipt_status='received',
                cycle_state='received',
                updated_at=?
            WHERE id=?
              AND lower(COALESCE(receipt_status, 'idle'))='pending librarian review'
            """, (
                now,
                int(alert_id),
            ))

        conn.commit()
        return cur.rowcount > 0
    finally:
        if conn is not None:
            conn.close()


def concert_control_conductor_alerts_mark_forwarded(alert_id: int) -> bool:
    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        now = datetime.utcnow().isoformat()

        cur.execute("""
        SELECT
            id,
            COALESCE(conductor_email, '') AS conductor_email,
            COALESCE(concert_name, '') AS concert_name,
            COALESCE(concert_ref, '') AS concert_ref,
            COALESCE(batch_token, '') AS batch_token
        FROM conductor_upload_receipts
        WHERE id=?
        LIMIT 1
        """, (int(alert_id),))
        row = cur.fetchone()

        if not row:
            return False

        batch_token = str(row["batch_token"] or "").strip()

        if batch_token:
            cur.execute("""
            UPDATE conductor_upload_receipts
            SET receipt_status='received',
                forward_status='forwarded',
                cycle_state='sent_to_all',
                updated_at=?
            WHERE lower(conductor_email)=lower(?)
              AND lower(COALESCE(concert_name, ''))=lower(?)
              AND lower(COALESCE(concert_ref, ''))=lower(?)
              AND COALESCE(batch_token, '')=?
              AND lower(COALESCE(receipt_status, 'idle'))='pending librarian review'
            """, (
                now,
                str(row["conductor_email"] or "").strip().lower(),
                str(row["concert_name"] or "").strip(),
                str(row["concert_ref"] or "").strip(),
                batch_token,
            ))
        else:
            cur.execute("""
            UPDATE conductor_upload_receipts
            SET receipt_status='received',
                forward_status='forwarded',
                cycle_state='sent_to_all',
                updated_at=?
            WHERE id=?
              AND lower(COALESCE(receipt_status, 'idle'))='pending librarian review'
            """, (
                now,
                int(alert_id),
            ))

        conn.commit()
        return cur.rowcount > 0
    finally:
        if conn is not None:
            conn.close()


@app.post("/librarian/conductor_alert/review")
def librarian_conductor_alert_review(
    alert_id: int = Form(...),
):
    concert_control_conductor_alerts_mark_reviewed(int(alert_id))
    return RedirectResponse("/librarian", status_code=303)


@app.post("/librarian/conductor_alert/forward")
def librarian_conductor_alert_forward(
    alert_id: int = Form(...),
):
    concert_control_conductor_alerts_mark_forwarded(int(alert_id))
    return RedirectResponse("/librarian", status_code=303) 


# =====================================================================
# CONCERT CONTROL PAGE
# =====================================================================

CONCERT_CONTROL_SECTIONS = [
    "Strings",
    "Winds",
    "Brass",
    "Percussion",
    "Voice",
    "Guests",
]


def concert_control_page_alerts_fetch_rows(limit: int = 12) -> list[sqlite3.Row]:
    clean_limit = 12
    try:
        clean_limit = int(limit)
    except Exception:
        clean_limit = 12

    if clean_limit < 1:
        clean_limit = 1
    if clean_limit > 200:
        clean_limit = 200

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                COALESCE(concert_name, '') AS concert_name,
                COALESCE(concert_ref, '') AS concert_ref,
                COALESCE(upload_filename, '') AS upload_filename,
                COALESCE(upload_timestamp, '') AS upload_timestamp,
                COALESCE(submit_mode, '') AS submit_mode,
                COALESCE(receipt_status, 'idle') AS receipt_status,
                COALESCE(forward_status, 'idle') AS forward_status,
                COALESCE(cycle_state, 'sent') AS cycle_state,
                COALESCE(message_note, '') AS message_note,
                COALESCE(score_note, '') AS score_note,
                COALESCE(created_at, '') AS created_at,
                COALESCE(updated_at, '') AS updated_at
            FROM conductor_upload_receipts
            ORDER BY
                COALESCE(upload_timestamp, created_at) DESC,
                id DESC
            LIMIT ?
            """,
            (clean_limit,),
        )
        return cur.fetchall()
    finally:
        if conn is not None:
            conn.close()


def concert_control_page_alerts_count_pending_review() -> int:
    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) AS total
            FROM conductor_upload_receipts
            WHERE lower(COALESCE(receipt_status, 'idle'))='pending librarian review'
            """
        )
        row = cur.fetchone()
        return int(row["total"] or 0) if row else 0
    finally:
        if conn is not None:
            conn.close()


def concert_control_page_alerts_count_forwarded() -> int:
    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) AS total
            FROM conductor_upload_receipts
            WHERE lower(COALESCE(forward_status, 'idle'))='forwarded'
            """
        )
        row = cur.fetchone()
        return int(row["total"] or 0) if row else 0
    finally:
        if conn is not None:
            conn.close()


def concert_control_page_cycle_state_label(cycle_state: str) -> str:
    clean_state = str(cycle_state or "").strip().lower()

    if clean_state == "failed":
        return "Failed"
    if clean_state == "sent_to_all":
        return "Sent to All"
    if clean_state == "received":
        return "Received"
    if clean_state == "sent":
        return "Sent"
    if clean_state == "superseded":
        return "Superseded"
    return "Pending"


def concert_control_page_cycle_state_summary(cycle_state: str) -> str:
    clean_state = str(cycle_state or "").strip().lower()

    if clean_state == "failed":
        return "Upload failed at Concert Control"
    if clean_state == "sent_to_all":
        return "Outward issue completed"
    if clean_state == "received":
        return "Concert Control has this file"
    if clean_state == "sent":
        return "Waiting for Concert Control receipt"
    if clean_state == "superseded":
        return "Replaced by a newer cycle"
    return "Pending"


def concert_control_tier_seat_limit(tier: int) -> int:
    seat_limits = {
        1: 8,
        2: 16,
        3: 24,
        4: 32,
        5: 45,
        6: 60,
        7: 80,
        8: 100,
        9: 120,
        10: 9999,
    }
    clean_tier = int(tier or 1)
    if clean_tier < 1:
        clean_tier = 1
    if clean_tier > 10:
        clean_tier = 10
    return int(seat_limits.get(clean_tier, seat_limits[1]))


def concert_control_tier_pricing(tier: int) -> dict:
    pricing = {
        1: {"first": 49, "repeat": 20},
        2: {"first": 59, "repeat": 20},
        3: {"first": 69, "repeat": 20},
        4: {"first": 89, "repeat": 25},
        5: {"first": 109, "repeat": 25},
        6: {"first": 129, "repeat": 30},
        7: {"first": 149, "repeat": 30},
        8: {"first": 169, "repeat": 35},
        9: {"first": 189, "repeat": 40},
        10: {"first": 219, "repeat": 45},
    }
    clean_tier = int(tier or 1)
    if clean_tier < 1:
        clean_tier = 1
    if clean_tier > 10:
        clean_tier = 10
    return pricing.get(clean_tier, pricing[1])


def concert_control_tier_update(
    concert_name: str,
    concert_ref: str,
    new_tier: int,
) -> bool:
    clean_name = concert_control_detail_normalize_concert_name(concert_name)
    clean_ref = concert_control_detail_normalize_concert_ref(concert_ref)
    clean_tier = int(new_tier or 1)

    if clean_tier < 1:
        clean_tier = 1
    if clean_tier > 10:
        clean_tier = 10

    if not clean_name and not clean_ref:
        return False

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE librarian_created_concerts
            SET concert_tier=?,
                updated_at=?
            WHERE lower(trim(COALESCE(concert_name, '')))=lower(?)
              AND lower(trim(COALESCE(concert_ref, '')))=lower(?)
            """,
            (
                clean_tier,
                datetime.utcnow().isoformat(),
                clean_name,
                clean_ref,
            ),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        if conn is not None:
            conn.close()


def concert_control_page_latest_alert_snapshot(conductor_alerts: list[sqlite3.Row]) -> dict[str, str]:
    latest_alert = conductor_alerts[0] if conductor_alerts else None

    if latest_alert:
        cycle_state = str(latest_alert["cycle_state"] or "sent").strip().lower()
        score_note = str(latest_alert["score_note"] or "").strip()
        message_note = str(latest_alert["message_note"] or "").strip()
        combined_note = message_note or "No conductor bundle note supplied."
        if score_note:
            combined_note = f"{combined_note}\n\nScore note: {score_note}"

        return {
            "name": latest_alert["concert_name"] or "Unnamed concert",
            "concert_ref": latest_alert["concert_ref"] or "",
            "file": latest_alert["upload_filename"] or "No file name",
            "receipt": latest_alert["receipt_status"] or "idle",
            "forward": latest_alert["forward_status"] or "idle",
            "cycle_state": cycle_state,
            "cycle_label": concert_control_page_cycle_state_label(cycle_state),
            "cycle_summary": concert_control_page_cycle_state_summary(cycle_state),
            "time": latest_alert["updated_at"] or latest_alert["upload_timestamp"] or latest_alert["created_at"] or "—",
            "note": combined_note,
        }

    return {
        "name": "None",
        "concert_ref": "",
        "file": "No file name",
        "receipt": "idle",
        "forward": "idle",
        "cycle_state": "pending",
        "cycle_label": "Pending",
        "cycle_summary": "No conductor uploads yet.",
        "time": "—",
        "note": "No conductor bundle note supplied.",
    }


def concert_control_page_alert_rows_markup(conductor_alerts: list[sqlite3.Row]) -> str:
    if not conductor_alerts:
        return """
        <tr>
            <td colspan="6" style="padding:18px; border-bottom:none; color:#c8d0d8;">No conductor upload alerts yet.</td>
        </tr>
        """

    rows_html = ""
    for alert in conductor_alerts:
        cycle_state = str(alert["cycle_state"] or "sent").strip().lower()
        cycle_label = concert_control_page_cycle_state_label(cycle_state)
        cycle_summary = concert_control_page_cycle_state_summary(cycle_state)
        bundle_note = str(alert["message_note"] or "").strip()
        score_note = str(alert["score_note"] or "").strip()

        note_parts = []
        if bundle_note:
            note_parts.append(bundle_note)
        if score_note:
            note_parts.append(f"Score note: {score_note}")
        note_display = "<br><br>".join(note_parts) if note_parts else "—"

        rows_html += f"""
        <tr>
            <td style="padding:12px 8px; border-bottom:1px solid #31455c;">
                <div style="font-size:17px; color:#f2f0ea;">{alert["concert_name"] or "Unnamed concert"}</div>
                <div style="font-size:14px; color:#c8d0d8;">{alert["concert_ref"] or "—"}</div>
            </td>
            <td style="padding:12px 8px; border-bottom:1px solid #31455c;">{alert["upload_filename"] or "—"}</td>
            <td style="padding:12px 8px; border-bottom:1px solid #31455c;">
                <strong>{cycle_label}</strong><br>
                <span style="color:#c8d0d8; font-size:14px;">{cycle_summary}</span>
            </td>
            <td style="padding:12px 8px; border-bottom:1px solid #31455c;">{alert["forward_status"] or "idle"}</td>
            <td style="padding:12px 8px; border-bottom:1px solid #31455c;">{alert["updated_at"] or alert["upload_timestamp"] or alert["created_at"] or "—"}</td>
            <td style="padding:12px 8px; border-bottom:1px solid #31455c;">{note_display}</td>
        </tr>
        """
    return rows_html


def concert_control_page_visible_sections(focus_section: str) -> list[str]:
    clean_focus_section = str(focus_section or "").strip().lower()
    if not clean_focus_section:
        return list(CONCERT_CONTROL_SECTIONS)

    visible_sections = [
        section_name
        for section_name in CONCERT_CONTROL_SECTIONS
        if section_name.strip().lower() == clean_focus_section
    ]
    if visible_sections:
        return visible_sections
    return list(CONCERT_CONTROL_SECTIONS)


def concert_control_page_section_rows(section_rows: list[dict], highlight_unopened: str) -> list[dict]:
    rows = list(section_rows)
    if not str(highlight_unopened or "").strip():
        return rows

    rows.sort(
        key=lambda row: (
            0 if str(row.get("confirmed") or "").strip().lower() == "no" else 1,
            str(row.get("musician_name") or "").strip().lower(),
        )
    )
    return rows


def concert_control_page_available_options_markup(section_rows: list[dict]) -> str:
    options_html = ""
    for row in section_rows:
        options_html += f'<option>{row["musician_name"]} · {row["instrument"]}</option>'

    if options_html:
        return options_html
    return '<option>No musicians available</option>'


def concert_control_page_member_rows_markup(section_rows: list[dict]) -> str:
    member_rows = ""
    for row in section_rows:
        member_rows += f"""
            <div style="padding:12px 14px; margin-bottom:10px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#f2f0ea; font-size:15px; line-height:1.65;">
                <strong>{row["musician_name"]}</strong><br>
                {row["instrument"]}<br>
                {row["musician_email"]}<br>
                Confirmed: {row["confirmed"]} · Current file: {row["has_current_file"]}
            </div>
        """

    if member_rows:
        return member_rows

    return """
            <div style="padding:16px; border:1px dashed #31455c; border-radius:12px; background:#0b1622; color:#aeb8c2; font-size:15px; line-height:1.7;">
                No musicians in this section.
            </div>
    """


def concert_control_page_section_markup(
    section_name: str,
    section_count: dict,
    section_rows: list[dict],
) -> str:
    available_options = concert_control_page_available_options_markup(section_rows)
    member_rows = concert_control_page_member_rows_markup(section_rows)

    return f"""
    <div style="display:block; width:100%; max-width:100%; min-width:0; box-sizing:border-box; border:1px solid #223548; border-radius:16px; background:#101e2d; margin-bottom:16px; overflow:hidden; position:relative;">
        <div style="padding:16px 18px; border-bottom:1px solid #223548; box-sizing:border-box;">
            <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">{section_name}</div>

            <div style="display:grid; grid-template-columns:repeat(3, minmax(120px, 1fr)); gap:12px; margin-bottom:12px;">
                <div style="min-width:0; padding:10px 12px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#f2f0ea; font-size:15px; box-sizing:border-box;">
                    Selected: {section_count["selected"]}
                </div>
                <div style="min-width:0; padding:10px 12px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#f2f0ea; font-size:15px; box-sizing:border-box;">
                    Available: {section_count["available"]}
                </div>
                <div style="min-width:0; padding:10px 12px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#f2f0ea; font-size:15px; box-sizing:border-box;">
                    Qty: {section_count["qty"]}
                </div>
            </div>

            <div style="display:flex; flex-wrap:wrap; gap:12px;">
                <button disabled style="padding:11px 18px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif; font-size:15px; white-space:nowrap;">
                    Select musician/s
                </button>
                <button disabled style="padding:11px 18px; border-radius:999px; border:1px solid #31455c; background:#0d1927; color:#c8d0d8; font-family:Georgia, 'Times New Roman', serif; font-size:15px; white-space:nowrap;">
                    Away
                </button>
            </div>
        </div>

        <div style="padding:18px; box-sizing:border-box;">
            <div style="display:grid; grid-template-columns:minmax(0, 1fr) auto; gap:12px; align-items:end; margin-bottom:14px;">
                <div style="min-width:0;">
                    <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Available musicians</div>
                    <select disabled style="width:100%; max-width:100%; padding:12px 14px; border-radius:12px; border:1px solid #31455c; background:#0b1622; color:#c8d0d8; font-family:Georgia, 'Times New Roman', serif; font-size:16px; box-sizing:border-box;">
                        {available_options}
                    </select>
                </div>

                <div style="flex:0 0 auto;">
                    <button disabled style="padding:12px 18px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif; font-size:15px; white-space:nowrap;">
                        Add musician
                    </button>
                </div>
            </div>

            {member_rows}
        </div>
    </div>
    """


def render_concert_control_page(
    venue_names: list[str] | None = None,
    concert_name: str = "",
    concert_ref: str = "",
    focus_section: str = "",
    highlight_unopened: str = "",
    librarian_email: str = "",
) -> str:
    venue_names = venue_names or []
    venue_display = venue_names[0] if venue_names else "Not yet set."

    clean_concert_name = concert_control_detail_normalize_concert_name(concert_name)
    clean_concert_ref = concert_control_detail_normalize_concert_ref(concert_ref)
    clean_librarian_email = str(librarian_email or "").strip() or "librarian@local"
    encoded_librarian_email = quote_plus(clean_librarian_email)

    concert_detail = concert_control_detail_fetch(clean_concert_name, clean_concert_ref)

    current_tier = int(concert_detail.get("concert_tier", 1) or 1)
    if current_tier < 1:
        current_tier = 1
    if current_tier > 10:
        current_tier = 10

    tier_pricing = concert_control_tier_pricing(current_tier)
    tier_seat_limit = concert_control_tier_seat_limit(current_tier)
    tier_minus = current_tier - 1 if current_tier > 1 else 1
    tier_plus = current_tier + 1 if current_tier < 10 else 10
    tier_minus_url = (
        f"/control?email={encoded_librarian_email}"
        f"&concert_name={quote_plus(clean_concert_name)}"
        f"&concert_ref={quote_plus(clean_concert_ref)}"
        f"&tier_adjust=set_{tier_minus}"
    )
    tier_plus_url = (
        f"/control?email={encoded_librarian_email}"
        f"&concert_name={quote_plus(clean_concert_name)}"
        f"&concert_ref={quote_plus(clean_concert_ref)}"
        f"&tier_adjust=set_{tier_plus}"
    )

    automatic_forwarding_enabled = concert_control_forwarding_enabled_for_concert(
        clean_concert_name,
        clean_concert_ref,
    )
    automatic_forwarding_label = "ON" if automatic_forwarding_enabled else "OFF"
    automatic_forwarding_note = (
        "Conductor receipt auto-forwards to all musicians."
        if automatic_forwarding_enabled
        else "Manual librarian forward remains in control."
    )
    automatic_forwarding_toggle_url = (
        f"/control?email={encoded_librarian_email}"
        f"&concert_name={quote_plus(clean_concert_name)}"
        f"&concert_ref={quote_plus(clean_concert_ref)}"
        f"&automatic_forwarding={'off' if automatic_forwarding_enabled else 'on'}"
    )

    conductor_alerts = concert_control_page_alerts_fetch_rows(limit=12)
    latest_alert_snapshot = concert_control_page_latest_alert_snapshot(conductor_alerts)
    conductor_alert_rows = concert_control_page_alert_rows_markup(conductor_alerts)

    sent_count = sum(
        1 for alert in conductor_alerts
        if str(alert["cycle_state"] or "").strip().lower() == "sent"
    )
    received_count = sum(
        1 for alert in conductor_alerts
        if str(alert["cycle_state"] or "").strip().lower() == "received"
    )
    sent_to_all_count = sum(
        1 for alert in conductor_alerts
        if str(alert["cycle_state"] or "").strip().lower() == "sent_to_all"
    )

    musicians_step_done = concert_detail["selected_musicians"] > 0
    files_step_done = bool(concert_detail["has_current_file"])
    issue_step_done = bool(concert_detail["has_current_file"] and concert_detail["selected_musicians"] > 0)
    conductor_step_done = len(conductor_alerts) > 0

    musicians_step_color = "#3d8d57" if musicians_step_done else "#74d3de"
    files_step_color = "#3d8d57" if files_step_done else "#74d3de"
    issue_step_color = "#3d8d57" if issue_step_done else "#74d3de"
    conductor_step_color = "#3d8d57" if conductor_step_done else "#74d3de"

    concert_file_rows = concert_control_current_file_list(clean_concert_name, clean_concert_ref)
    concert_file_options_html = ""
    concert_file_list_html = ""

    for file_row in concert_file_rows:
        file_id = int(file_row["id"])
        file_name = str(file_row["original_filename"] or "").strip() or "Unnamed file"
        file_uploaded_at = str(file_row["uploaded_at"] or "").strip() or "—"

        concert_file_options_html += f"""
                                <option value="{file_id}">{file_name} — Uploaded {file_uploaded_at}</option>
        """


    if not concert_file_options_html:
        concert_file_options_html = """
                                <option value="">No uploaded files yet</option>
        """

    if not concert_file_list_html:
        concert_file_list_html = """
                                <div style="padding:14px 16px; border:1px dashed #31455c; border-radius:12px; background:#0d1927; color:#c8d0d8; font-size:15px; line-height:1.7;">
                                    No uploaded files yet.
                                </div>
        """

    section_markup = ""
    for section_name in concert_control_page_visible_sections(focus_section):
        section_count = concert_detail["section_counts"][section_name]
        section_rows = concert_control_page_section_rows(
            concert_detail["section_rows"][section_name],
            highlight_unopened,
        )
        section_markup += concert_control_page_section_markup(
            section_name=section_name,
            section_count=section_count,
            section_rows=section_rows,
        )

    page_html = f""" 
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Annotatio — Concert Control</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
    </head>
    <body style="margin:0; background:#071018; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif;">
        <div style="width:100%; max-width:none; margin:18px 0 42px 0; padding:0 18px;">

            <div style="background:#0b1622; border:1px solid #223548; border-radius:18px; overflow:hidden; box-shadow:0 10px 30px rgba(0,0,0,0.28);">

                <div style="padding:18px 24px 10px 24px; text-align:center; border-bottom:1px solid #223548;">
                    <div style="display:grid; grid-template-columns:minmax(220px, 1fr) minmax(420px, 520px) minmax(420px, 520px); gap:18px; align-items:start;">
                        <div style="display:grid; gap:10px; justify-items:start; padding-top:14px;">
                            <a href="/librarian?email={quote_plus('librarian@local')}" style="display:inline-flex; align-items:center; justify-content:center; min-height:42px; padding:10px 18px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; text-decoration:none; font-size:15px;">
                                Return to Librarian
                            </a>
                            <a href="/control/conductor_ensemble_pdf?email={quote_plus('librarian@local')}&concert_name={quote_plus(clean_concert_name)}&concert_ref={quote_plus(clean_concert_ref)}" style="display:inline-flex; align-items:center; justify-content:center; min-height:42px; padding:10px 18px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; text-decoration:none; font-size:15px;">
                                Export Ensemble PDF
                            </a>
                        </div>

                        <div style="display:block;">
                            <div style="font-size:36px; color:#74d3de; margin-bottom:8px;">Concert Control</div>
                            <div style="width:190px; height:1px; background:#b89457; margin:0 auto 12px auto;"></div>
                            <div style="display:flex; justify-content:center; margin:0 auto 12px auto;">
                                <div style="width:min(520px, 100%); min-height:74px; padding:14px 16px; border:1px solid #223548; border-radius:16px; background:#101e2d; box-shadow:0 10px 30px rgba(0,0,0,0.18); box-sizing:border-box;">
                                    <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:8px;">Automatic Send</div>
                                    <div style="display:flex; align-items:center; justify-content:space-between; gap:14px; flex-wrap:wrap;">
                                        <div style="text-align:left; min-width:0; flex:1 1 240px;">
                                            <div style="font-size:32px; font-weight:700; letter-spacing:0.04em; color:{'#3d8d57' if automatic_forwarding_enabled else '#d5b06a'}; line-height:1; margin-bottom:8px; text-shadow:0 0 8px rgba(255,255,255,0.34), 0 0 18px rgba(255,255,255,0.26), 0 0 34px rgba(255,255,255,0.18), 0 0 54px rgba(255,255,255,0.12), 0 0 76px rgba(255,255,255,0.08);">{automatic_forwarding_label}</div>
                                            <div style="font-size:15px; color:#c8d0d8; line-height:1.6;">{automatic_forwarding_note}</div>
                                        </div>
                                        <a href="{automatic_forwarding_toggle_url}" style="display:inline-flex; align-items:center; justify-content:center; min-height:42px; padding:10px 20px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; text-decoration:none; font-size:15px; white-space:nowrap;">
                                            Turn {'OFF' if automatic_forwarding_enabled else 'ON'}
                                        </a>
                                    </div>
                                </div>
                            </div>
                            <div style="font-size:16px; color:#c8d0d8;">This is the brains page.</div>
                        </div>

                        <div style="display:flex; justify-content:flex-end; align-items:flex-start; padding-top:28px;">
                            <div style="width:min(520px, 100%);">
                                <div style="width:min(520px, 100%); min-height:74px; padding:14px 16px; border:1px solid #223548; border-radius:16px; background:#101e2d; box-shadow:0 10px 30px rgba(0,0,0,0.18); box-sizing:border-box; text-align:left;">
                                    <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:8px;">Latest Conductor Note</div>
                                    <div style="color:#f2f0ea; font-size:15px; line-height:1.6;">
                                        {latest_alert_snapshot["note"]}
                                    </div>
                                </div>
                            </div>
                        </div>
                </div>

                <div style="padding:20px 24px 28px 24px;">

                    <div style="margin-bottom:18px; padding:10px 14px; border:1px solid #223548; border-radius:12px; background:#101e2d; font-size:13px; line-height:1.7; color:#c8d0d8;">
                        <span style="color:#e5dccb; margin-right:10px;">Quick Steps</span>
                        <a id="concert-step-link-musicians" href="#concert-step-musicians" style="color:{musicians_step_color}; text-decoration:none; margin-right:14px;">1. Select Musicians</a>
                        <a id="concert-step-link-files" href="#concert-step-files" style="color:{files_step_color}; text-decoration:none; margin-right:14px;">2. Add Files to Send</a>
                        <a id="concert-step-link-issue" href="#concert-step-issue" style="color:{issue_step_color}; text-decoration:none; margin-right:14px;">3. Issue Concert Update</a>
                        <a id="concert-step-link-conductor" href="#concert-step-conductor" style="color:{conductor_step_color}; text-decoration:none;">4. Review Conductor Uploads</a>
                    </div>

                    <div style="display:grid; grid-template-columns:minmax(320px, 420px) minmax(0, 1fr) minmax(320px, 420px); gap:18px; align-items:start; width:100%; min-width:0;">

                        <div style="display:grid; gap:18px; min-width:0; width:100%; max-width:100%; overflow:hidden;">
                            <div style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Current File Status</div>
                                <div style="display:grid; grid-template-columns:1fr; gap:12px;">
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Current update</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{concert_detail["current_file_label"]}</div>
                                    </div>
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Files in update</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{concert_detail["current_file_count"]}</div>
                                    </div>
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">File active</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{"Yes" if concert_detail["has_current_file"] else "No"}</div>
                                    </div>
                                </div>
                            </div>

                            <div id="concert-step-files" style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Current File Upload</div>
                                <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Upload or replace</div>
                                <form method="post" action="/control/current_file/upload?email={encoded_librarian_email}" enctype="multipart/form-data" style="margin:0;">
                                    <input type="hidden" name="concert_name" value="{concert_detail['concert_name']}">
                                    <input type="hidden" name="concert_ref" value="{concert_detail['concert_ref']}">
                                    <div style="display:flex; gap:12px; flex-wrap:wrap; margin-bottom:12px;">
                                        <input type="file" name="current_file_upload" required style="flex:1 1 320px; padding:12px 14px; border-radius:12px; border:1px solid #31455c; background:#0d1927; color:#c8d0d8; font-family:Georgia, 'Times New Roman', serif; font-size:15px;">
                                        <button type="submit" style="padding:12px 18px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif; font-size:15px;">Upload Current File</button>
                                    </div>
                                </form>
                                <div style="padding:14px 16px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#c8d0d8; font-size:15px; line-height:1.7;">
                                    When a new file is sent from this page, the older musician-facing file should be flushed from access. Musicians should only ever reach the latest current file for this concert.
                                </div>
                            </div>

                            <div id="concert-step-issue" style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Concert Distribution</div>
                                <select style="width:100%; padding:12px 14px; border-radius:12px; border:1px solid #31455c; background:#0d1927; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif; font-size:16px; margin-bottom:12px;">
                                    {concert_file_options_html}
                                </select>
                                <div style="padding:14px 16px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#c8d0d8; font-size:15px; line-height:1.7;">
                                    Current authorised file: <span style="color:#f2f0ea;">{concert_detail["current_file_label"]}</span>
                                </div>
                                <div style="margin-top:12px; max-height:240px; overflow:auto;">
                                    {concert_file_list_html}
                                </div>
                            </div>

                            <div style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Conductor Cycle Overview</div>
                                <div style="display:grid; grid-template-columns:repeat(3, 1fr); gap:12px; margin-bottom:12px;">
                                    <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Sent</div>
                                        <div style="font-size:24px; color:#f2f0ea;">{sent_count}</div>
                                    </div>
                                    <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Received</div>
                                        <div style="font-size:24px; color:#f2f0ea;">{received_count}</div>
                                    </div>
                                    <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Sent to all</div>
                                        <div style="font-size:24px; color:#f2f0ea;">{sent_to_all_count}</div>
                                    </div>
                                </div>
                                <div style="padding:14px 16px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#c8d0d8; font-size:15px; line-height:1.7;">
                                    Latest upload: <span style="color:#f2f0ea;">{latest_alert_snapshot["name"]}</span><br>
                                    {'Concert ref: <span style="color:#f2f0ea;">' + latest_alert_snapshot["concert_ref"] + '</span><br>' if latest_alert_snapshot["concert_ref"] else ''}
                                    File: <span style="color:#f2f0ea;">{latest_alert_snapshot["file"]}</span><br>
                                    Send state: <span style="color:#f2f0ea;">{latest_alert_snapshot["cycle_label"]}</span><br>
                                    State detail: <span style="color:#f2f0ea;">{latest_alert_snapshot["cycle_summary"]}</span><br>
                                    Updated at: <span style="color:#f2f0ea;">{latest_alert_snapshot["time"]}</span>
                                </div>
                            </div>

                            <div style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Conductor Cycle Overview</div>
                                <div style="display:grid; grid-template-columns:repeat(3, 1fr); gap:12px; margin-bottom:12px;">
                                    <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Sent</div>
                                        <div style="font-size:24px; color:#f2f0ea;">{sent_count}</div>
                                    </div>
                                    <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Received</div>
                                        <div style="font-size:24px; color:#f2f0ea;">{received_count}</div>
                                    </div>
                                    <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Sent to all</div>
                                        <div style="font-size:24px; color:#f2f0ea;">{sent_to_all_count}</div>
                                    </div>
                                </div>
                                <div style="padding:14px 16px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#c8d0d8; font-size:15px; line-height:1.7;">
                                    Latest upload: <span style="color:#f2f0ea;">{latest_alert_snapshot.get("name", "None")}</span><br>
                                    {'Concert ref: <span style="color:#f2f0ea;">' + latest_alert_snapshot.get("concert_ref", "") + '</span><br>' if latest_alert_snapshot.get("concert_ref", "") else ''}
                                    File: <span style="color:#f2f0ea;">{latest_alert_snapshot.get("file", "No file name")}</span><br>
                                    Send state: <span style="color:#f2f0ea;">{latest_alert_snapshot.get("cycle_label", "Pending")}</span><br>
                                    State detail: <span style="color:#f2f0ea;">{latest_alert_snapshot.get("cycle_summary", "No conductor upload alerts yet.")}</span><br>
                                    Updated at: <span style="color:#f2f0ea;">{latest_alert_snapshot.get("updated_at", "Not yet available")}</span>
                                </div>
                            </div>

                            <div style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Concert Overview</div>
                                <div style="display:grid; grid-template-columns:1fr; gap:12px;">
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Concert name</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{concert_detail["concert_name"]}</div>
                                    </div>
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Concert ref</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{concert_detail["concert_ref"]}</div>
                                    </div>
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Conductor</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{concert_detail["conductor"]}</div>
                                    </div>
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Concert date</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{concert_detail["concert_date"]}</div>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <div id="concert-step-musicians" style="min-width:0; width:100%; max-width:100%; overflow:hidden;">
                            {section_markup}
                        </div>

                        <div style="display:grid; gap:18px; min-width:0; width:100%;">
                            <div style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Tier Seating & Pricing</div>
                                <div style="display:grid; grid-template-columns:auto 1fr auto; gap:12px; align-items:center; margin-bottom:14px;">
                                    <a href="{tier_minus_url}" style="display:inline-flex; align-items:center; justify-content:center; width:44px; height:44px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; text-decoration:none; font-size:24px; line-height:1;">−</a>
                                    <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927; text-align:center;">
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Current tier</div>
                                        <div style="font-size:24px; color:#f2f0ea;">Tier {current_tier}</div>
                                    </div>
                                    <a href="{tier_plus_url}" style="display:inline-flex; align-items:center; justify-content:center; width:44px; height:44px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; text-decoration:none; font-size:24px; line-height:1;">+</a>
                                </div>
                                <div style="display:grid; grid-template-columns:1fr; gap:12px;">
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Seat limit</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{tier_seat_limit}</div>
                                    </div>
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">First send</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">${int(tier_pricing["first"])}</div>
                                    </div>
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Repeat sends</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">${int(tier_pricing["repeat"])}</div>
                                    </div>
                                </div>
                            </div>

                            <div style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Concert Totals</div>
                                <div style="display:grid; grid-template-columns:1fr; gap:12px;">
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Selected musicians</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{concert_detail["selected_musicians"]}</div>
                                    </div>
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Available musicians</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{concert_detail["available_musicians"]}</div>
                                    </div>
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Seats left</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{concert_detail["seats_left"]}</div>
                                    </div>
                                    <div>
                                        <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Venue</div>
                                        <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">{venue_display}</div>
                                    </div>
                                </div>
                            </div>

                                                        <div style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Current File Source</div>
                                <div style="padding:16px 18px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#c8d0d8; font-size:16px; line-height:1.75;">
                                    Concert Control remains the active authority point for the live concert file, recipient validity, resend authority, and update release.
                                </div>
                            </div>

                            <div style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Conductor Upload Receipt Board</div>
                                <div style="overflow:auto;">
                                    <table style="width:100%; border-collapse:collapse; background:#0d1927; border:1px solid #31455c;">
                                        <thead>
                                            <tr>
                                                <th style="padding:12px 8px; text-align:left; border-bottom:1px solid #31455c; color:#e5dccb; font-size:13px; background:#0b1622;">Concert</th>
                                                <th style="padding:12px 8px; text-align:left; border-bottom:1px solid #31455c; color:#e5dccb; font-size:13px; background:#0b1622;">File</th>
                                                <th style="padding:12px 8px; text-align:left; border-bottom:1px solid #31455c; color:#e5dccb; font-size:13px; background:#0b1622;">Send state</th>
                                                <th style="padding:12px 8px; text-align:left; border-bottom:1px solid #31455c; color:#e5dccb; font-size:13px; background:#0b1622;">Forward status</th>
                                                <th style="padding:12px 8px; text-align:left; border-bottom:1px solid #31455c; color:#e5dccb; font-size:13px; background:#0b1622;">Updated at</th>
                                                <th style="padding:12px 8px; text-align:left; border-bottom:1px solid #31455c; color:#e5dccb; font-size:13px; background:#0b1622;">Note</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {conductor_alert_rows}
                                        </tbody>
                                    </table>
                                </div>
                            </div>

                            <div id="concert-step-files" style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Current File Upload</div>
                                <div style="font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#c8d0d8; margin-bottom:6px;">Upload or replace</div>
                                <form method="post" action="/control/current_file/upload?email={quote_plus('librarian@local')}" enctype="multipart/form-data" style="margin:0;">
                                    <input type="hidden" name="concert_name" value="{concert_detail['concert_name']}">
                                    <input type="hidden" name="concert_ref" value="{concert_detail['concert_ref']}">
                                    <div style="display:flex; gap:12px; flex-wrap:wrap; margin-bottom:12px;">
                                        <input type="file" name="current_file_upload" required style="flex:1 1 320px; padding:12px 14px; border-radius:12px; border:1px solid #31455c; background:#0d1927; color:#c8d0d8; font-family:Georgia, 'Times New Roman', serif; font-size:15px;">
                                        <button type="submit" style="padding:12px 18px; border-radius:999px; border:1px solid #b89457; background:linear-gradient(to bottom, #173247, #102435); color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif; font-size:15px;">Upload Current File</button>
                                    </div>
                                </form>
                                <div style="padding:14px 16px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#c8d0d8; font-size:15px; line-height:1.7;">
                                    When a new file is sent from this page, the older musician-facing file should be flushed from access. Musicians should only ever reach the latest current file for this concert.
                                </div>
                            </div>

                            <div id="concert-step-issue" style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Concert Distribution</div>
                                <select style="width:100%; padding:12px 14px; border-radius:12px; border:1px solid #31455c; background:#0d1927; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif; font-size:16px; margin-bottom:12px;">
                                    {concert_file_options_html}
                                </select>
                                <div style="padding:14px 16px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#c8d0d8; font-size:15px; line-height:1.7;">
                                    Current authorised file: <span style="color:#f2f0ea;">{concert_detail["current_file_label"]}</span>
                                </div>
                                <div style="margin-top:12px; max-height:240px; overflow:auto;">
                                    {concert_file_list_html}
                                </div>
                            </div>

                            <div id="concert-step-conductor" style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Musician Access Rule</div>
                                <div style="display:grid; grid-template-columns:1fr; gap:12px;">
                                    <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">Accepted once only unless librarian resends.</div>
                                    <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">Only active musicians in this concert retain access.</div>
                                    <div style="padding:12px 14px; border:1px solid #31455c; border-radius:12px; background:#0d1927;">Only the latest current file should be available.</div>
                                </div>
                            </div>

                            <div style="background:#101e2d; border:1px solid #223548; border-radius:16px; padding:18px;">
                                <div style="font-size:24px; color:#74d3de; margin-bottom:12px;">Concert File Rule</div>
                                <div style="padding:16px 18px; border:1px solid #31455c; border-radius:12px; background:#0d1927; color:#c8d0d8; font-size:16px; line-height:1.75;">
                                    This concert page is the authority point. If the librarian issues a new current file from here, the previous musician-facing file should no longer be reachable. Musicians may only access the live current concert file while they remain valid inside the selected concert pool.
                                </div>
                            </div>
                        </div>

                    </div>

                </div>
            </div>
        </div>

        <script>
        function concertControlMarkStep(stepId) {{
            try {{
                localStorage.setItem(stepId, "done");
            }} catch (e) {{}}
            concertControlApplyStepState(stepId);
        }}

        function concertControlApplyStepState(stepId) {{
            const link = document.getElementById(stepId);
            if (!link) {{
                return;
            }}
            link.style.color = "#3d8d57";
        }}

        function concertControlLoadStepState() {{
            const stepIds = [
                "concert-step-link-musicians",
                "concert-step-link-files",
                "concert-step-link-issue",
                "concert-step-link-conductor"
            ];

            for (const stepId of stepIds) {{
                try {{
                    if (localStorage.getItem(stepId) === "done") {{
                        concertControlApplyStepState(stepId);
                    }}
                }} catch (e) {{}}
            }}
        }}

        document.addEventListener("DOMContentLoaded", concertControlLoadStepState);
        </script>
    </body>
    </html>
    """
    return page_html


@app.get("/control/conductor_ensemble_pdf")
def concert_control_conductor_ensemble_pdf(request: Request):
    from io import BytesIO
    from fastapi.responses import StreamingResponse, PlainTextResponse

    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.lib.utils import ImageReader
        from reportlab.pdfgen import canvas
    except Exception as e:
        return PlainTextResponse(f"PDF export is not available: {e}", status_code=500)

    librarian_email = (request.query_params.get("email") or "").strip() or "librarian@local"
    concert_name = concert_control_detail_normalize_concert_name(request.query_params.get("concert_name") or "")
    concert_ref = concert_control_detail_normalize_concert_ref(request.query_params.get("concert_ref") or "")

    if not concert_name:
        return PlainTextResponse("Concert name missing.", status_code=400)

    concert_detail = concert_control_detail_fetch(concert_name, concert_ref)

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COALESCE(concert_name, '') AS concert_name,
                COALESCE(concert_ref, '') AS concert_ref,
                COALESCE(concert_date, '') AS concert_date,
                COALESCE(ensemble_name, '') AS ensemble_name,
                COALESCE(venue_name, '') AS venue_name,
                COALESCE(conductor_name, '') AS conductor_name,
                COALESCE(concert_tier, 1) AS concert_tier,
                COALESCE(created_at, '') AS created_at,
                COALESCE(updated_at, '') AS updated_at
            FROM librarian_created_concerts
            WHERE lower(trim(COALESCE(concert_name, '')))=lower(?)
              AND lower(trim(COALESCE(concert_ref, '')))=lower(?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (concert_name, concert_ref),
        )
        concert_row = cur.fetchone()
    finally:
        if conn is not None:
            conn.close()

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    page_width, page_height = A4

    left = 18 * mm
    right = page_width - (18 * mm)
    top = page_height - (18 * mm)
    y = top

    logo_path = STATIC_DIR / "annotatio_logo.png"
    if logo_path.exists():
        try:
            logo = ImageReader(str(logo_path))
            logo_width = 78 * mm
            logo_height = 18 * mm
            pdf.drawImage(
                logo,
                left,
                y - logo_height,
                width=logo_width,
                height=logo_height,
                preserveAspectRatio=True,
                mask="auto",
            )
            y -= 24 * mm
        except Exception:
            y -= 6 * mm

    pdf.setTitle("Annotatio Concert Ensemble Setup")
    pdf.setFont("Helvetica-Bold", 18)
    pdf.drawString(left, y, "Conductor's Orchestral Ensemble Setup")
    y -= 8 * mm

    pdf.setStrokeColorRGB(0.72, 0.58, 0.34)
    pdf.line(left, y, left + (70 * mm), y)
    y -= 8 * mm

    def new_page():
        nonlocal y
        pdf.showPage()
        y = top

    def write_label_value(label: str, value: str):
        nonlocal y
        if y < 28 * mm:
            new_page()
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(left, y, label)
        y -= 5 * mm
        pdf.setFont("Helvetica", 10)

        text = str(value or "—")
        chunk_size = 100
        chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)] or ["—"]
        for chunk in chunks:
            if y < 20 * mm:
                new_page()
                pdf.setFont("Helvetica", 10)
            pdf.drawString(left, y, chunk)
            y -= 4.8 * mm
        y -= 1.8 * mm

    display_concert_name = str((concert_row["concert_name"] if concert_row else concert_detail["concert_name"]) or "Not yet set.").strip()
    display_concert_ref = str((concert_row["concert_ref"] if concert_row else concert_detail["concert_ref"]) or "Not yet set.").strip()
    display_concert_date = str((concert_row["concert_date"] if concert_row else concert_detail["concert_date"]) or "Not yet set.").strip()
    display_ensemble = str((concert_row["ensemble_name"] if concert_row else "") or "Not yet set.").strip()
    display_venue = str((concert_row["venue_name"] if concert_row else "") or "Not yet set.").strip()
    display_conductor = str((concert_row["conductor_name"] if concert_row else concert_detail["conductor"]) or "Not yet set.").strip()
    display_tier = int((concert_row["concert_tier"] if concert_row else concert_detail.get("concert_tier", 1)) or 1)
    display_generated_at = datetime.utcnow().strftime("%d %B %Y %H:%M UTC")
    display_file_status = str(concert_detail.get("current_file_label") or "No current authorised file selected").strip()

    write_label_value("Concert name", display_concert_name)
    write_label_value("Concert ref", display_concert_ref)
    write_label_value("Concert date", display_concert_date)
    write_label_value("Ensemble", display_ensemble)
    write_label_value("Venue", display_venue)
    write_label_value("Conductor", display_conductor)
    write_label_value("Tier", f"Tier {display_tier}")
    write_label_value("Selected musicians", str(concert_detail.get("selected_musicians", 0) or 0))
    write_label_value("Current file status", display_file_status)
    write_label_value("Generated", display_generated_at)

    for section_name in CONCERT_CONTROL_SECTIONS:
        section_rows = concert_detail["section_rows"].get(section_name, [])
        section_count = concert_detail["section_counts"].get(section_name, {"selected": 0, "available": 0, "qty": 0})

        if y < 34 * mm:
            new_page()

        pdf.setFont("Helvetica-Bold", 12)
        pdf.drawString(left, y, f"{section_name} — {int(section_count.get('selected', 0) or 0)}")
        y -= 6 * mm

        if not section_rows:
            pdf.setFont("Helvetica", 10)
            pdf.drawString(left, y, "No musicians listed.")
            y -= 6 * mm
            continue

        for row in section_rows:
            if y < 20 * mm:
                new_page()

            musician_name = str(row.get("musician_name") or "Unknown musician").strip()
            instrument = str(row.get("instrument") or section_name).strip()
            musician_email = str(row.get("musician_email") or "—").strip()
            confirmed = str(row.get("confirmed") or "No").strip()

            pdf.setFont("Helvetica-Bold", 10)
            pdf.drawString(left, y, musician_name)
            y -= 4.8 * mm

            pdf.setFont("Helvetica", 10)
            pdf.drawString(left + (4 * mm), y, f"{instrument} | {musician_email} | Confirmed: {confirmed}")
            y -= 5.5 * mm

        y -= 2 * mm

    pdf.save()
    buffer.seek(0)

    safe_name = "_".join(display_concert_name.split()) or "concert_setup"
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{safe_name}_ensemble_setup.pdf"'},
    )


@app.get("/control", response_class=HTMLResponse)
def concert_control_page(request: Request):
    librarian_email = (request.query_params.get("email") or "").strip() or "librarian@local"
    concert_name = str(request.query_params.get("concert_name") or "").strip()
    concert_ref = str(request.query_params.get("concert_ref") or "").strip()
    focus_section = str(request.query_params.get("focus_section") or "").strip()
    highlight_unopened = str(request.query_params.get("highlight_unopened") or "").strip()
    tier_adjust = str(request.query_params.get("tier_adjust") or "").strip().lower()
    automatic_forwarding = str(request.query_params.get("automatic_forwarding") or "").strip().lower()

    clean_concert_name = concert_control_detail_normalize_concert_name(concert_name)
    clean_concert_ref = concert_control_detail_normalize_concert_ref(concert_ref)

    if tier_adjust.startswith("set_"):
        requested_tier_text = tier_adjust.replace("set_", "", 1)
        try:
            requested_tier = int(requested_tier_text)
        except Exception:
            requested_tier = 1
        concert_control_tier_update(
            concert_name=clean_concert_name,
            concert_ref=clean_concert_ref,
            new_tier=requested_tier,
        )

    if automatic_forwarding in {"on", "off"} and clean_concert_name:
        concert_control_forwarding_state_upsert(
            concert_name=clean_concert_name,
            concert_ref=clean_concert_ref,
            automatic_forwarding_enabled=1 if automatic_forwarding == "on" else 0,
        )

    venue_names = get_librarian_dashboard_venues(librarian_email)
    page_html = render_concert_control_page(
        venue_names=venue_names,
        concert_name=clean_concert_name,
        concert_ref=clean_concert_ref,
        focus_section=focus_section,
        highlight_unopened=highlight_unopened,
        librarian_email=librarian_email,
    )
    return HTMLResponse(page_html)


@app.post("/control/current_file/upload")
async def concert_control_current_file_upload(
    request: Request,
    concert_name: str = Form(""),
    concert_ref: str = Form(""),
    current_file_upload: UploadFile | None = File(None),
):
    librarian_email = (request.query_params.get("email") or "").strip() or "librarian@local"
    clean_concert_name = " ".join(str(concert_name or "").strip().split())
    clean_concert_ref = " ".join(str(concert_ref or "").strip().split())
    return_target = (
        f"/control?email={quote_plus(librarian_email)}"
        f"&concert_name={quote_plus(clean_concert_name)}"
        f"&concert_ref={quote_plus(clean_concert_ref)}"
        f"#concert-step-files"
    )

    if not current_file_upload:
        return RedirectResponse(
            return_target,
            status_code=303,
        )

    file_name = Path(str(current_file_upload.filename or "").strip()).name
    if not file_name:
        return RedirectResponse(
            return_target,
            status_code=303,
        )

    file_bytes = await current_file_upload.read()
    if not file_bytes:
        return RedirectResponse(
            return_target,
            status_code=303,
        )

    concert_control_current_file_store(
        librarian_email=librarian_email,
        concert_name=clean_concert_name,
        concert_ref=clean_concert_ref,
        original_filename=file_name,
        file_bytes=file_bytes,
    )

    return RedirectResponse(
        return_target,
        status_code=303,
    )


@app.get("/control/current_file/{file_id}/download")
def concert_control_current_file_download(file_id: int):
    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
        SELECT
            id,
            COALESCE(original_filename, '') AS original_filename,
            COALESCE(stored_rel_path, '') AS stored_rel_path
        FROM concert_control_current_files
        WHERE id=?
        LIMIT 1
        """, (int(file_id),))
        row = cur.fetchone()
    finally:
        if conn is not None:
            conn.close()

    if not row:
        return HTMLResponse("File not found.", status_code=404)

    stored_path = APP_DIR / str(row["stored_rel_path"] or "").strip()
    if not stored_path.exists():
        return HTMLResponse("Stored file not found.", status_code=404)

    file_name = str(row["original_filename"] or "").strip() or stored_path.name
    return HTMLResponse(
        f'<html><body style="margin:0;"><iframe src="/static/../{row["stored_rel_path"]}" style="width:100%; height:100vh; border:none;"></iframe></body></html>'
    ) if file_name.lower().endswith(".pdf") else RedirectResponse(f"/static/../{row['stored_rel_path']}", status_code=302)


@app.get("/librarian/concert/{concert_key}", response_class=HTMLResponse)
def concert_control_open_redirect(request: Request, concert_key: str):
    librarian_email = (request.query_params.get("email") or "").strip() or "librarian@local"
    concert_name, concert_ref = concert_control_route_key_parse(concert_key)
    target = (
        f"/control?email={quote_plus(librarian_email)}"
        f"&concert_name={quote_plus(concert_name)}"
        f"&concert_ref={quote_plus(concert_ref)}"
    )
    return RedirectResponse(target, status_code=303)


@app.post("/control/current_file/{file_id}/delete")
def concert_control_current_file_delete(
    request: Request,
    file_id: int,
):
    librarian_email = (request.query_params.get("email") or "").strip() or "librarian@local"
    concert_name = str(request.query_params.get("concert_name") or "").strip()
    concert_ref = str(request.query_params.get("concert_ref") or "").strip()
    clean_concert_name = " ".join(str(concert_name or "").strip().split())
    clean_concert_ref = " ".join(str(concert_ref or "").strip().split())

    conn = None
    stored_rel_path = ""

    try:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                COALESCE(stored_rel_path, '') AS stored_rel_path
            FROM concert_control_current_files
            WHERE id=?
            LIMIT 1
            """,
            (int(file_id),),
        )
        row = cur.fetchone()

        if row:
            stored_rel_path = str(row["stored_rel_path"] or "").strip()
            cur.execute(
                """
                DELETE FROM concert_control_current_files
                WHERE id=?
                """,
                (int(file_id),),
            )
            conn.commit()
    finally:
        if conn is not None:
            conn.close()

    if stored_rel_path:
        stored_path = APP_DIR / stored_rel_path
        try:
            if stored_path.exists():
                stored_path.unlink()
        except Exception:
            pass

    return RedirectResponse(
        f"/control?email={quote_plus(librarian_email)}&concert_name={quote_plus(clean_concert_name)}&concert_ref={quote_plus(clean_concert_ref)}#concert-step-files",
        status_code=303,
    )


@app.get("/librarian/concert/{concert_key}/assign", response_class=HTMLResponse)
def concert_control_assign_redirect(request: Request, concert_key: str):
    librarian_email = (request.query_params.get("email") or "").strip() or "librarian@local"
    focus_section = str(request.query_params.get("focus_section") or "").strip()
    highlight_unopened = str(request.query_params.get("highlight_unopened") or "").strip()
    concert_name, concert_ref = concert_control_route_key_parse(concert_key)

    target = (
        f"/control?email={quote_plus(librarian_email)}"
        f"&concert_name={quote_plus(concert_name)}"
        f"&concert_ref={quote_plus(concert_ref)}"
    )

    if focus_section:
        target += f"&focus_section={quote_plus(focus_section)}"

    if highlight_unopened:
        target += f"&highlight_unopened={quote_plus(highlight_unopened)}"

    return RedirectResponse(target, status_code=303)


# COUNTRY-SHARED VENUES — GLOBAL FOUNDATION

def normalize_country_code(country_code) -> str:
    return str(country_code or "").strip().upper()


def global_venue_registry_normalize_text(value) -> str:
    return " ".join(str(value or "").strip().split())


def global_venue_registry_make_key(venue_name: str) -> str:
    return global_venue_registry_normalize_text(venue_name).lower()


def get_librarian_country_code_by_email(librarian_email: str) -> str:
    email = str(librarian_email or "").strip().lower()
    if not email:
        return ""

    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT o.country
            FROM users u
            LEFT JOIN organisations o ON o.id = u.organisation_id
            WHERE lower(u.email)=lower(?)
              AND u.role='librarian'
            LIMIT 1
            """,
            (email,),
        )
        row = cur.fetchone()

        if row and normalize_country_code(row["country"]):
            return normalize_country_code(row["country"])

        if email == "librarian@local":
            return "NZ"

        return ""
    except Exception:
        if email == "librarian@local":
            return "NZ"
        return ""
    finally:
        if conn is not None:
            conn.close()


def global_venue_registry_fetch_country_rows(country_code: str, search_text: str = "") -> list[sqlite3.Row]:
    code = normalize_country_code(country_code)
    if not code:
        return []

    search_value = global_venue_registry_normalize_text(search_text).lower()
    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        if search_value:
            like_value = f"%{search_value}%"
            cur.execute(
                """
                SELECT
                    id,
                    country_code,
                    venue_name,
                    city,
                    capacity,
                    added_by_user_id,
                    is_system_seeded,
                    is_active,
                    created_at,
                    updated_at
                FROM global_country_venue_registry
                WHERE country_code=?
                  AND is_active=1
                  AND (
                      lower(venue_name) LIKE ?
                      OR lower(COALESCE(city, '')) LIKE ?
                  )
                ORDER BY
                    lower(venue_name),
                    lower(COALESCE(city, '')),
                    id
                """,
                (code, like_value, like_value),
            )
        else:
            cur.execute(
                """
                SELECT
                    id,
                    country_code,
                    venue_name,
                    city,
                    capacity,
                    added_by_user_id,
                    is_system_seeded,
                    is_active,
                    created_at,
                    updated_at
                FROM global_country_venue_registry
                WHERE country_code=?
                  AND is_active=1
                ORDER BY
                    lower(venue_name),
                    lower(COALESCE(city, '')),
                    id
                """,
                (code,),
            )

        return cur.fetchall()
    finally:
        if conn is not None:
            conn.close()


def global_venue_registry_fetch_country_names(country_code: str) -> list[str]:
    rows = global_venue_registry_fetch_country_rows(country_code)
    venue_names = []
    seen = set()

    for row in rows:
        venue_name = global_venue_registry_normalize_text(row["venue_name"])
        if not venue_name:
            continue

        venue_key = venue_name.casefold()
        if venue_key in seen:
            continue

        seen.add(venue_key)
        venue_names.append(venue_name)

    return venue_names


def global_venue_registry_add_for_librarian(
    librarian_email: str,
    venue_name: str,
    city: str = "",
    capacity: int | None = None,
) -> tuple[bool, str]:
    email = str(librarian_email or "").strip().lower()
    clean_venue_name = global_venue_registry_normalize_text(venue_name)
    clean_city = global_venue_registry_normalize_text(city)
    venue_key = global_venue_registry_make_key(clean_venue_name)

    if not email:
        return False, "Librarian email is required."
    if not clean_venue_name:
        return False, "Venue name is required."

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT u.id AS user_id, u.role, o.country
            FROM users u
            LEFT JOIN organisations o ON o.id = u.organisation_id
            WHERE lower(u.email)=lower(?)
            LIMIT 1
            """,
            (email,),
        )
        librarian_row = cur.fetchone()

        if not librarian_row:
            return False, "Librarian not found."
        if (librarian_row["role"] or "").strip().lower() != "librarian":
            return False, "Only librarians may add venues."

        country_code = normalize_country_code(librarian_row["country"])
        if not country_code:
            return False, "No librarian country exists yet."

        now = datetime.utcnow().isoformat()

        cur.execute(
            """
            INSERT INTO global_country_venue_registry (
                country_code,
                venue_name,
                venue_key,
                city,
                capacity,
                added_by_user_id,
                is_system_seeded,
                is_active,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, 1, ?, ?)
            ON CONFLICT(country_code, venue_key)
            DO UPDATE SET
                venue_name=excluded.venue_name,
                city=excluded.city,
                capacity=excluded.capacity,
                is_active=1,
                updated_at=excluded.updated_at
            """,
            (
                country_code,
                clean_venue_name,
                venue_key,
                clean_city or None,
                capacity,
                librarian_row["user_id"],
                now,
                now,
            ),
        )

        conn.commit()
        return True, "Venue saved."
    finally:
        if conn is not None:
            conn.close()


def global_venue_registry_archive_by_admin(admin_email: str, venue_id: int) -> tuple[bool, str]:
    email = str(admin_email or "").strip().lower()
    if not email:
        return False, "Admin email is required."

    conn = None
    try:
        conn = db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id, role
            FROM users
            WHERE lower(email)=lower(?)
            LIMIT 1
            """,
            (email,),
        )
        admin_row = cur.fetchone()

        if not admin_row:
            return False, "Admin not found."
        if (admin_row["role"] or "").strip().lower() != "admin":
            return False, "Only admin may archive venues."

        now = datetime.utcnow().isoformat()
        cur.execute(
            """
            UPDATE global_country_venue_registry
            SET is_active=0,
                updated_at=?
            WHERE id=?
            """,
            (now, int(venue_id)),
        )

        if cur.rowcount == 0:
            return False, "Venue not found."

        conn.commit()
        return True, "Venue archived."
    finally:
        if conn is not None:
            conn.close()


def get_librarian_country_shared_venues(librarian_country_code) -> list[str]:
    code = normalize_country_code(librarian_country_code)
    if not code:
        return []

    return global_venue_registry_fetch_country_names(code)


def get_librarian_dashboard_venues(librarian_email: str) -> list[str]:
    librarian_country_code = get_librarian_country_code_by_email(librarian_email)
    if not librarian_country_code:
        return []

    return get_librarian_country_shared_venues(librarian_country_code)


@app.get("/venues/search", response_class=HTMLResponse)
def global_venue_registry_search(request: Request):
    librarian_email = (request.query_params.get("email") or "").strip()
    search_text = (request.query_params.get("q") or "").strip()
    country_code = get_librarian_country_code_by_email(librarian_email)
    rows = global_venue_registry_fetch_country_rows(country_code, search_text)

    html_out = """
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Annotatio — Country Venue Search</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
    </head>
    <body style="margin:0; background:#071018; color:#f2f0ea; font-family:Georgia, 'Times New Roman', serif;">
        <div style="max-width:1200px; margin:18px auto; padding:0 18px;">
            <div style="background:#0b1622; border:1px solid #223548; border-radius:18px; overflow:hidden;">
                <div style="padding:18px 24px; text-align:center; border-bottom:1px solid #223548;">
                    <div style="font-size:34px; color:#74d3de;">Country Venue Search</div>
                </div>
                <div style="padding:20px 24px 28px 24px;">
    """

    if not country_code:
        html_out += """
                    <div style="padding:16px; border:1px dashed #31455c; border-radius:12px; background:#101e2d; color:#c8d0d8;">
                        No librarian country exists yet.
                    </div>
        """
    elif rows:
        for row in rows:
            capacity_display = row["capacity"] if row["capacity"] is not None else "Not set"
            seeded_display = "System seeded" if int(row["is_system_seeded"] or 0) == 1 else "Librarian added"
            html_out += f"""
                    <div style="padding:14px 16px; margin-bottom:12px; border:1px solid #223548; border-radius:12px; background:#101e2d;">
                        <div style="font-size:20px; color:#74d3de; margin-bottom:6px;">{row['venue_name']}</div>
                        <div style="font-size:15px; color:#d8e1e8;">
                            City: {row['city'] or 'Not set'}<br>
                            Capacity: {capacity_display}<br>
                            Type: {seeded_display}
                        </div>
                    </div>
            """
    else:
        html_out += """
                    <div style="padding:16px; border:1px dashed #31455c; border-radius:12px; background:#101e2d; color:#c8d0d8;">
                        No venues found in this librarian country.
                    </div>
        """

    html_out += """
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(html_out)


@app.post("/venues/add")
def global_venue_registry_add_route(
    librarian_email: str = Form(...),
    venue_name: str = Form(...),
    city: str = Form(""),
    capacity: int | None = Form(None),
):
    ok, message = global_venue_registry_add_for_librarian(
        librarian_email=librarian_email,
        venue_name=venue_name,
        city=city,
        capacity=capacity,
    )
    if not ok:
        return HTMLResponse(message, status_code=400)

    return RedirectResponse(f"/venues/search?email={librarian_email}", status_code=303)


@app.post("/venues/archive")
def global_venue_registry_archive_route(
    admin_email: str = Form(...),
    venue_id: int = Form(...),
):
    ok, message = global_venue_registry_archive_by_admin(admin_email, venue_id)
    if not ok:
        return HTMLResponse(message, status_code=400)

    return HTMLResponse(message)
