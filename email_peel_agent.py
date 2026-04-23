"""
Email Peel Region Water Bill Agent
====================================
Scans your email inbox for Region of Peel water bill notifications,
extracts bill summary data (bill number, amount due, due date, etc.), and
exports everything to a Google Sheet — SUMMARY DATA ONLY.

Works with:
  - Email BODY text (HTML is converted to plain text automatically)
  - Email ATTACHMENTS (images / PDFs — OCR'd then extracted)

Configurable via .env and a JSON extraction config file.
"""

import os
import io
import re
import json
import hashlib
import gspread
import traceback
import time
import fitz  # PyMuPDF
from html.parser import HTMLParser
from datetime import datetime
from PIL import Image
from imap_tools import MailBox, AND
from dotenv import load_dotenv

import langextract as lx
from google import genai
from google.genai import types

# Load variables from .env file
load_dotenv()

# ──────────────────────────────────────────────
# Configuration & Secrets
# ──────────────────────────────────────────────
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
CREDS_FILE = "service_account.json"

EMAIL_HOST = os.environ.get("EMAIL_HOST", "imap.gmail.com")
EMAIL_USER = os.environ.get("EMAIL_USER", "")
EMAIL_PASS = os.environ.get("EMAIL_PASS", "")

# Who to scan emails from
PEEL_SENDER = os.environ.get("PEEL_SENDER", "")
PEEL_START_DATE = os.environ.get("PEEL_START_DATE", "2024-01-01")
PEEL_END_DATE = os.environ.get("PEEL_END_DATE", "2026-03-18")

# Google Sheet settings
PEEL_SHEET_URL = os.environ.get("PEEL_SHEET_URL", os.environ.get("SHEET_URL", ""))
PEEL_TAB_SUMMARY = os.environ.get("PEEL_TAB_SUMMARY", "Peel_Water_Bills")
PEEL_TAB_TRACKER = os.environ.get("PEEL_TAB_TRACKER", "peel_tracker")

# Extraction config file
PEEL_CONFIG_FILE = os.environ.get("PEEL_EXTRACTION_CONFIG", "extraction_config_peel.json")

os.environ["GEMINI_API_KEY"] = GEMINI_API_KEY


# ──────────────────────────────────────────────
# Load Extraction Config
# ──────────────────────────────────────────────
def load_extraction_config(config_path: str) -> dict:
    """Loads the extraction field config from a JSON file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_extraction_config(PEEL_CONFIG_FILE)


# ──────────────────────────────────────────────
# HTML → Plain Text Converter
# ──────────────────────────────────────────────
class HTMLTextExtractor(HTMLParser):
    """Strips HTML tags and returns clean plain text."""
    def __init__(self):
        super().__init__()
        self._text = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip = True
        elif tag in ("br", "p", "div", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._text.append("\n")
        elif tag == "td":
            self._text.append("\t")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = False
        elif tag in ("p", "div", "tr", "table", "ul", "ol"):
            self._text.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._text.append(data)

    def get_text(self):
        raw = "".join(self._text)
        lines = [re.sub(r'[ \t]+', ' ', line).strip() for line in raw.splitlines()]
        cleaned = []
        blank_count = 0
        for line in lines:
            if not line:
                blank_count += 1
                if blank_count <= 2:
                    cleaned.append("")
            else:
                blank_count = 0
                cleaned.append(line)
        return "\n".join(cleaned).strip()


def html_to_text(html: str) -> str:
    """Convert HTML email body to clean plain text."""
    parser = HTMLTextExtractor()
    parser.feed(html)
    return parser.get_text()


# ──────────────────────────────────────────────
# OCR for attachments (images / PDFs)
# ──────────────────────────────────────────────
def ocr_image(image: Image.Image, api_key: str) -> str:
    """Uses Gemini to OCR an image with retry logic."""
    client = genai.Client(api_key=api_key)
    buffered = io.BytesIO()
    if image.mode in ("RGBA", "P"):
        image = image.convert("RGB")
    image.save(buffered, format="JPEG")

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[
                    "Transcribe exactly all the text from this image. Maintain lines and layout as best as possible. Do not interpret data, just OCR.",
                    types.Part.from_bytes(data=buffered.getvalue(), mime_type="image/jpeg"),
                ],
            )
            return response.text
        except Exception as e:
            if "503" in str(e) and attempt < max_retries - 1:
                print(f"      ⏳ Gemini busy (503), retrying in {2**(attempt+1)}s...")
                time.sleep(2**(attempt + 1))
                continue
            raise e


# ──────────────────────────────────────────────
# Gemini-based Extraction (for email body text)
# ──────────────────────────────────────────────
def extract_with_gemini(text: str, api_key: str, config: dict) -> dict:
    """
    Uses Gemini directly to extract structured bill data from email text.
    Falls back to LangExtract if Gemini structured extraction fails.
    """
    fields_cfg = config.get("fields", {})

    # Build a JSON schema description for Gemini
    field_descriptions = []
    for name, info in fields_cfg.items():
        field_descriptions.append(f'  "{name}": "<{info.get("label", name)}>"')

    schema = "{\n" + ",\n".join(field_descriptions) + "\n}"

    prompt = f"""You are an expert data extraction assistant specializing in water utility bills.
Extract all billing information from the following Region of Peel water bill email or bill text.

INSTRUCTIONS:
- {config.get("prompt", "Extract all bill details.")}
- Return ONLY valid JSON, no markdown, no explanation.
- If a field is not found, use an empty string "".
- For dates, always use YYYY-MM-DD format.
- For monetary amounts, return numbers only (no $ sign).
- For water consumption, return the number only (no units).

EXPECTED JSON FORMAT:
{schema}

EMAIL / BILL TEXT:
\"\"\"
{text}
\"\"\"

Return ONLY the JSON:"""

    client = genai.Client(api_key=api_key)

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[prompt],
            )

            # Parse the response — strip any markdown fences
            response_text = response.text.strip()
            if response_text.startswith("```"):
                response_text = re.sub(r'^```(?:json)?\s*\n?', '', response_text)
                response_text = re.sub(r'\n?```\s*$', '', response_text)

            data = json.loads(response_text)
            return data

        except json.JSONDecodeError as e:
            print(f"      ⚠️ JSON parse error (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            # Fall back to LangExtract
            print("      🔄 Falling back to LangExtract...")
            return extract_with_langextract(text, api_key, config)
        except Exception as e:
            if "503" in str(e) and attempt < max_retries - 1:
                print(f"      ⏳ Gemini busy (503), retrying in {2**(attempt+1)}s...")
                time.sleep(2**(attempt + 1))
                continue
            raise e


def extract_with_langextract(text: str, api_key: str, config: dict):
    """Fallback: Uses LangExtract to extract structured details based on config."""
    example_cfg = config["example"]

    lx_extractions = []
    for ext in example_cfg["extractions"]:
        attrs = ext.get("attributes", None)
        if attrs:
            lx_extractions.append(lx.data.Extraction(ext["class"], ext["text"], attributes=attrs))
        else:
            lx_extractions.append(lx.data.Extraction(ext["class"], ext["text"]))

    examples = [
        lx.data.ExampleData(
            text=example_cfg["text"],
            extractions=lx_extractions
        )
    ]

    results = lx.extract(
        text_or_documents=text,
        prompt_description=config["prompt"],
        examples=examples,
        api_key=api_key,
        model_id="gemini-2.5-flash",
        temperature=0.0
    )

    # Convert LangExtract results to dict format
    fields_cfg = config.get("fields", {})
    processed = {field: "" for field in fields_cfg}

    if hasattr(results, "extractions") and results.extractions:
        for ext_obj in results.extractions:
            cls = ext_obj.extraction_class
            text_val = ext_obj.extraction_text
            if cls in fields_cfg:
                processed[cls] = text_val

    return processed


# ──────────────────────────────────────────────
# Google Sheets Helpers
# ──────────────────────────────────────────────
def get_or_create_tab(sheet, tab_name: str, header_row: list = None, cols: int = 20):
    """Get or create a worksheet tab with optional header."""
    try:
        ws = sheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows="2000", cols=str(cols))
        if header_row:
            ws.append_row(header_row)
    return ws


def generate_email_id(msg_subject: str, msg_date, body_hash: str) -> str:
    """Generate a unique ID for an email to prevent duplicate processing."""
    date_str = str(msg_date.date()) if hasattr(msg_date, 'date') else str(msg_date)
    raw = f"{msg_subject}_{date_str}_{body_hash}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def check_if_processed(email_id: str, creds_path: str, sheet_url: str, tracker_tab: str) -> bool:
    """Check if this email has already been processed."""
    try:
        gc = gspread.service_account(filename=creds_path)
        sheet = gc.open_by_url(sheet_url)
        tracker = get_or_create_tab(sheet, tracker_tab, ["Email_ID", "Subject", "Date", "Processed_At"])

        records = tracker.get_all_values()
        existing_ids = {row[0].strip() for row in records if row}
        return email_id in existing_ids
    except Exception:
        return False


def append_bill_to_sheet(processed_data: dict, email_subject: str, email_date,
                         email_id: str, creds_path: str, sheet_url: str,
                         summary_tab: str, tracker_tab: str,
                         config: dict):
    """Write extracted Peel Region water bill summary data to Google Sheets (summary only)."""
    gc = gspread.service_account(filename=creds_path)
    sheet_doc = gc.open_by_url(sheet_url)

    # Get/create tabs
    tracker_sheet = get_or_create_tab(sheet_doc, tracker_tab, ["Email_ID", "Subject", "Date", "Processed_At"])

    summary_cols = config.get("summary_columns", [])
    summary_cols_with_ts = summary_cols + ["Processed At"]
    summary_sheet = get_or_create_tab(sheet_doc, summary_tab, summary_cols_with_ts, cols=len(summary_cols_with_ts))

    # ── Summary row (this IS the main output — summary data only) ──
    summary_field_order = config.get("summary_field_order", [])
    summary_row = [processed_data.get(f, "") for f in summary_field_order]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary_row.append(timestamp)
    summary_sheet.append_row(summary_row)

    # ── Track this email ──
    date_str = str(email_date.date()) if hasattr(email_date, 'date') else str(email_date)
    tracker_sheet.append_row([email_id, email_subject[:80], date_str, timestamp])


# ──────────────────────────────────────────────
# Process Attachments
# ──────────────────────────────────────────────
def extract_text_from_attachments(attachments) -> str:
    """OCR any image/PDF attachments and return combined text."""
    combined_text = ""
    for att in attachments:
        ext = att.filename.lower()
        if not ext.endswith(('.png', '.jpg', '.jpeg', '.pdf', '.webp')):
            continue

        print(f"      📎 OCR'ing attachment: {att.filename}")
        file_bytes = att.payload

        images_to_process = []
        if ext.endswith('.pdf'):
            pdf_doc = fitz.open(stream=file_bytes, filetype="pdf")
            for page_num in range(len(pdf_doc)):
                page = pdf_doc.load_page(page_num)
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                images_to_process.append(Image.open(io.BytesIO(pix.tobytes("png"))))
            pdf_doc.close()
        else:
            images_to_process.append(Image.open(io.BytesIO(file_bytes)))

        for i, img in enumerate(images_to_process):
            combined_text += f"\n--- Attachment: {att.filename} (Page {i+1}) ---\n"
            combined_text += ocr_image(img, GEMINI_API_KEY)

    return combined_text


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    print("=" * 60)
    print("💧 Region of Peel Water Bill Agent")
    print("=" * 60)

    if not EMAIL_USER or not EMAIL_PASS:
        print("❌ Error: EMAIL_USER and EMAIL_PASS must be set in .env")
        return

    if not PEEL_SHEET_URL:
        print("❌ Error: PEEL_SHEET_URL (or SHEET_URL) must be set in .env")
        return

    # Parse senders (support comma-separated list)
    senders = [s.strip() for s in PEEL_SENDER.split(",") if s.strip()]

    # Parse dates
    try:
        start_date = datetime.strptime(PEEL_START_DATE, "%Y-%m-%d").date()
        end_date = datetime.strptime(PEEL_END_DATE, "%Y-%m-%d").date()
    except Exception as e:
        print(f"❌ Error parsing dates: {e}. Use YYYY-MM-DD format.")
        return

    print(f"📬 Email:    {EMAIL_USER}")
    if senders:
        print(f"🔍 Senders:  {', '.join(senders)}")
    else:
        print(f"🔍 Senders:  ALL (no filter)")
    print(f"📅 Range:    {start_date} → {end_date}")
    print(f"📊 Sheet:    {PEEL_SHEET_URL[:60]}...")
    print(f"📋 Tab:      {PEEL_TAB_SUMMARY} (summary only)")
    print(f"⚙️  Config:   {PEEL_CONFIG_FILE}")
    print("-" * 60)

    processed_count = 0
    skipped_count = 0
    error_count = 0

    try:
        with MailBox(EMAIL_HOST).login(EMAIL_USER, EMAIL_PASS) as mailbox:
            # Build search criteria
            if senders:
                all_messages = []
                for sender in senders:
                    criteria = AND(from_=sender, date_gte=start_date, date_lt=end_date)
                    msgs = list(mailbox.fetch(criteria))
                    all_messages.extend(msgs)
                    print(f"   📬 Found {len(msgs)} emails from {sender}")
            else:
                criteria = AND(date_gte=start_date, date_lt=end_date)
                all_messages = list(mailbox.fetch(criteria))
                print(f"   📬 Found {len(all_messages)} emails in date range")

            print(f"\n📊 Total emails to process: {len(all_messages)}")
            print("=" * 60)

            for idx, msg in enumerate(all_messages, 1):
                subject = msg.subject or "(no subject)"
                print(f"\n[{idx}/{len(all_messages)}] 📧 '{subject}'")
                print(f"   From: {msg.from_}  |  Date: {msg.date}")

                # ── Get email body text ──
                email_text = ""

                if msg.text:
                    email_text = msg.text
                elif msg.html:
                    email_text = html_to_text(msg.html)

                # Also check attachments for additional data (PDF bills)
                attachment_text = ""
                if msg.attachments:
                    try:
                        attachment_text = extract_text_from_attachments(msg.attachments)
                    except Exception as e:
                        print(f"      ⚠️ Attachment OCR failed: {e}")

                # Combine body + attachment text
                full_text = email_text
                if attachment_text:
                    full_text += "\n\n--- FROM ATTACHMENTS ---\n" + attachment_text

                if not full_text.strip():
                    print("   ⏭️  Empty email body, skipping.")
                    skipped_count += 1
                    continue

                # ── Check for duplicate ──
                body_hash = hashlib.md5(full_text.encode()).hexdigest()[:10]
                email_id = generate_email_id(subject, msg.date, body_hash)

                if check_if_processed(email_id, CREDS_FILE, PEEL_SHEET_URL, PEEL_TAB_TRACKER):
                    print("   ⏭️  Already processed, skipping.")
                    skipped_count += 1
                    continue

                # ── Extract bill data ──
                try:
                    print("   🧠 Extracting bill data with Gemini...")
                    processed_data = extract_with_gemini(full_text, GEMINI_API_KEY, CONFIG)

                    # Validate: check if we got any meaningful data
                    fields_cfg = CONFIG.get("fields", {})
                    has_data = any(processed_data.get(f) for f in fields_cfg)

                    if not has_data:
                        print("   ⏭️  No bill data found in this email.")
                        skipped_count += 1
                        continue

                    # ── Write to Google Sheets (SUMMARY ONLY) ──
                    print("   💾 Writing summary to Google Sheets...")
                    append_bill_to_sheet(
                        processed_data, subject, msg.date, email_id,
                        CREDS_FILE, PEEL_SHEET_URL,
                        PEEL_TAB_SUMMARY, PEEL_TAB_TRACKER,
                        CONFIG
                    )

                    # Print summary
                    bill_num = processed_data.get("bill_number", "N/A")
                    amount = processed_data.get("amount_due", "?")
                    due = processed_data.get("due_date", "?")
                    consumption = processed_data.get("total_consumption", "?")
                    print(f"   ✅ Bill #{bill_num} | ${amount} due {due} | {consumption} m³")
                    processed_count += 1

                    # Rate limiting to avoid API throttling
                    time.sleep(2)

                except Exception as e:
                    print(f"   ❌ Error: {e}")
                    traceback.print_exc()
                    error_count += 1
                    continue

        # ── Final Summary ──
        print("\n" + "=" * 60)
        print("📊 FINAL SUMMARY")
        print("=" * 60)
        print(f"   ✅ Processed: {processed_count}")
        print(f"   ⏭️  Skipped:   {skipped_count}")
        print(f"   ❌ Errors:    {error_count}")
        print(f"   📊 Total:     {len(all_messages)}")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
