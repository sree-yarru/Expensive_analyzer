"""
Email Order Agent
=================
Scans your email inbox for order confirmations / purchase receipts,
extracts order numbers, items with prices & descriptions, and exports
everything to a Google Sheet.

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

# Who to scan emails from (comma-separated for multiple senders)
ORDER_SENDER = os.environ.get("ORDER_SENDER", "")
ORDER_START_DATE = os.environ.get("ORDER_START_DATE", "2026-01-01")
ORDER_END_DATE = os.environ.get("ORDER_END_DATE", "2026-03-18")

# Google Sheet settings
ORDER_SHEET_URL = os.environ.get("ORDER_SHEET_URL", os.environ.get("SHEET_URL", ""))
ORDER_TAB_DETAILS = os.environ.get("ORDER_TAB_DETAILS", "Orders")
ORDER_TAB_TRACKER = os.environ.get("ORDER_TAB_TRACKER", "order_tracker")
ORDER_TAB_SUMMARY = os.environ.get("ORDER_TAB_SUMMARY", "Order_Summary")

# Extraction config file
ORDER_CONFIG_FILE = os.environ.get("ORDER_EXTRACTION_CONFIG", "extraction_config_orders.json")

os.environ["GEMINI_API_KEY"] = GEMINI_API_KEY


# ──────────────────────────────────────────────
# Load Extraction Config
# ──────────────────────────────────────────────
def load_extraction_config(config_path: str) -> dict:
    """Loads the extraction field config from a JSON file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_extraction_config(ORDER_CONFIG_FILE)


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
        # Collapse excessive whitespace but keep newlines
        lines = [re.sub(r'[ \t]+', ' ', line).strip() for line in raw.splitlines()]
        # Remove excessive blank lines (max 2 consecutive)
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
    Uses Gemini directly to extract structured order data from email text.
    Falls back to LangExtract if Gemini structured extraction fails.
    """
    fields_cfg = config.get("fields", {})
    item_cfg = config.get("item_field", {})
    item_attrs = item_cfg.get("attributes", [])

    # Build a JSON schema description for Gemini
    field_descriptions = []
    for name, info in fields_cfg.items():
        field_descriptions.append(f'  "{name}": "<{info.get("label", name)}>"')
    
    item_attr_desc = ", ".join([f'"{a}": "<value>"' for a in item_attrs])
    
    schema = "{\n" + ",\n".join(field_descriptions) + ',\n  "items": [\n    {\n      "name": "<item name>",\n      ' + item_attr_desc + '\n    }\n  ]\n}'

    prompt = f"""You are an expert data extraction assistant. Extract all order/purchase information from the following email text.

INSTRUCTIONS:
- {config.get("prompt", "Extract all order details.")}
- Return ONLY valid JSON, no markdown, no explanation.
- If a field is not found, use an empty string "".
- For dates, always use YYYY-MM-DD format.
- Extract ALL items, not just the first one.

EXPECTED JSON FORMAT:
{schema}

EMAIL TEXT:
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
                # Remove markdown code fences
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
    item_cfg = config.get("item_field", {})
    item_field_name = item_cfg.get("name", "item")
    item_attrs = item_cfg.get("attributes", [])

    processed = {field: "" for field in fields_cfg}
    processed["items"] = []
    
    if hasattr(results, "extractions") and results.extractions:
        for ext_obj in results.extractions:
            cls = ext_obj.extraction_class
            text_val = ext_obj.extraction_text
            attrs = ext_obj.attributes or {}
            
            if cls == item_field_name:
                item_data = {"name": text_val}
                for attr in item_attrs:
                    item_data[attr] = attrs.get(attr, "")
                processed["items"].append(item_data)
            elif cls in fields_cfg:
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


def append_order_to_sheet(processed_data: dict, email_subject: str, email_date,
                          email_id: str, creds_path: str, sheet_url: str,
                          details_tab: str, tracker_tab: str, summary_tab: str,
                          config: dict):
    """Write extracted order data to Google Sheets."""
    gc = gspread.service_account(filename=creds_path)
    sheet_doc = gc.open_by_url(sheet_url)
    
    # Get/create tabs
    main_sheet = get_or_create_tab(sheet_doc, details_tab)
    tracker_sheet = get_or_create_tab(sheet_doc, tracker_tab, ["Email_ID", "Subject", "Date", "Processed_At"])
    
    summary_cols = config.get("summary_columns", ["Order #", "Vendor", "Date", "Total"])
    summary_cols_with_ts = summary_cols + ["# Items", "Processed At"]
    summary_sheet = get_or_create_tab(sheet_doc, summary_tab, summary_cols_with_ts, cols=len(summary_cols_with_ts))

    fields_cfg = config.get("fields", {})
    item_cfg = config.get("item_field", {})
    item_attrs = item_cfg.get("attributes", [])
    item_columns = item_cfg.get("columns", [])
    
    # ── Build rows for the Details tab ──
    rows = []
    rows.append(["📧 Email Subject:", email_subject])
    rows.append(["📅 Email Date:", str(email_date)])
    rows.append([])
    
    # Header fields
    for field_name, field_info in fields_cfg.items():
        label = field_info.get("label", f"{field_name}:")
        value = processed_data.get(field_name, "")
        rows.append([label, value])
    rows.append([])
    
    # Item rows
    items = processed_data.get("items", [])
    if items:
        if item_columns:
            rows.append(item_columns)
        for item in items:
            row = [item.get("name", "")]
            for attr in item_attrs:
                row.append(item.get(attr, ""))
            rows.append(row)
    
    rows.append(["───"] * max(len(item_columns), 5))
    rows.append([])
    
    # Write to details sheet
    if rows:
        main_sheet.append_rows(rows)
    
    # ── Track this email ──
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_str = str(email_date.date()) if hasattr(email_date, 'date') else str(email_date)
    tracker_sheet.append_row([email_id, email_subject[:80], date_str, timestamp])
    
    # ── Summary row ──
    summary_field_order = config.get("summary_field_order", [])
    summary_row = [processed_data.get(f, "") for f in summary_field_order]
    summary_row.append(str(len(items)))   # item count
    summary_row.append(timestamp)
    summary_sheet.append_row(summary_row)


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
    print("📦 Email Order Agent")
    print("=" * 60)
    
    if not EMAIL_USER or not EMAIL_PASS:
        print("❌ Error: EMAIL_USER and EMAIL_PASS must be set in .env")
        return
    
    if not ORDER_SHEET_URL:
        print("❌ Error: ORDER_SHEET_URL (or SHEET_URL) must be set in .env")
        return

    # Parse senders (support comma-separated list)
    senders = [s.strip() for s in ORDER_SENDER.split(",") if s.strip()]
    
    # Parse dates
    try:
        start_date = datetime.strptime(ORDER_START_DATE, "%Y-%m-%d").date()
        end_date = datetime.strptime(ORDER_END_DATE, "%Y-%m-%d").date()
    except Exception as e:
        print(f"❌ Error parsing dates: {e}. Use YYYY-MM-DD format.")
        return
    
    print(f"📬 Email:    {EMAIL_USER}")
    if senders:
        print(f"🔍 Senders:  {', '.join(senders)}")
    else:
        print(f"🔍 Senders:  ALL (no filter)")
    print(f"📅 Range:    {start_date} → {end_date}")
    print(f"📊 Sheet:    {ORDER_SHEET_URL[:60]}...")
    print(f"📋 Tabs:     {ORDER_TAB_DETAILS} | {ORDER_TAB_TRACKER} | {ORDER_TAB_SUMMARY}")
    print(f"⚙️  Config:   {ORDER_CONFIG_FILE}")
    print("-" * 60)
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    
    try:
        with MailBox(EMAIL_HOST).login(EMAIL_USER, EMAIL_PASS) as mailbox:
            # Build search criteria
            if senders:
                # Process each sender separately (IMAP doesn't support OR on FROM easily)
                all_messages = []
                for sender in senders:
                    criteria = AND(from_=sender, date_gte=start_date, date_lt=end_date)
                    msgs = list(mailbox.fetch(criteria))
                    all_messages.extend(msgs)
                    print(f"   📬 Found {len(msgs)} emails from {sender}")
            else:
                # No sender filter — get all emails in date range
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
                
                # Prefer plain text body
                if msg.text:
                    email_text = msg.text
                elif msg.html:
                    email_text = html_to_text(msg.html)
                
                # Also check attachments for additional data
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
                
                if check_if_processed(email_id, CREDS_FILE, ORDER_SHEET_URL, ORDER_TAB_TRACKER):
                    print("   ⏭️  Already processed, skipping.")
                    skipped_count += 1
                    continue
                
                # ── Extract order data ──
                try:
                    print("   🧠 Extracting order data with Gemini...")
                    processed_data = extract_with_gemini(full_text, GEMINI_API_KEY, CONFIG)
                    
                    # Validate: check if we got any meaningful data
                    fields_cfg = CONFIG.get("fields", {})
                    has_data = any(processed_data.get(f) for f in fields_cfg)
                    has_items = bool(processed_data.get("items"))
                    
                    if not has_data and not has_items:
                        print("   ⏭️  No order data found in this email.")
                        skipped_count += 1
                        continue
                    
                    # ── Write to Google Sheets ──
                    print("   💾 Writing to Google Sheets...")
                    append_order_to_sheet(
                        processed_data, subject, msg.date, email_id,
                        CREDS_FILE, ORDER_SHEET_URL,
                        ORDER_TAB_DETAILS, ORDER_TAB_TRACKER, ORDER_TAB_SUMMARY,
                        CONFIG
                    )
                    
                    # Print summary
                    order_num = processed_data.get("order_number", "N/A")
                    vendor = processed_data.get("vendor", "Unknown")
                    total = processed_data.get("total_amount", "?")
                    item_count = len(processed_data.get("items", []))
                    print(f"   ✅ Order #{order_num} | {vendor} | ${total} | {item_count} items")
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
