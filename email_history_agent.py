import os
import io
import json
import fitz  # PyMuPDF
import hashlib
import gspread
import traceback
from datetime import datetime, date
import time
from PIL import Image
from imap_tools import MailBox, AND
from dotenv import load_dotenv

import langextract as lx
from google import genai
from google.genai import types

# Load variables from .env file
load_dotenv()

# --- Configuration & Secrets ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
CREDS_FILE = "service_account.json"

EMAIL_HOST = os.environ.get("EMAIL_HOST", "imap.gmail.com")
EMAIL_USER = os.environ.get("EMAIL_USER", "")
EMAIL_PASS = os.environ.get("EMAIL_PASS", "")

# --- Configurable Sheet & Tab names ---
HISTORY_SHEET_URL = os.environ.get("HISTORY_SHEET_URL", os.environ.get("SHEET_URL", ""))
HISTORY_TAB_DETAILS = os.environ.get("HISTORY_TAB_DETAILS", "Sheet1")       # Main tab for extracted details
HISTORY_TAB_TRACKER = os.environ.get("HISTORY_TAB_TRACKER", "receipt_tracker") # Duplicate tracking tab
HISTORY_TAB_SUMMARY = os.environ.get("HISTORY_TAB_SUMMARY", "Summary")       # Summary tab

# --- Historical email settings ---
HISTORY_SENDER = os.environ.get("HISTORY_SENDER", "ysrphy@gmail.com")
HISTORY_START_DATE = os.environ.get("HISTORY_START_DATE", "2026-01-01")
HISTORY_END_DATE = os.environ.get("HISTORY_END_DATE", "2026-01-31")

os.environ["GEMINI_API_KEY"] = GEMINI_API_KEY

# --- Load Extraction Config ---
EXTRACTION_CONFIG_FILE = os.environ.get("EXTRACTION_CONFIG", "extraction_config_Tennis.json")

def load_extraction_config(config_path: str) -> dict:
    """Loads the extraction field config from a JSON file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_extraction_config(EXTRACTION_CONFIG_FILE)

# --- Helper Functions ---

def ocr_receipt(image: Image.Image, api_key: str) -> str:
    """Uses Google GenAI SDK to perform OCR on the receipt image with simple retry."""
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
                    "Transcribe exactly all the text from this receipt. Maintain lines and layout as best as possible. Do not interpret data, just OCR.",
                    types.Part.from_bytes(data=buffered.getvalue(), mime_type="image/jpeg"),
                ],
            )
            return response.text
        except Exception as e:
            if "503" in str(e) and attempt < max_retries - 1:
                print(f"      ⏳ Gemini busy (503), retrying in {2**(attempt+1)}s...")
                time.sleep(2**(attempt+1))
                continue
            raise e

def extract_with_langextract(ocr_text: str, api_key: str, config: dict):
    """Uses LangExtract to extract structured details based on config."""
    example_cfg = config["example"]
    
    # Build LangExtract example extractions from config
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
        text_or_documents=ocr_text,
        prompt_description=config["prompt"],
        examples=examples,
        api_key=api_key,
        model_id="gemini-2.5-flash",
        temperature=0.0
    )
    return results

def get_or_create_tracker(gc, sheet, tab_name="receipt_tracker"):
    try:
        tracker_sheet = sheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        tracker_sheet = sheet.add_worksheet(title=tab_name, rows="1000", cols="2")
        tracker_sheet.append_row(["Upload_ID", "Timestamp"])
    return tracker_sheet

def get_or_create_summary(gc, sheet, tab_name="Summary", config=None):
    try:
        summary_sheet = sheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        cols = config.get("summary_columns", ["Date", "Time", "Shop", "Place (Address)", "Total Cost"]) if config else ["Date", "Time", "Shop", "Place (Address)", "Total Cost"]
        cols = cols + ["Processed At"]  # System timestamp column
        summary_sheet = sheet.add_worksheet(title=tab_name, rows="1000", cols=str(len(cols)))
        summary_sheet.append_row(cols)
    return summary_sheet

def generate_receipt_id(processed_data: dict, file_bytes: bytes) -> str:
    file_hash = hashlib.md5(file_bytes).hexdigest()[:10]
    date_str = str(processed_data.get("date", "")).strip()
    time_str = str(processed_data.get("time", "")).strip()
    return f"{file_hash}_{date_str}_{time_str}"

def check_if_exists(file_bytes: bytes, creds_path: str, sheet_url: str, tracker_tab: str = "receipt_tracker") -> bool:
    try:
        gc = gspread.service_account(filename=creds_path)
        sheet = gc.open_by_url(sheet_url)
        tracker_sheet = get_or_create_tracker(gc, sheet, tracker_tab)
        
        file_hash = hashlib.md5(file_bytes).hexdigest()[:10]
        records = tracker_sheet.get_all_values()
        
        for row in records:
            if row and str(row[0]).strip().startswith(file_hash + "_"):
                return True
        return False
    except:
        return False

def append_to_sheet(processed_data: dict, creds_path: str, sheet_url: str, file_bytes: bytes,
                    details_tab: str = "Sheet1", tracker_tab: str = "receipt_tracker", summary_tab: str = "Summary",
                    config: dict = None):
    gc = gspread.service_account(filename=creds_path)
    sheet_doc = gc.open_by_url(sheet_url)
    # Use configurable tab name for main details (create if missing)
    try:
        main_sheet = sheet_doc.worksheet(details_tab)
    except gspread.exceptions.WorksheetNotFound:
        main_sheet = sheet_doc.add_worksheet(title=details_tab, rows="1000", cols="20")
    tracker_sheet = get_or_create_tracker(gc, sheet_doc, tracker_tab)
    summary_sheet = get_or_create_summary(gc, sheet_doc, summary_tab, config)
    
    rows_to_insert = []
    
    # Write header fields from config
    fields_cfg = config["fields"] if config else {}
    for field_name, field_info in fields_cfg.items():
        label = field_info.get("label", f"{field_name}:")
        rows_to_insert.append([label, processed_data.get(field_name, "")])
    rows_to_insert.append([])
    
    # Write item rows from config
    item_cfg = config.get("item_field", {}) if config else {}
    item_field_name = item_cfg.get("name", "item")
    item_attrs = item_cfg.get("attributes", [])
    item_columns = item_cfg.get("columns", [])
    
    if processed_data.get("items"):
        if item_columns:
            rows_to_insert.append(item_columns)
        for item in processed_data["items"]:
            row = [item.get("name", "")]
            for attr in item_attrs:
                row.append(item.get(attr, ""))
            rows_to_insert.append(row)
            
    rows_to_insert.append(["---"] * max(len(item_columns), 4))
    rows_to_insert.append([])
            
    if rows_to_insert:
        main_sheet.append_rows(rows_to_insert)
        
        receipt_id = generate_receipt_id(processed_data, file_bytes)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tracker_sheet.append_row([receipt_id, timestamp])
        
        # Write summary row from config + system timestamp
        summary_field_order = config.get("summary_field_order", []) if config else []
        summary_row = [processed_data.get(f, "") for f in summary_field_order]
        summary_row.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        summary_sheet.append_row(summary_row)

def main():
    print("📜 Email History Agent starting...")
    
    if not EMAIL_USER or not EMAIL_PASS:
        print("❌ Error: EMAIL_USER and EMAIL_PASS must be set in .env")
        return

    # Parse dates
    try:
        start_date = datetime.strptime(HISTORY_START_DATE, "%Y-%m-%d").date()
        end_date = datetime.strptime(HISTORY_END_DATE, "%Y-%m-%d").date()
    except Exception as e:
        print(f"❌ Error parsing dates: {e}. Use YYYY-MM-DD format.")
        return

    print(f"🔍 Searching for emails from: {HISTORY_SENDER}")
    print(f"📅 Timeline: {start_date} to {end_date}")
    
    try:
        with MailBox(EMAIL_HOST).login(EMAIL_USER, EMAIL_PASS) as mailbox:
            # imap_tools date_gte is inclusive, date_lt is exclusive.
            # We fetch regardless of seen status for history search.
            criteria = AND(from_=HISTORY_SENDER, date_gte=start_date, date_lt=end_date)
            
            messages = list(mailbox.fetch(criteria))
            print(f"📬 Found {len(messages)} emails matching criteria.")
            
            for msg in messages:
                print(f"\n📧 Processing: '{msg.subject}' (Date: {msg.date})")
                
                for att in msg.attachments:
                    ext = att.filename.lower()
                    if ext.endswith(('.png', '.jpg', '.jpeg', '.pdf', '.webp')):
                        print(f"   📎 Found attachment: {att.filename}")
                        file_bytes = att.payload
                        
                        # 1. Duplicate Check
                        if check_if_exists(file_bytes, CREDS_FILE, HISTORY_SHEET_URL, HISTORY_TAB_TRACKER):
                            print("   ⚠️ Duplicate found in sheet. Skipping.")
                            continue
                            
                        # 2. PDF / Image Processing
                        images_to_process = []
                        if ext.endswith('.pdf'):
                            pdf_document = fitz.open(stream=file_bytes, filetype="pdf")
                            for page_num in range(len(pdf_document)):
                                page = pdf_document.load_page(page_num)
                                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                                images_to_process.append(Image.open(io.BytesIO(pix.tobytes("png"))))
                            pdf_document.close()
                        else:
                            images_to_process.append(Image.open(io.BytesIO(file_bytes)))

                        # 3. OCR & 4. Extraction
                        try:
                            raw_ocr_text = ""
                            for i, img in enumerate(images_to_process):
                                raw_ocr_text += f"\n--- Page {i + 1} ---\n{ocr_receipt(img, GEMINI_API_KEY)}"

                            lx_results = extract_with_langextract(raw_ocr_text, GEMINI_API_KEY, CONFIG)
                            
                            # Build processed_data dynamically from config fields
                            fields_cfg = CONFIG.get("fields", {})
                            item_cfg = CONFIG.get("item_field", {})
                            item_field_name = item_cfg.get("name", "item")
                            item_attrs = item_cfg.get("attributes", [])
                            
                            processed_data = {field: "" for field in fields_cfg}
                            processed_data["items"] = []
                            
                            if hasattr(lx_results, "extractions") and lx_results.extractions:
                                for ext_obj in lx_results.extractions:
                                    cls = ext_obj.extraction_class
                                    text = ext_obj.extraction_text
                                    attrs = ext_obj.attributes or {}
                                    
                                    if cls == item_field_name:
                                        item_data = {"name": text}
                                        for attr in item_attrs:
                                            item_data[attr] = attrs.get(attr, "")
                                        processed_data["items"].append(item_data)
                                    elif cls in fields_cfg:
                                        processed_data[cls] = text

                            # 5. Save to Sheets
                            has_data = any(processed_data.get(f) for f in fields_cfg)
                            if has_data:
                                append_to_sheet(processed_data, CREDS_FILE, HISTORY_SHEET_URL, file_bytes,
                                                HISTORY_TAB_DETAILS, HISTORY_TAB_TRACKER, HISTORY_TAB_SUMMARY, CONFIG)
                                # Print first two non-empty fields as confirmation
                                preview = [f"{k}={processed_data[k]}" for k in list(fields_cfg)[:2] if processed_data.get(k)]
                                print(f"   ✅ Logged: {', '.join(preview)}")
                            else:
                                print("   ⚠️ Could not extract data from attachment.")
                        except Exception as e:
                            print(f"   ❌ Error processing attachment: {e}")
                            continue # Move to next attachment
                            
        print("\n✅ History processing complete.")

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
