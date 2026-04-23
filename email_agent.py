import os
import sys
import re
import io
import fitz  # PyMuPDF
from PIL import Image
import gspread
from datetime import datetime, timedelta
from imap_tools import MailBox, AND
from dotenv import load_dotenv

from google import genai
from google.genai import types
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pydantic
import json

# Load variables from .env file for local testing
load_dotenv()

# --- Configuration & Secrets ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
SHEET_URL = os.environ.get("SHEET_URL", "")
# In a true GCP environment, we use 'service_account.json' mounted or load via Default Credentials
CREDS_FILE = "service_account.json"

EMAIL_HOST = os.environ.get("EMAIL_HOST", "imap.gmail.com")
EMAIL_USER = os.environ.get("EMAIL_USER", "YOUR_EMAIL@gmail.com")
EMAIL_PASS = os.environ.get("EMAIL_PASS", "YOUR_APP_PASSWORD")
ALLOWED_SENDER = os.environ.get("ALLOWED_SENDER", "YOUR_PERSONAL_EMAIL@gmail.com")

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
SCAN_DAYS = int(os.environ.get("SCAN_DAYS", "1"))

os.environ["GEMINI_API_KEY"] = GEMINI_API_KEY


# --- Tools ---

def ocr_document(image_bytes: bytes, api_key: str, is_pdf: bool) -> str:
    """Uses Google GenAI SDK to perform OCR on a document."""
    client = genai.Client(api_key=api_key)
    
    images = []
    if is_pdf:
        pdf_document = fitz.open(stream=image_bytes, filetype="pdf")
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            img_bytes = pix.tobytes("png")
            images.append(Image.open(io.BytesIO(img_bytes)))
        pdf_document.close()
    else:
        images.append(Image.open(io.BytesIO(image_bytes)))
        
    ocr_text = ""
    for i, img in enumerate(images):
        buffered = io.BytesIO()
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        img.save(buffered, format="JPEG")
        
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[
                    "Transcribe exactly all the text from this document. Maintain lines and layout as best as possible.",
                    types.Part.from_bytes(data=buffered.getvalue(), mime_type="image/jpeg"),
                ],
            )
            ocr_text += f"\n--- Page {i + 1} ---\n{response.text}"
        except Exception as e:
            print(f"OCR Error: {e}")
            
    return ocr_text

class TriageResult(pydantic.BaseModel):
    is_relevant: bool = pydantic.Field(description="True if it might be a bill, receipt, purchase, payment, or is brief/ambiguous. False if clearly spam or conversational.")

def triage_email(text: str, api_key: str) -> bool:
    """Fast pass to determine if we should bother doing OCR and full extraction."""
    client = genai.Client(api_key=api_key)
    prompt = "Determine if this email content could possibly be a bill, receipt, statement, or payment confirmation. If the email is completely blank, very short (like 'attached'), or ambiguous, err on the side of caution and return True. If it's clearly a promotional newsletter, spam, login alert, or casual chat, return False."
    
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt + f"\n\nText:\n{text}",
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=TriageResult,
                temperature=0.0
            ),
        )
        return json.loads(response.text).get('is_relevant', True)
    except Exception as e:
        print(f"Triage error: {e}")
        return True  # Default to processing if triage fails

class ClassificationResult(pydantic.BaseModel):
    category: str = pydantic.Field(description="Must be BILL_DUE, RECEIPT_PURCHASE, or OTHER")
    
    # Bill fields
    merchant_name: str | None = pydantic.Field(default=None, description="The merchant or issuer of the bill")
    due_date: str | None = pydantic.Field(default=None, description="Due date in YYYY-MM-DD format if found")
    
    # Receipt / Generic fields
    date: str | None = pydantic.Field(default=None, description="Date of purchase in YYYY-MM-DD")
    total_amount: str | None = pydantic.Field(default=None, description="Total amount (including tax for receipts, or account balance/amount due for bills), e.g. 50.25")
    vendor: str | None = pydantic.Field(default=None, description="Vendor or store for the receipt")
    purchase_category: str | None = pydantic.Field(default=None, description="Category like Groceries, Electronics, Dining, etc.")


def classify_and_extract(text: str, api_key: str) -> dict:
    """Classifies the email/document and extracts fields directly using Gemini Structured Output."""
    client = genai.Client(api_key=api_key)
    
    prompt = """
    Analyze the following email body and/or attached document text.
    First, classify it as one of the following:
    - BILL_DUE (if it is a request for payment, a utility bill, a credit card statement with a balance, etc.)
    - RECEIPT_PURCHASE (if it is proof of a completed purchase or a receipt)
    - OTHER (if it is neither)
    
    Then extract the requested fields based on the category. Use YYYY-MM-DD for dates.
    Leave fields null if they are not present.
    """
    
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt + f"\n\nText to analyze:\n{text}",
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=ClassificationResult,
                temperature=0.1
            ),
        )
        return json.loads(response.text)
    except Exception as e:
        print(f"Classification error: {e}")
        return {"category": "OTHER"}

def execute_google_calendar_tool(merchant: str, amount: str, due_date: str, creds_path: str):
    """Creates a Calendar Event using the service account credentials."""
    SCOPES = ['https://www.googleapis.com/auth/calendar.events']
    
    # Calculate fallback date if due_date is missing or wonky
    event_date = None
    try:
        if due_date:
            parsed_date = datetime.strptime(due_date, "%Y-%m-%d").date()
            event_date = parsed_date.strftime("%Y-%m-%d")
            # For all day events, end date is exclusive (next day)
            end_date = (parsed_date + timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        pass
        
    if not event_date:
        print("   ⚠️ Due date missing or invalid, defaulting to 3 days from today.")
        fallback_date = datetime.now().date() + timedelta(days=3)
        event_date = fallback_date.strftime("%Y-%m-%d")
        end_date = (fallback_date + timedelta(days=1)).strftime("%Y-%m-%d")
    
    try:
        credentials = service_account.Credentials.from_service_account_file(
            creds_path, scopes=SCOPES)
        service = build('calendar', 'v3', credentials=credentials)
        
        event = {
            'summary': f'Pay: {merchant} - ${amount}',
            'start': {
                'date': event_date,
            },
            'end': {
                'date': end_date,
            },
        }
        
        # Service accounts on non-Workspace domains cannot send invites.
        # Instead, we insert it directly into the user's calendar.
        # (You MUST share your personal Google Calendar with the service account email first!)
        calendar_target = ALLOWED_SENDER if ALLOWED_SENDER and ALLOWED_SENDER != "YOUR_PERSONAL_EMAIL@gmail.com" else 'primary'
        
        event_result = service.events().insert(calendarId=calendar_target, body=event).execute()
        return True, event_result.get('htmlLink')
    except Exception as e:
        return False, str(e)

def execute_excel_integration_tool(date: str, vendor: str, amount: str, category: str, creds_path: str, sheet_url: str):
    """Appends data to Google Sheets (used in place of Excel for Cloud Functions)."""
    try:
        gc = gspread.service_account(filename=creds_path)
        sheet_doc = gc.open_by_url(sheet_url)
        
        try:
            expenses_sheet = sheet_doc.worksheet("My_Expenses_2026")
        except gspread.exceptions.WorksheetNotFound:
            expenses_sheet = sheet_doc.add_worksheet(title="My_Expenses_2026", rows="1000", cols="6")
            expenses_sheet.append_row(["Timestamp", "Date", "Vendor", "Category", "Amount", "Status"])
            
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Add the row at the bottom
        expenses_sheet.append_row([
            timestamp,
            date or "",
            vendor or "",
            category or "",
            amount or "",
            "Appended via Cloud Function"
        ])
        return True, "Appended to sheet"
    except Exception as e:
        return False, str(e)


# --- Standalone Entry Point for Cloud Run Jobs ---
def run_agent():
    """
    Triggered by running the script directly (e.g. as a Cloud Run Job).
    """
    print(f"📧 Financial Agent Job triggered at {datetime.now()}")
    
    if EMAIL_USER == "YOUR_EMAIL@gmail.com" or EMAIL_PASS == "YOUR_APP_PASSWORD":
        msg = "❌ Error: Please configure the EMAIL variables in your environment/Cloud Run environment!"
        print(msg)
        sys.exit(1)

    summary_logs = []
    
    try:
        with MailBox(EMAIL_HOST).login(EMAIL_USER, EMAIL_PASS) as mailbox:
            # We fetch ALL emails (read and unread) from the past 24 hours.
            # Using mark_seen=False ensures it never alters the Read/Unread state in your inbox.
            scan_start_date = (datetime.now() - timedelta(days=SCAN_DAYS)).date()
            messages = list(mailbox.fetch(AND(date_gte=scan_start_date), mark_seen=False))
            
            summary_logs.append(f"Found {len(messages)} unread messages.")
            
            for msg in messages:
                print(f"\n📬 Processing '{msg.subject}'")
                
                # Build context from body
                body_text = msg.text or ""
                if len(body_text.strip()) < 50 and getattr(msg, 'html', None):
                    # Fallback to HTML if plain text is missing or very short
                    clean_html = re.sub(r'<style.*?>.*?</style>', '', msg.html, flags=re.DOTALL|re.IGNORECASE)
                    clean_html = re.sub(r'<script.*?>.*?</script>', '', clean_html, flags=re.DOTALL|re.IGNORECASE)
                    body_text = re.sub(r'<[^>]+>', ' ', clean_html)
                    
                full_text = f"Subject: {msg.subject}\nBody: {body_text}\n"
                
                print("   ⚡ Triaging email body...")
                is_relevant = triage_email(full_text, GEMINI_API_KEY)
                
                if not is_relevant:
                    log_msg = f"⏭️ [OTHER] Skipped by Triage (not relevant): {msg.subject}"
                    print(log_msg)
                    summary_logs.append(log_msg)
                    continue
                
                # Check for attachments and perform OCR
                has_attachment = False
                for att in msg.attachments:
                    ext = att.filename.lower()
                    if ext.endswith(('.png', '.jpg', '.jpeg', '.pdf', '.webp')):
                        has_attachment = True
                        is_pdf = ext.endswith('.pdf')
                        print(f"   📎 OCR running on attachment: {att.filename}")
                        ocr_result = ocr_document(att.payload, GEMINI_API_KEY, is_pdf)
                        full_text += f"\nAttachment OCR:\n{ocr_result}"
                
                # Classify
                print("   🧠 Classifying email...")
                result = classify_and_extract(full_text, GEMINI_API_KEY)
                category = result.get('category', 'OTHER')
                print(f"   🏷️ Classified as: {category}")
                
                if category == "BILL_DUE":
                    merchant = result.get('merchant_name') or 'Unknown'
                    amount = result.get('total_amount') or '0'
                    due_date = result.get('due_date')
                    
                    success, detail = execute_google_calendar_tool(merchant, amount, due_date, CREDS_FILE)
                    if success:
                        log_msg = f"✅ [BILL_DUE] Created Calendar event for {merchant} (${amount})"
                    else:
                        log_msg = f"❌ [BILL_DUE] Calendar error for {merchant}: {detail}"
                    
                    print(log_msg)
                    summary_logs.append(log_msg)
                    
                elif category == "RECEIPT_PURCHASE":
                    vendor = result.get('vendor') or 'Unknown'
                    amount = result.get('total_amount') or '0'
                    dt = result.get('date') or ''
                    cat = result.get('purchase_category') or 'General'
                    
                    success, detail = execute_excel_integration_tool(dt, vendor, amount, cat, CREDS_FILE, SHEET_URL)
                    if success:
                        log_msg = f"✅ [RECEIPT_PURCHASE] Appended {vendor} (${amount}) to Expenses Sheet"
                    else:
                        log_msg = f"❌ [RECEIPT_PURCHASE] Sheets error for {vendor}: {detail}"
                        
                    print(log_msg)
                    summary_logs.append(log_msg)
                    
                else:
                    log_msg = f"⏭️ [OTHER] Ignored message: {msg.subject}"
                    print(log_msg)
                    summary_logs.append(log_msg)
                    
        # Print Summary Log to Cloud Logging
        print("\n\n=== DAILY SUMMARY REPORT ===")
        for log in summary_logs:
            print(log)
        print("============================\n")
        
        print("Agent executed successfully.")
        sys.exit(0)

    except Exception as e:
        error_msg = f"❌ Error checking email: {e}"
        print(error_msg)
        sys.exit(1)

if __name__ == "__main__":
    run_agent()
