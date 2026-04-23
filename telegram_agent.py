import os
import io
import fitz  # PyMuPDF
import hashlib
from PIL import Image
import gspread
import traceback
from datetime import datetime
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

import langextract as lx
from google import genai
from google.genai import types

# Load variables from .env file
load_dotenv()

# --- Configuration & Secrets ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_TOKEN_HERE")
ALLOWED_USER_ID = os.environ.get("ALLOWED_USER_ID", "YOUR_TELEGRAM_USER_ID_HERE")  # VERY IMPORTANT: Set this to your Telegram User ID!
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "YOUR_GEMINI_KEY_HERE")
SHEET_URL = os.environ.get("SHEET_URL", "YOUR_GOOGLE_SHEET_URL_HERE")
CREDS_FILE = "service_account.json" 

# Set Gemini API Key for LangExtract
os.environ["GEMINI_API_KEY"] = GEMINI_API_KEY

# --- Helper Functions (From your original app!) ---
def ocr_receipt(image: Image.Image, api_key: str) -> str:
    """Uses Google GenAI SDK to perform OCR on the receipt image."""
    client = genai.Client(api_key=api_key)
    buffered = io.BytesIO()
    if image.mode in ("RGBA", "P"):
        image = image.convert("RGB")
    image.save(buffered, format="JPEG")
    
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
            "Transcribe exactly all the text from this receipt. Maintain lines and layout as best as possible. Do not interpret data, just OCR.",
            types.Part.from_bytes(data=buffered.getvalue(), mime_type="image/jpeg"),
        ],
    )
    return response.text

def extract_with_langextract(ocr_text: str, api_key: str):
    """Uses LangExtract to extract structured details."""
    example_text = """
SuperMart Store #12
123 Main St, Springfield
Tel: 555-0192
Date: 2023-11-04

1x Apple            $1.50
2 Bananas          $2.00
3  Milk 1gal        $4.50

Tax: $0.80
Total: $8.80
"""
    examples = [
        lx.data.ExampleData(
            text=example_text,
            extractions=[
                lx.data.Extraction("shop", "SuperMart Store #12"),
                lx.data.Extraction("address", "123 Main St, Springfield"),
                lx.data.Extraction("date", "2023-11-04"),
                lx.data.Extraction("time", "14:32"),
                lx.data.Extraction("item", "Apple", attributes={"quantity": "1", "unit_price": "1.50", "total_price": "1.50"}),
                lx.data.Extraction("item", "Bananas", attributes={"quantity": "2", "unit_price": "1.00", "total_price": "2.00"}),
                lx.data.Extraction("item", "Milk 1gal", attributes={"quantity": "3", "unit_price": "1.50", "total_price": "4.50"}),
                lx.data.Extraction("discount", "0.50"),
                lx.data.Extraction("total_amount", "8.80"),
            ]
        )
    ]
    prompt = "Extract the receipt details. Include the shop name, shop address, date (FORMAT STRICTLY AS YYYY-MM-DD), time, any discounts applied, all the items purchased (with their individual quantity, unit_price, and total_price as attributes), and the grand total_amount."

    results = lx.extract(
        text_or_documents=ocr_text,
        prompt_description=prompt,
        examples=examples,
        api_key=api_key,
        model_id="gemini-2.5-flash",
        temperature=0.0
    )
    return results

def get_or_create_tracker(gc, sheet):
    try:
        tracker_sheet = sheet.worksheet("receipt_tracker")
    except gspread.exceptions.WorksheetNotFound:
        tracker_sheet = sheet.add_worksheet(title="receipt_tracker", rows="1000", cols="2")
        tracker_sheet.append_row(["Upload_ID", "Timestamp"])
    return tracker_sheet

def get_or_create_summary(gc, sheet):
    try:
        summary_sheet = sheet.worksheet("Summary")
    except gspread.exceptions.WorksheetNotFound:
        summary_sheet = sheet.add_worksheet(title="Summary", rows="1000", cols="5")
        summary_sheet.append_row(["Date", "Time", "Shop", "Place (Address)", "Total Cost"])
    return summary_sheet

def generate_receipt_id(processed_data: dict, file_bytes: bytes) -> str:
    file_hash = hashlib.md5(file_bytes).hexdigest()[:10]
    date = str(processed_data.get("date", "")).strip()
    time = str(processed_data.get("time", "")).strip()
    return f"{file_hash}_{date}_{time}"

def check_if_exists(file_bytes: bytes, creds_path: str, sheet_url: str) -> bool:
    gc = gspread.service_account(filename=creds_path)
    sheet = gc.open_by_url(sheet_url)
    tracker_sheet = get_or_create_tracker(gc, sheet)
    
    file_hash = hashlib.md5(file_bytes).hexdigest()[:10]
    records = tracker_sheet.get_all_values()
    
    for row in records:
        if row and str(row[0]).strip().startswith(file_hash + "_"):
            return True
    return False

def append_to_sheet(processed_data: dict, creds_path: str, sheet_url: str, file_bytes: bytes):
    gc = gspread.service_account(filename=creds_path)
    sheet_doc = gc.open_by_url(sheet_url)
    main_sheet = sheet_doc.sheet1
    tracker_sheet = get_or_create_tracker(gc, sheet_doc)
    summary_sheet = get_or_create_summary(gc, sheet_doc)
    
    rows_to_insert = []
    rows_to_insert.append(["Shop:", processed_data.get("shop", "")])
    rows_to_insert.append(["Address:", processed_data.get("address", "")])
    rows_to_insert.append(["Date:", processed_data.get("date", "")])
    rows_to_insert.append(["Time:", processed_data.get("time", "")])
    rows_to_insert.append(["Discount:", processed_data.get("discount", "")])
    rows_to_insert.append(["Grand Total:", processed_data.get("total_amount", "")])
    rows_to_insert.append([])
    
    if processed_data["items"]:
        rows_to_insert.append(["Item Name", "Quantity", "Unit Price", "Total Price"])
        for item in processed_data["items"]:
            rows_to_insert.append([
                item.get("name", ""),
                item.get("quantity", ""),
                item.get("unit_price", ""),
                item.get("price", "")
            ])
            
    rows_to_insert.append(["---", "---", "---", "---"])
    rows_to_insert.append([])
            
    if rows_to_insert:
        main_sheet.append_rows(rows_to_insert)
        
        receipt_id = generate_receipt_id(processed_data, file_bytes)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tracker_sheet.append_row([receipt_id, timestamp])
        
        summary_sheet.append_row([
            processed_data.get("date", ""),
            processed_data.get("time", ""),
            processed_data.get("shop", ""),
            processed_data.get("address", ""),
            processed_data.get("total_amount", "")
        ])

# --- Telegram Bot Agent Logic ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Answers the /start command."""
    user_id = str(update.effective_user.id)
    if user_id != ALLOWED_USER_ID and ALLOWED_USER_ID != "YOUR_TELEGRAM_USER_ID_HERE":
        await update.message.reply_text("🚨 Security Alert: You are not authorized to use this personal bot. (Your ID: {})".format(user_id))
        return
        
    await update.message.reply_text("👋 Hello Creator! I am your personal Receipt Tracking Agent.\nJust snap a photo of a receipt and send it to me right here. I'll read it, check for duplicates, and push it straight to your Google Sheet!")


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Extracts data when a user sends a photo or document."""
    user_id = str(update.effective_user.id)
    if user_id != ALLOWED_USER_ID and ALLOWED_USER_ID != "YOUR_TELEGRAM_USER_ID_HERE":
        await update.message.reply_text(f"🚨 Security Alert: You are not authorized to use this personal bot. (Your ID: {user_id})")
        return

    # Acknowledge receipt
    status_msg = await update.message.reply_text("📸 Got it! Analyzing receipt...")

    try:
        # Check if they sent a Photo or a Document (PDF/uncompressed image)
        file = None
        is_pdf = False
        if update.message.photo:
            # Grab the highest resolution photo
            file = await update.message.photo[-1].get_file()
        elif update.message.document:
            file = await update.message.document.get_file()
            if update.message.document.mime_type == 'application/pdf':
                is_pdf = True
        
        if not file:
            await status_msg.edit_text("❌ I couldn't read that file. Please send a clear Photo or PDF.")
            return

        # Download file to memory
        file_bytes_array = bytearray(await file.download_as_bytearray())
        file_bytes = bytes(file_bytes_array)

        # 1. Immediate Duplicate Check
        await status_msg.edit_text("🔍 Checking duplicate database...")
        is_duplicate = check_if_exists(file_bytes, CREDS_FILE, SHEET_URL)
        
        if is_duplicate:
            await status_msg.edit_text("⚠️ Wait! I've already logged this exact receipt to your Google Sheet previously. I'm skipping it to prevent a duplicate entry!")
            return

        # 2. PDF / Image Processing
        images_to_process = []
        if is_pdf:
            await status_msg.edit_text("📄 Converting PDF into images...")
            pdf_document = fitz.open(stream=file_bytes, filetype="pdf")
            for page_num in range(len(pdf_document)):
                page = pdf_document.load_page(page_num)
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                img_bytes = pix.tobytes("png")
                images_to_process.append(Image.open(io.BytesIO(img_bytes)))
            pdf_document.close()
        else:
            images_to_process.append(Image.open(io.BytesIO(file_bytes)))

        # 3. Running OCR & LangExtract
        await status_msg.edit_text("🧠 Reading the text with Gemini...")
        raw_ocr_text = ""
        for i, img in enumerate(images_to_process):
            raw_ocr_text += f"\n--- Page {i + 1} ---\n{ocr_receipt(img, GEMINI_API_KEY)}"

        await status_msg.edit_text("🧾 Extracting math and items...")
        lx_results = extract_with_langextract(raw_ocr_text, GEMINI_API_KEY)
        
        # Parse data
        processed_data = {
            "shop": "", "address": "", "date": "", "time": "", 
            "discount": "", "total_amount": "", "items": []
        }
        
        if hasattr(lx_results, "extractions") and lx_results.extractions:
            for ext in lx_results.extractions:
                cls = ext.extraction_class
                text = ext.extraction_text
                attrs = ext.attributes or {}
                
                if cls == "shop": processed_data["shop"] = text
                elif cls == "address": processed_data["address"] = text
                elif cls == "date": processed_data["date"] = text
                elif cls == "time": processed_data["time"] = text
                elif cls == "discount": processed_data["discount"] = text
                elif cls == "total_amount": processed_data["total_amount"] = text
                elif cls == "item":
                    processed_data["items"].append({
                        "name": text,
                        "quantity": attrs.get("quantity", "1"),
                        "unit_price": attrs.get("unit_price", "0"),
                        "price": attrs.get("total_price", "0")
                    })

        # 4. Save to Sheets
        await status_msg.edit_text("💾 Logging everything to Google Sheets...")
        append_to_sheet(processed_data, CREDS_FILE, SHEET_URL, file_bytes)
        
        # 5. Final Report
        report = f"✅ **Logged Successfully!**\n\n"
        report += f"🏙️ **Shop:** {processed_data['shop']}\n"
        report += f"📅 **Date:** {processed_data['date']}\n"
        report += f"🛒 **Items:** {len(processed_data['items'])}\n"
        report += f"💰 **Total:** ${processed_data['total_amount']}\n\n"
        report += f"*(Check your Summary Tab!)*"
        
        await status_msg.edit_text(report, parse_mode='Markdown')

    except Exception as e:
        error_msg = f"❌ Encountered an error:\n{str(e)[:200]}"
        await status_msg.edit_text(error_msg)
        print(traceback.format_exc())

# --- Main App Execution ---
def main():
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_TOKEN_HERE" or TELEGRAM_BOT_TOKEN == "PASTE_YOUR_TELEGRAM_BOT_TOKEN_HERE":
        print("❌ Error: Please open your '.env' file and paste your TELEGRAM_BOT_TOKEN inside it!")
        return
        
    print("🤖 Agent is starting up... Waiting for Telegram Messages!")
    
    # Build the Telegram Bot App
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Add Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.PHOTO | filters.Document.PDF, handle_photo))

    # Run the bot infinitely until Ctrl+C
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
