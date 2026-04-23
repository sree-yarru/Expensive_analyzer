import streamlit as st
import pandas as pd
from PIL import Image
import os
import base64
from io import BytesIO
import gspread
import traceback
import hashlib
import fitz # PyMuPDF for PDF Support, doesn't require poppler
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
import datetime

# LangExtract imports
import langextract as lx
from google import genai
from google.genai import types

# --- Helper Functions ---
def ocr_receipt(image: Image.Image, api_key: str) -> str:
    """Uses Google GenAI SDK to perform OCR on the receipt image."""
    client = genai.Client(api_key=api_key)
    
    # Convert image to bytes
    buffered = BytesIO()
    if image.mode in ("RGBA", "P"):
        image = image.convert("RGB")
    image.save(buffered, format="JPEG")
    
    # Call Gemini to get raw text
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
            "Transcribe exactly all the text from this receipt. Maintain lines and layout as best as possible. Do not interpret data, just OCR.",
            types.Part.from_bytes(data=buffered.getvalue(), mime_type="image/jpeg"),
        ],
    )
    return response.text

def extract_with_langextract(ocr_text: str, api_key: str):
    """Uses LangExtract to extract structured details from the raw OCR text."""
    
    os.environ["GEMINI_API_KEY"] = api_key
    
    # Define few-shot examples for LangExtract
    # The example guides the LLM on what entities we want and how to extract attributes.
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
                # Items with attributes
                lx.data.Extraction("item", "Apple", attributes={"quantity": "1", "unit_price": "1.50", "total_price": "1.50"}),
                lx.data.Extraction("item", "Bananas", attributes={"quantity": "2", "unit_price": "1.00", "total_price": "2.00"}),
                lx.data.Extraction("item", "Milk 1gal", attributes={"quantity": "3", "unit_price": "1.50", "total_price": "4.50"}),
                lx.data.Extraction("discount", "0.50"),
                lx.data.Extraction("total_amount", "8.80"),
            ]
        )
    ]

    prompt = "Extract the receipt details. Include the shop name, shop address, date (FORMAT STRICTLY AS YYYY-MM-DD), time, any discounts applied, all the items purchased (with their individual quantity, unit_price, and total_price as attributes), and the grand total_amount."


    # Call LangExtract
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
    """Gets the 'receipt_tracker' worksheet, or creates it if it doesn't exist."""
    try:
        tracker_sheet = sheet.worksheet("receipt_tracker")
    except gspread.exceptions.WorksheetNotFound:
        # Create it and add a header
        tracker_sheet = sheet.add_worksheet(title="receipt_tracker", rows="1000", cols="2")
        tracker_sheet.append_row(["Upload_ID", "Timestamp"])
    return tracker_sheet

def get_or_create_summary(gc, sheet):
    """Gets the 'Summary' worksheet, or creates it if it doesn't exist."""
    try:
        summary_sheet = sheet.worksheet("Summary")
    except gspread.exceptions.WorksheetNotFound:
        summary_sheet = sheet.add_worksheet(title="Summary", rows="1000", cols="5")
        # Add headers for the summary sheet
        summary_sheet.append_row(["Date", "Time", "Shop", "Place (Address)", "Total Cost"])
    return summary_sheet

def generate_receipt_id(processed_data: dict, file_bytes: bytes) -> str:
    """Generates a unique deterministic ID utilizing a strict MD5 Image Hash plus OCR time/date."""
    # 1. Create an immutable hash of the physical image file itself
    file_hash = hashlib.md5(file_bytes).hexdigest()[:10] # Using first 10 chars is plenty for collision avoidance
    
    # 2. Append the OCR time/date for readability
    date = str(processed_data.get("date", "")).strip()
    time = str(processed_data.get("time", "")).strip()
    
    return f"{file_hash}_{date}_{time}"

def check_if_exists(file_bytes: bytes, creds_path: str, sheet_url: str) -> bool:
    """Checks if the exact image hash already exists in the tracker sheet before extraction."""
    try:
        gc = gspread.service_account(filename=creds_path)
        sheet = gc.open_by_url(sheet_url)
        tracker_sheet = get_or_create_tracker(gc, sheet)
        
        file_hash = hashlib.md5(file_bytes).hexdigest()[:10]
        st.info(f"🔍 Fast Dup Check - Image Hash: `{file_hash}`")
        
        records = tracker_sheet.get_all_values()
        
        for row in records:
            if row and str(row[0]).strip().startswith(file_hash + "_"):
                st.error(f"🚨 Match found in tracker database: {row}")
                return True # Exact duplicate image found!
            
    except gspread.exceptions.APIError as e:
        st.error(f"Google Sheets API Error (Permissions?): {e}")
    except Exception as e:
        st.error(f"Could not verify duplicates: {e}")
        
    return False

def append_to_sheet(processed_data: dict, creds_path: str, sheet_url: str, file_bytes: bytes):
    """Appends the formatted receipt data to Google Sheets AND logs the hash."""
    gc = gspread.service_account(filename=creds_path)
    sheet_doc = gc.open_by_url(sheet_url)
    main_sheet = sheet_doc.sheet1
    tracker_sheet = get_or_create_tracker(gc, sheet_doc)
    summary_sheet = get_or_create_summary(gc, sheet_doc)
    
    rows_to_insert = []
    
    # Adding Header details
    rows_to_insert.append(["Shop:", processed_data.get("shop", "")])
    rows_to_insert.append(["Address:", processed_data.get("address", "")])
    rows_to_insert.append(["Date:", processed_data.get("date", "")])
    rows_to_insert.append(["Time:", processed_data.get("time", "")])
    rows_to_insert.append(["Discount:", processed_data.get("discount", "")])
    rows_to_insert.append(["Grand Total:", processed_data.get("total_amount", "")])
    
    # Adding Empty Row
    rows_to_insert.append([])
    
    # Adding Items Table Header
    if processed_data["items"]:
        rows_to_insert.append(["Item Name", "Quantity", "Unit Price", "Total Price"])
        
        # Adding Item Rows
        for item in processed_data["items"]:
            rows_to_insert.append([
                item.get("name", ""),
                item.get("quantity", ""),
                item.get("unit_price", ""),
                item.get("price", "")
            ])
            
    # Add a semantic separator row between receipts
    rows_to_insert.append(["---", "---", "---", "---"])
    rows_to_insert.append([])
            
    if rows_to_insert:
        main_sheet.append_rows(rows_to_insert)
        
        # Log the unique ID in the tracking sheet
        receipt_id = generate_receipt_id(processed_data, file_bytes)
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tracker_sheet.append_row([receipt_id, timestamp])
        
        # Log the 1-line summary to the Summary Tab
        summary_sheet.append_row([
            processed_data.get("date", ""),
            processed_data.get("time", ""),
            processed_data.get("shop", ""),
            processed_data.get("address", ""),
            processed_data.get("total_amount", "")
        ])

def convert_to_pdf(file_bytes: bytes, file_ext: str) -> bytes:
    """Converts image or existing PDF bytes to standard PDF bytes using PyMuPDF."""
    if file_ext.lower() == "pdf":
        return file_bytes
    try:
        doc = fitz.open(stream=file_bytes, filetype=file_ext)
        pdf_bytes = doc.convert_to_pdf()
        return pdf_bytes
    except Exception as e:
        st.error(f"Failed to convert image to PDF: {e}")
        return None

def append_to_drive_pdf(new_pdf_bytes: bytes, folder_id: str, creds_path: str):
    """Appends new PDF bytes to a monthly PDF in Google Drive."""
    try:
        creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=["https://www.googleapis.com/auth/drive"]
        )
        drive_service = build('drive', 'v3', credentials=creds)
        
        current_month = datetime.datetime.now().strftime("%Y_%m")
        target_filename = f"Receipts_{current_month}.pdf"
        
        query = f"'{folder_id}' in parents and name='{target_filename}' and trashed=false"
        results = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        items = results.get('files', [])
        
        new_pdf_doc = fitz.open(stream=new_pdf_bytes, filetype="pdf")
        
        if items:
            file_id = items[0]['id']
            request = drive_service.files().get_media(fileId=file_id)
            existing_pdf_bytes = BytesIO()
            downloader = MediaIoBaseDownload(existing_pdf_bytes, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                
            existing_pdf_content = existing_pdf_bytes.getvalue()
            try:
                if len(existing_pdf_content) == 0:
                    raise Exception("Empty stream")
                existing_pdf_doc = fitz.open(stream=existing_pdf_content, filetype="pdf")
                existing_pdf_doc.insert_pdf(new_pdf_doc)
                updated_pdf_bytes = existing_pdf_doc.write()
            except Exception:
                # The existing file was empty or corrupted (e.g. 0-byte dummy file). 
                # So we just overwrite it with the new PDF safely without crashing!
                updated_pdf_bytes = new_pdf_doc.write()
            
            media = MediaIoBaseUpload(BytesIO(updated_pdf_bytes), mimetype='application/pdf', resumable=True)
            drive_service.files().update(fileId=file_id, media_body=media).execute()
        else:
            file_metadata = {
                'name': target_filename,
                'parents': [folder_id]
            }
            media = MediaIoBaseUpload(BytesIO(new_pdf_bytes), mimetype='application/pdf', resumable=True)
            try:
                drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            except Exception as create_exc:
                error_str = str(create_exc).lower()
                if "storage quota" in error_str or "storagequotaexceeded" in error_str or "403" in error_str:
                    raise Exception(f"Google Workspace blocks Service Accounts from creating new files under their own quota. Please manually upload an empty PDF named '{target_filename}' into your shared folder first! Once the file exists, the system can bypass the quota by simply appending to your file instead.")
                raise create_exc
            
    except Exception as e:
        raise Exception(f"Drive API Error: {e}")

# --- Streamlit UI ---
def main():
    st.set_page_config(page_title="Receipt to Sheets Extractor (LangExtract)", page_icon="🧾", layout="wide")
    st.title("🧾 Receipt Extractor using LangExtract + Gemini 2.5")
    st.markdown("Upload receipt images, convert to text, extract details using **LangExtract**, and push them to **Google Sheets**.")
    
    with st.sidebar:
        st.header("🔑 Configuration")
        st.info("Setup your APIs and Google Sheets connection")
        
        from dotenv import load_dotenv
        import os
        load_dotenv()
        
        default_api = os.environ.get("GEMINI_API_KEY", "")
        default_sheet = os.environ.get("SHEET_URL", "")
        default_drive = os.environ.get("DRIVE_FOLDER_ID", "")
        
        try:
            if not default_api: default_api = st.secrets.get("GEMINI_API_KEY", "")
            if not default_sheet: default_sheet = st.secrets.get("SHEET_URL", "")
            if not default_drive: default_drive = st.secrets.get("DRIVE_FOLDER_ID", "")
        except:
            pass

        if default_api:
            st.success("🔑 Gemini API Key loaded from environment.")
            api_key = default_api
        else:
            api_key = st.text_input("Google Gemini API Key", type="password")
            
        if default_sheet:
            st.success("📊 Google Sheet URL loaded from environment.")
            sheet_url = default_sheet
        else:
            sheet_url = st.text_input("Google Sheet URL", help="Make sure your Service Account has Edit access to this sheet.")
            
        if default_drive:
            st.success("📂 Google Drive Folder ID loaded from environment.")
            drive_folder_id = default_drive
        else:
            drive_folder_id = st.text_input("Google Drive Folder ID (Optional)", help="Enter Folder ID shared with Service Account to store Receipts PDF.")
        
        local_creds_exist = os.path.exists("service_account.json")
        if local_creds_exist:
            st.success("🔐 Local Service Account connected natively.")
            creds_file = "LOCAL"
        else:
            creds_file = st.file_uploader("Google Service Account JSON", type=["json"], help="Upload your Service Account JSON.")
            
        if api_key and sheet_url and creds_file and creds_file != "LOCAL":
            if st.button("💾 Save Settings for Mobile App"):
                os.makedirs(".streamlit", exist_ok=True)
                with open(".streamlit/secrets.toml", "w") as f:
                    f.write(f'GEMINI_API_KEY = "{api_key}"\n')
                    f.write(f'SHEET_URL = "{sheet_url}"\n')
                    f.write(f'DRIVE_FOLDER_ID = "{drive_folder_id}"\n')
                with open("service_account.json", "wb") as f:
                    f.write(creds_file.getvalue())
                st.success("Config saved! You can now use your phone without entering credentials.")
                st.rerun()

        if local_creds_exist:
            if st.button("🗑️ Clear saved settings"):
                if os.path.exists(".streamlit/secrets.toml"):
                    os.remove(".streamlit/secrets.toml")
                if os.path.exists("service_account.json"):
                    os.remove("service_account.json")
                st.rerun()

        st.markdown("---")
        st.markdown("**Sheet formatting:**\\nThe data will be appended as a structured block with Receipt Details (headers) followed by an Items Table.")
        
    uploaded_files = st.file_uploader("📸 Upload Receipt Images or PDFs", type=["jpg", "jpeg", "png", "webp", "pdf"], accept_multiple_files=True)
    
    if uploaded_files and api_key:
        if st.button("🚀 Process All with LangExtract & Save", type="primary", use_container_width=True):
            st.session_state["start_processing_batch"] = True
            
        if st.session_state.get("start_processing_batch", False):
            # Resolve Credentials Path
            if creds_file == "LOCAL":
                creds_path = "service_account.json"
            elif creds_file and sheet_url:
                creds_path = "temp_creds.json"
                with open(creds_path, "wb") as f:
                    f.write(creds_file.getvalue())
            else:
                creds_path = ""

            for i, uploaded_file in enumerate(uploaded_files):
                st.markdown(f"---")
                st.subheader(f"Processing: {uploaded_file.name}")
                
                # Fast UI-Level Block (prevent duplicate file names in the same active browser session)
                if uploaded_file.name in st.session_state.get("processed_session_files", set()):
                    st.success(f"⏭️ Automatically skipped **{uploaded_file.name}** (You already processed this exact file during this session).")
                    continue
                
                proceed_key = f"proceed_{uploaded_file.name}"
                
                file_ext = uploaded_file.name.lower().split('.')[-1]
                
                # Check for User-Skip
                if st.session_state.get(f"skip_{proceed_key}", False):
                    st.info(f"⏭️ Skipped {uploaded_file.name}.")
                    continue
                    
                file_bytes = uploaded_file.getvalue()
                is_duplicate = False
                
                # Pre-Extraction Dup Check!
                if creds_file and sheet_url and os.path.exists(creds_path):
                    with st.spinner("Fast checking for identical file hash before extraction..."):
                        is_duplicate = check_if_exists(file_bytes, creds_path, sheet_url)
                        
                if is_duplicate and not st.session_state.get(f"force_save_{proceed_key}", False):
                    st.warning(f"⚠️ An identical file for **{uploaded_file.name}** already exists in the Google Sheet.")
                    
                    btn_col1, btn_col2 = st.columns(2)
                    with btn_col1:
                        if st.button(f"Proceed & Extract Anyway", key=f"btn_save_{proceed_key}"):
                            st.session_state[f"force_save_{proceed_key}"] = True
                            st.rerun()
                    with btn_col2:
                        if st.button(f"Skip this receipt", key=f"btn_skip_{proceed_key}"):
                            st.session_state[f"skip_{proceed_key}"] = True
                            st.rerun()
                            
                    st.stop() # Freeze here until user chooses proceeding or skipping

                
                col1, col2 = st.columns([1, 2])
                
                # --- NEW PDF HANDLING LOGIC ---
                images_to_process = []
                
                if file_ext == "pdf":
                    with st.spinner("Converting PDF to images..."):
                        try:
                            # Use PyMuPDF (fitz) which is fully self-contained and requires no Windows system installs
                            pdf_document = fitz.open(stream=file_bytes, filetype="pdf")
                            for page_num in range(len(pdf_document)):
                                page = pdf_document.load_page(page_num)
                                # Get a high resolution image of the page
                                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                                img_bytes = pix.tobytes("png")
                                images_to_process.append(Image.open(BytesIO(img_bytes)))
                            pdf_document.close()
                        except Exception as e:
                            st.error(f"Failed to convert PDF. Ensure PyMuPDF is installed correctly: {e}")
                            st.stop()
                            
                    with col1:
                        st.write(f"PDF contains {len(images_to_process)} pages.")
                        # Display the first page as a preview
                        if images_to_process:
                            st.image(images_to_process[0], caption="Page 1 Preview", use_container_width=True)
                else:
                    # It's a standard image file
                    image = Image.open(uploaded_file)
                    images_to_process.append(image)
                    with col1:
                        st.image(image, caption="Uploaded Receipt", use_container_width=True)
                # ------------------------------
                
                with col2:
                    with st.spinner(f"Step 1: Running OCR..."):
                        try:
                            raw_ocr_text = ""
                            # If it's a multi-page PDF, run OCR on each page sequentially
                            for page_num, img in enumerate(images_to_process):
                                page_text = ocr_receipt(img, api_key)
                                raw_ocr_text += f"\\n--- Page {page_num + 1} ---\\n{page_text}"
                            
                            with st.expander("Show Raw OCR Text"):
                                st.text(raw_ocr_text)
                                
                            with st.spinner("Step 2: Extracting structures via LangExtract..."):
                                lx_results = extract_with_langextract(raw_ocr_text, api_key)
                                
                                st.success("✅ Extraction Successful!")
                                
                                # Parse out LangExtract results
                                processed_data = {
                                    "shop": "",
                                    "address": "",
                                    "date": "",
                                    "time": "",
                                    "discount": "",
                                    "total_amount": "",
                                    "items": []
                                }
                                
                                # Usually returns an AnnotatedDocument. Let's iterate extractions
                                if hasattr(lx_results, "extractions") and lx_results.extractions:
                                    for ext in lx_results.extractions:
                                        # ext is an lx.Extraction object
                                        cls = ext.extraction_class
                                        text = ext.extraction_text
                                        attrs = ext.attributes or {}
                                        
                                        if cls == "shop":
                                            processed_data["shop"] = text
                                        elif cls == "address":
                                            processed_data["address"] = text
                                        elif cls == "date":
                                            processed_data["date"] = text
                                        elif cls == "time":
                                            processed_data["time"] = text
                                        elif cls == "discount":
                                            processed_data["discount"] = text
                                        elif cls == "total_amount":
                                            processed_data["total_amount"] = text
                                        elif cls == "item":
                                            item_data = {
                                                "name": text,
                                                "quantity": attrs.get("quantity", "1"),
                                                "unit_price": attrs.get("unit_price", "0"),
                                                "price": attrs.get("total_price", "0")
                                            }
                                            processed_data["items"].append(item_data)
                                
                                st.subheader("Extracted Details")
                                st.write(f"**Place (Shop):** {processed_data['shop']}")
                                st.write(f"**Address:** {processed_data['address']}")
                                st.write(f"**Date:** {processed_data['date']} at {processed_data['time']}")
                                st.write(f"**Discount:** ${processed_data['discount']}")
                                st.write(f"**Total Amount:** ${processed_data['total_amount']}")
                                st.write(f"*(System Tracker Hash: `{generate_receipt_id(processed_data, uploaded_file.getvalue())}`)*")
                                
                                st.write("**Extracted Items:**")
                                if processed_data["items"]:
                                    items_df = pd.DataFrame(processed_data["items"])
                                    # Reorder columns
                                    if not items_df.empty:
                                        items_df = items_df[["name", "quantity", "unit_price", "price"]]
                                        items_df.columns = ["Item", "Quantity", "Unit Price", "Total Price"]
                                    st.table(items_df)
                                else:
                                    st.write("*No items extracted.*")
                                    
                                # Handle Google Sheets Saving
                                if creds_file and sheet_url and os.path.exists(creds_path):
                                    try:
                                        with st.spinner("Pushing to Google Sheets..."):
                                            append_to_sheet(processed_data, creds_path, sheet_url, file_bytes)
                                            
                                        # Handle Google Drive PDF Upload
                                        if drive_folder_id:
                                            with st.spinner("Updating Google Drive PDF..."):
                                                pdf_bytes = convert_to_pdf(file_bytes, file_ext)
                                                if pdf_bytes:
                                                    append_to_drive_pdf(pdf_bytes, drive_folder_id, creds_path)
                                                    st.success("📄 PDF logged to Google Drive successfully!")
                                                    
                                        # Record the file as successfully processed in the UI memory
                                        st.session_state.setdefault("processed_session_files", set()).add(uploaded_file.name)
                                        
                                        st.success("🎉 Successfully pushed to Google Sheets!")
                                        
                                        if i == len(uploaded_files) - 1:
                                            st.balloons()
                                            
                                    except Exception as e:
                                        st.error(f"❌ Error saving to Google Sheets: {e}")
                                else:
                                    st.warning("⚠️ Details were extracted, but not saved. Please provide the **Google Sheet URL** and **Service Account JSON** in the sidebar to save.")
                                    
                        except Exception as e:
                            st.error(f"Error during extraction: {e}")
                            st.code(traceback.format_exc())

            # Clean up temporary credentials after processing all files
            if 'creds_path' in locals() and os.path.exists(creds_path) and creds_path == "temp_creds.json":
                os.remove(creds_path)

if __name__ == "__main__":
    main()
