# 🧾 Receipt to Sheets Extractor & Financial Automation Agent

This repository contains a suite of tools designed to automate the extraction of financial data from receipts, invoices, and automated billing emails. The project features a **Streamlit Web Dashboard** for manual uploads, as well as several autonomous **Python Email Agents** for background processing. 

At its core, it leverages **Google's Gemini 2.5** and **LangExtract** to convert unstructured documents (images/PDFs/emails) into structured data, which is then automatically logged into **Google Sheets** and archived as monthly PDFs in **Google Drive**.

---

## ✨ Features

### 🖥️ Streamlit Web Dashboard (`app.py`)
- **Drag-and-Drop Interface**: Upload receipt images (JPG/PNG) or PDFs directly via the UI.
- **Smart Data Extraction**: Uses LangExtract and Gemini to parse Shop Name, Address, Date, Time, Individual Items, Quantities, Prices, and Grand Totals.
- **Duplicate Detection**: Hashes images to prevent uploading the exact same receipt twice.
- **Google Sheets Sync**: Appends formatted receipt data into designated tabs (`receipt_tracker`, `Summary`).
- **Google Drive PDF Archiving**: Automatically converts uploaded images into a master Monthly PDF in a shared Google Drive folder to save space.

### 🤖 Autonomous Email Agents
The project includes several purpose-built scripts to scan an email inbox and process bills automatically:
- **`email_agent.py` & `email_history_agent.py`**: Scans for general bills and receipts.
- **`email_order_agent.py`**: Specifically parses and extracts Amazon shipping and order details.
- **`email_alectra_agent.py`**: Tailored for extracting utility data from Alectra bills.
- **`email_peel_agent.py`**: Tailored for Region of Peel Water bills.
- **`telegram_agent.py`**: Sends push notifications or alerts to a Telegram bot based on extraction status.

---

## 🚀 Getting Started

### 1. Prerequisites
- Python 3.12+
- A Google Cloud Project with the following APIs enabled:
  - **Google Sheets API**
  - **Google Drive API**
- A **Google Service Account JSON** with access to your Sheets and Drive folders.
- A **Google Gemini API Key**.

### 2. Local Installation

Clone the repository and install the required dependencies:
```bash
git clone https://github.com/sree-yarru/Expensive_analyzer.git
cd Expensive_analyzer

# Install dependencies
pip install -r requirements.txt
```

### 3. Environment Variables
Copy the provided `.env.sample` to a new `.env` file and fill in your keys:
```bash
cp .env.sample .env
```
Ensure you provide your `GEMINI_API_KEY`, `SHEET_URL`, and the path/contents for your Google Service Account credentials.

### 4. Running Locally
To launch the Streamlit Web UI:
```bash
streamlit run app.py
```
To run one of the background email agents:
```bash
python email_agent.py
```

---

## ☁️ Deployment (Google Cloud Run)

The application is fully containerized and ready to be deployed to Google Cloud Run.

1. Ensure the `Dockerfile` and `.dockerignore` are present.
2. Run the deployment command:
```bash
gcloud run deploy receipt-extractor \
  --source . \
  --memory 2Gi \
  --allow-unauthenticated \
  --timeout 3600
```
*(Note: 2GB of memory is recommended due to the heavy ML, Pandas, and PyMuPDF libraries required for local PDF/Image processing).*

---

## 🔒 Security & Privacy
- **No stored secrets**: The `.gitignore` is configured to ignore `.env` and `service_account.json`. Never commit your API keys or Google Service accounts.
- **In-Memory Processing**: Receipts processed via the Streamlit app are handled completely in-memory (using `BytesIO`) and are never saved to the local disk, ensuring your private financial data is not leaked.

## 📄 License
Private Repository.
