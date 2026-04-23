import os
from dotenv import load_dotenv
from email_agent import execute_google_calendar_tool, execute_excel_integration_tool

load_dotenv()

CREDS_FILE = "service_account.json"
SHEET_URL = os.environ.get("SHEET_URL")

print("--- Testing Google Calendar Integration ---")
cal_success, cal_detail = execute_google_calendar_tool(
    merchant="Dummy Hydro Bill",
    amount="150.25",
    due_date="2026-04-20",
    creds_path=CREDS_FILE
)
if cal_success:
    print(f"Calendar Event Created Successfully! Link: {cal_detail}")
else:
    print(f"Calendar Event Failed! Error: {cal_detail}")

print("\n--- Testing Google Sheets Integration ---")
sheet_success, sheet_detail = execute_excel_integration_tool(
    date="2026-04-16",
    vendor="Dummy Coffee Shop",
    amount="4.50",
    category="Dining",
    creds_path=CREDS_FILE,
    sheet_url=SHEET_URL
)
if sheet_success:
    print(f"Sheets appended successfully! {sheet_detail}")
else:
    print(f"Sheets failed! Error: {sheet_detail}")
