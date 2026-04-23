import functions_framework
from email_agent_function import run_agent

@functions_framework.http
def process_daily_emails(request):
    """HTTP Cloud Function that calls our email agent block."""
    msg, code = run_agent(request)
    return msg, code
