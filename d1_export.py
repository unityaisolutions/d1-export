import os
import io
import csv
import modal

# 1. Initialize the Modal App and define environment dependencies
app = modal.App("cloudflare-d1-export")
image = modal.Image.debian_slim(python_version="3.12").pip_install("resend", "requests")

# 2. Schedule the CRON job (e.g., runs daily at midnight UTC)
@app.function(
    image=image,
    schedule=modal.Cron("0 0 * * *"),
    secrets=[
        modal.Secret.from_name("cloudflare-secrets"),
        modal.Secret.from_name("resend-secrets")
    ]
)
def export_d1_to_csv_and_email():
    import requests
    import resend

    # Retrieve environment variables
    cf_account_id = os.environ["CLOUDFLARE_ACCOUNT_ID"]
    cf_db_id = os.environ["CLOUDFLARE_DATABASE_ID"]
    cf_api_token = os.environ["CLOUDFLARE_API_TOKEN"]
    resend.api_key = os.environ["RESEND_API_KEY"]
    
    # Define your specific table and sender address
    table_name = "your_table_name"
    sender_email = "onboarding@resend.dev" # Replace with your verified Resend domain
    target_email = "namelessonbandlab@outlook.com"

    # 3. Query Cloudflare D1 via the REST API
    url = f"https://api.cloudflare.com/client/v4/accounts/{cf_account_id}/d1/database/{cf_db_id}/query"
    headers = {
        "Authorization": f"Bearer {cf_api_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "sql": f"SELECT * FROM {table_name}"
    }
    
    print(f"Querying D1 table: {table_name}...")
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json()
    
    # Validate successful data retrieval
    if not data.get("success") or not data["result"][0].get("results"):
        print("Query failed or the table returned no results.")
        return
        
    rows = data["result"][0]["results"]
    
    # 4. Convert the JSON rows into a CSV string buffer
    print(f"Exporting {len(rows)} rows to CSV...")
    csv_buffer = io.StringIO()
    if rows:
        writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        
    csv_content = csv_buffer.getvalue()
    
    # Convert string to a list of bytes (integer array) for the Resend API
    file_bytes = list(csv_content.encode("utf-8"))
    
    # 5. Email the CSV via the Resend Python SDK
    print("Dispatching email via Resend...")
    params: resend.Emails.SendParams = {
        "from": sender_email,
        "to": [target_email],
        "subject": f"Automated D1 Database Export: {table_name}",
        "html": f"<p>Please find the latest CSV export for the <strong>{table_name}</strong> table attached.</p>",
        "attachments": [
            {
                "filename": f"{table_name}_export.csv",
                "content": file_bytes
            }
        ]
    }
    
    email_response = resend.Emails.send(params)
    print(f"Email sent successfully! ID: {email_response}")