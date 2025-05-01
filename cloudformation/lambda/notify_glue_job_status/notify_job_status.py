import json
import os
import http.client
import urllib.parse
import datetime

def lambda_handler(event, context):
    job_name = event.get("JobName", "Unknown")
    status = event.get("Status", "UNKNOWN")
    
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    current_datetime = (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

    color = "#36a64f" if status == "SUCCEEDED" else "#FF0000"
    message = {
        "attachments": [
            {
                "color": color,
                "title": "AWS Glue Job Notification",
                "fields": [
                    {"title": "Job Name", "value": job_name, "short": True},
                    {"title": "Status", "value": status, "short": True},
                    {"title": "Fecha y Hora", "value": current_datetime, "short": True}
                ],
                "footer": "AWS Lambda Notification"
            }
        ]
    }

    # Parse Slack Webhook URL
    parsed_url = urllib.parse.urlparse(slack_webhook_url)
    conn = http.client.HTTPSConnection(parsed_url.netloc)

    headers = {"Content-Type": "application/json"}
    conn.request("POST", parsed_url.path, body=json.dumps(message), headers=headers)
    response = conn.getresponse()
    
    return {
        "statusCode": response.status,
        "body": json.dumps({
            "message": "Notification sent to Slack!",
            "status": status,
            "fecha_hora": current_datetime
        })
    }