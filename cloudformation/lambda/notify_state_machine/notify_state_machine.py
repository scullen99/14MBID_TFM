import json
import os
import http.client
import urllib.parse
import datetime

def lambda_handler(event, context):
  
    execution_name = event.get("execution_name", "Unknown")
    state_machine_name = event.get("state_machine_name", "Unknown")
    execution_arn = event.get("execution_arn", "Unknown")
    status = event.get("status", "UNKNOWN")

    # Obtener la hora actual con formato adecuado
    current_time = (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

    # Construir el enlace de la ejecución en AWS
    region = os.getenv("AWS_REGION", "eu-west-1")  # Por defecto, tu región
    execution_link = f"https://{region}.console.aws.amazon.com/states/home?region={region}#/executions/details/{execution_arn}"

    # Definir color según el estado de la ejecución
    color = "#ff62b4" if status == "SUCCEEDED" else "#FF0000" if status == "FAILED" else "#3498db"

    # Construcción del mensaje de Slack
    message = {
        "attachments": [
            {
                "color": color,
                "title": f"Step Functions Execution Notification",
                "fields": [
                    {"title": "State Machine", "value": state_machine_name, "short": True},
                    {"title": "Execution ID", "value": execution_name, "short": True},
                    {"title": "Status", "value": status, "short": True},
                    {"title": "Execution Time", "value": current_time, "short": True},
                    {"title": "Execution Link", "value": f"<{execution_link}|Enlace Ejecución - Ver en AWS Console>", "short": False}
                ],
                "footer": "AWS Lambda Notification"
            }
        ]
    }

    # Si la ejecución ha finalizado, calcular la duración
    if "start_time" in event and "end_time" in event:
        start_time = datetime.datetime.strptime(event["start_time"], "%Y-%m-%d %H:%M:%S")
        end_time = datetime.datetime.strptime(event["end_time"], "%Y-%m-%d %H:%M:%S")
        duration = str(end_time - start_time)

        message["attachments"][0]["fields"].append({"title": "Execution Duration", "value": duration, "short": True})

    # Obtener el Webhook de Slack desde las variables de entorno
    #slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_webhook_url:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "SLACK_WEBHOOK_URL no configurado en las variables de entorno"})
        }

    # Enviar el mensaje a Slack
    parsed_url = urllib.parse.urlparse(slack_webhook_url)
    conn = http.client.HTTPSConnection(parsed_url.netloc)
    headers = {"Content-Type": "application/json"}
    conn.request("POST", parsed_url.path, body=json.dumps(message), headers=headers)
    response = conn.getresponse()

    return {
        "statusCode": response.status,
        "body": json.dumps({
            "message": "Notificación enviada a Slack!",
            "state_machine": state_machine_name,
            "execution_id": execution_name,
            "status": status,
            "execution_time": current_time
        })
    }