import numpy as np
import pandas as pd
import boto3
import json
import warnings

warnings.filterwarnings("ignore")
import mysql.connector
from io import BytesIO
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# Fetch secrets
client = boto3.client("secretsmanager")
response = client.get_secret_value(SecretId="Kloo-ChatGpt")
data = json.loads(response["SecretString"])

response2 = client.get_secret_value(SecretId="kloo-dev-environment-variables")
data2 = json.loads(response2["SecretString"])

EMAIL_PASSWORD_KLOOCHATGPT = data["EMAIL_PASSWORD_KLOOCHATGPT"]
KLOO_PROD_DB_PASSWORD_KLOOCHATGPT = data["KLOO_PROD_DB_PASSWORD_KLOOCHATGPT"]
KLOO_DEV_DB_PASSWORD_KLOOCHATGPT = data2["Dev_DB_PASSWORD"]

smtp_server = "email-smtp.eu-west-2.amazonaws.com"
smtp_port = 587
username = "AKIA2K7IVGYFQ7Y64Q67"
sender_email = "support@getkloo.com"
mail_name_from = "Kloo"
sender_password = EMAIL_PASSWORD_KLOOCHATGPT
recipient_emails = [
    "vaibhav.chotaliya@blenheimchalcot.com",
    "zeeshan.siddiquie@blenheimchalcot.com",
]


def lambda_handler(event, context):
    try:
        # Connect to the database server
        mydb = mysql.connector.connect(
            host="mysql-kloo-prod.internal-service-kloo.com",
            user="masterkloo",
            password=KLOO_PROD_DB_PASSWORD_KLOOCHATGPT,
            database="myrdssql01",
            port=3306,
        )
        cursor = mydb.cursor()
        today = datetime.datetime.now().strftime("%Y-%m-%d")

        # Initialize Excel writer
        buffer = BytesIO()
        writer = pd.ExcelWriter(buffer, engine="openpyxl")
        chunk_size = 1000
        offset = 0

        # Call the stored procedure and fetch data in chunks
        cursor.callproc("all-transactions-report")

        # Fetch data in chunks and write to Excel
        for result in cursor.stored_results():
            while True:
                rows = result.fetchmany(chunk_size)
                if not rows:
                    break

                # Convert to DataFrame
                columns = result.column_names
                df_chunk = pd.DataFrame(rows, columns=columns)
                df_chunk.to_excel(
                    writer, sheet_name=f"Chunk_{offset // chunk_size}", index=False
                )

                offset += chunk_size

        writer.save()
        excel_data = buffer.getvalue()

        # Create a MIME message with the Excel file attachment
        msg = MIMEMultipart()
        msg["Subject"] = f"Platform.Getkloo.Com: MIS Report Transactions Order {today}"
        msg["From"] = f"Kloo <{sender_email}>"
        msg["To"] = ", ".join(recipient_emails)

        part = MIMEApplication(excel_data, Name=f"transactions_{today}.xlsx")
        part["Content-Disposition"] = (
            f'attachment; filename="transactions_{today}.xlsx"'
        )
        msg.attach(part)

        # Send the email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(username, sender_password)
            server.sendmail(sender_email, recipient_emails, msg.as_string())

        print("Email sent successfully!")
        mydb.close()
    except Exception as e:
        print(f"Error: {type(e).__name__} - {str(e)}")
    finally:
        print("Completed")
