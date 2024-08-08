import os
import logging
import json
import mysql.connector
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from utils.db_utils import close_connection
from utils.utils import get_db_password
import boto3
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Environment variables
DB_USER = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_DATABASE")
DB_PORT = int(os.getenv("DB_PORT"))
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEYID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESSKEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = "kloo-mis-transaction"


def write_to_excel(query, conn, chunksize, file_path):
    logger.info("Starting to write data to Excel.")
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        start_row = 0
        for i, df_chunk in enumerate(
            pd.read_sql_query(query, conn, chunksize=chunksize)
        ):
            logger.info(
                f"Chunk {i + 1} data:\n{df_chunk.head()}"
            )  # Print the first few rows of the chunk
            if i == 0:
                df_chunk.to_excel(
                    writer, sheet_name="DATA", index=False, startrow=start_row
                )
            else:
                start_row = i * chunksize + 1
                df_chunk.to_excel(
                    writer,
                    sheet_name="DATA",
                    index=False,
                    startrow=start_row,
                    header=False,
                )
            logger.info(f"Written chunk {i + 1} to Excel.")
    logger.info("Finished writing data to Excel.")


def upload_to_s3(file_path, s3_bucket, s3_key):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    try:
        s3_client.upload_file(file_path, s3_bucket, s3_key)
        logger.info(
            f"File uploaded successfully to S3 bucket {s3_bucket} under {s3_key}"
        )
    except ClientError as e:
        logger.error(f"Failed to upload file to S3: {e}")
        raise


def download_from_s3(s3_bucket, s3_key, download_path):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    try:
        s3_client.download_file(s3_bucket, s3_key, download_path)
        logger.info(
            f"File downloaded successfully from S3 bucket {s3_bucket} under {s3_key}"
        )
    except ClientError as e:
        logger.error(f"Failed to download file from S3: {e}")
        raise


def send_email_via_sesv2(
    sender_email,
    recipient_emails,
    subject,
    body_text,
    attachment_path=None,
    aws_region=AWS_REGION,
):
    ses_client = boto3.client("sesv2", region_name=aws_region)

    # Create a multipart/mixed parent container
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = f"Kloo <{sender_email}>"
    msg["To"] = ", ".join(recipient_emails)

    # Create a multipart/alternative child container
    msg_body = MIMEMultipart("alternative")
    textpart = MIMEText(body_text.encode("utf-8"), "plain", "utf-8")
    msg_body.attach(textpart)

    # Attach the multipart/alternative child container to the multipart/mixed parent container
    msg.attach(msg_body)

    # Attach the file as an attachment if provided
    if attachment_path:
        try:
            with open(attachment_path, "rb") as file:
                att = MIMEApplication(file.read())
                att.add_header(
                    "Content-Disposition",
                    "attachment",
                    filename=os.path.basename(attachment_path),
                )
                msg.attach(att)
        except IOError as e:
            logger.error(f"Failed to attach file {attachment_path}: {e}")
            raise

    try:
        # Send the email
        response = ses_client.send_email(
            FromEmailAddress=sender_email,
            Destination={"ToAddresses": recipient_emails},
            Content={
                "Raw": {
                    "Data": msg.as_string(),
                }
            },
        )
        logger.info("Email sent successfully!")
        return response["MessageId"]
    except ClientError as e:
        logger.error(f"Failed to send email via SESv2: {e}")
        raise


def generate_report_and_upload_to_s3():
    conn = None
    try:
        logger.info("Environment variables loaded.")

        conn = mysql.connector.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_NAME,
            port=DB_PORT,
        )
        logger.info("Database connection established.")
    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
        return {"statusCode": 500, "body": json.dumps(f"Database error: {str(err)}")}

    try:
        query = """SELECT * FROM `invoiceOcrFailedFields`;"""
        erp_sync_query = """select `organizations`.`organization_name` AS 'Organisation',`invoice_ocr_logs`.created_at AS 'Date:Time', `organization_entity_details`.field_name AS 'Entity',`suppliers`.`name` AS 'Supplier',`invoices`.`invoice_number` AS 'Invoice Number',`invoice_ocr_logs`.invoice_failure_field AS 'Failed Field', `invoice_ocr_logs`.invoice_failure_reason AS 'Failure reason'
                            from `invoices` 
                            inner join `user_organization_details` on `invoices`.`user_org_id` = `user_organization_details`.`id` 
                            left join `suppliers` on `invoices`.`supplier_id` = `suppliers`.`id` 
                            inner join `users` on `user_organization_details`.`user_id` = `users`.`id` 
                            inner join `organizations` on `organizations`.`id` = `invoices`.`organization_id` 
                            LEFT JOIN `invoice_ocr_logs` ON `invoices`.id = `invoice_ocr_logs`.invoice_id
                            LEFT JOIN `organization_entity_details` ON `invoices`.entity_id = `organization_entity_details`.id 
                            where `invoices`.`deleted_at` is NULL 
                            AND invoices.status = "draft" 
                            AND `invoice_ocr_logs`.document_id IS NOT NULL AND `invoice_ocr_logs`.invoice_failure_field IS NOT NULL;"""
        current_date = datetime.now().strftime("%d-%m-%Y")
        chunksize = 200
        file_path = f"/tmp/Kloo-Mis-InvoiceFieldFailure_Report_{current_date}.xlsx"
        erp_sync_file_path = (
            f"/tmp/Kloo-MIS-ERP-Invoice-Sync_Report_{current_date}.xlsx"
        )

        logger.info("Writing data to Excel file...")
        write_to_excel(query, conn, chunksize, file_path)

        logger.info("Writing ERP Invoice Sync report to Excel file...")
        write_to_excel(erp_sync_query, conn, chunksize, erp_sync_file_path)

        s3_key = f"Kloo-Mis-InvoiceFieldFailure_Report_{current_date}.xlsx"
        logger.info(f"Uploading file to S3 bucket {S3_BUCKET_NAME} under {s3_key}...")
        upload_to_s3(file_path, S3_BUCKET_NAME, s3_key)

        erp_sync_s3_key = f"Kloo-MIS-ERP-Invoice-Sync_Report_{current_date}.xlsx"
        logger.info(
            f"Uploading file to S3 bucket {S3_BUCKET_NAME} under {erp_sync_s3_key}..."
        )
        upload_to_s3(erp_sync_file_path, S3_BUCKET_NAME, erp_sync_s3_key)

        # Send the email with the attachment
        sender_email = "support@getkloo.com"
        recipient_emails = [
            "deepika.kangne@blenheimchalcot.com"
        ]
        subject = f"Platform.Getkloo.Com: MIS Report Invoice Feild Failure Order {current_date}"
        body_text = "Please find your MIS report attached. Download File."

        logger.info("Sending email with attachment...")
        send_email_via_sesv2(
            sender_email,
            recipient_emails,
            subject,
            body_text,
            attachment_path=file_path,
        )

        erp_sync_subject = (
            f"Platform.Getkloo.Com: MIS Invoice Field Failure Report_ {current_date}"
        )
        erp_sync_body_text = (
            "Please find your ERP Invoice Sync MIS report attached. Download File."
        )

        logger.info("Sending Invoice Field Failure Report Status email with attachment...")
        send_email_via_sesv2(
            sender_email,
            recipient_emails,
            erp_sync_subject,
            erp_sync_body_text,
            attachment_path=erp_sync_file_path,
        )

        return {
            "statusCode": 200,
        }
    except Exception as error:
        logger.error(f"An unexpected error occurred: {error}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"An error occurred: {str(error)}"),
        }
    finally:
        if conn:
            close_connection(conn)
            logger.info("Database connection closed.")


if __name__ == "__main__":
    generate_report_and_upload_to_s3()
