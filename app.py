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
        query = """SELECT * FROM `all-transactions-report`;"""
        erp_sync_query = """SELECT
                        A.Organisation,
                        A.Entity,
                        A.Supplier,
                        A.`Invoice Number`,
                        A.Description,
                        A.`Net Amount`,
                        A.`Tax Amount`,
                        A.`Total Amount`,
                        B.`Approval date`,
                        A.`Sync status`,
                        ial.message AS Reason
                    FROM (SELECT
                            i.id,
                            REPLACE(i.id, '-', '') AS invoice_id_without_hyphens,
                            o.organization_name AS Organisation,
                            oed.field_name AS Entity,
                            s.name AS Supplier,
                            i.invoice_number AS `Invoice Number`,
                            i.description AS Description,
                            i.net_amount AS `Net Amount`,
                            i.tax_amount AS `Tax Amount`,
                            i.amount AS `Total Amount`,
                            CASE
                                WHEN i.send_to_accounting_portal = 'pending' THEN 'Pending'
                                WHEN i.send_to_accounting_portal = 'success' THEN 'Success'
                                WHEN i.send_to_accounting_portal = 'failed' THEN 'Failed'
                                ELSE i.send_to_accounting_portal
                            END AS `Sync status`
                        FROM invoices i
                        JOIN suppliers s ON s.id = i.supplier_id
                        
                        JOIN organizations o ON o.id = i.organization_id
                        JOIN organization_entity_details oed ON oed.id = i.entity_id AND oed.organization_id = i.organization_id 
                        WHERE i.send_to_accounting_portal IN ('pending','success','failed') AND o.organization_name != 'Kloo QA' AND i.deleted_at IS NULL AND s.deleted_at IS NULL ) AS A
                    LEFT JOIN (SELECT 
                            ApprovedDate.`Approval date`, 
                            ApprovedDate.event_ref_id
                        FROM 
                            (SELECT
                                    wa.updated_at AS `Approval date`,
                                    wa.event_ref_id,
                                    ROW_NUMBER() OVER (PARTITION BY wa.event_ref_id ORDER BY wa.created_at DESC) AS row_num
                                FROM
                                    workflow_activities wa
                                    INNER JOIN workflows w ON wa.workflow_id = w.id
                                WHERE
                                    w.workflow_type = 'account-payable-approval'
                                    AND wa.flow_completed = 1
                                    AND w.deleted_at IS NULL
                                    AND wa.deleted_at IS NULL) AS ApprovedDate
                        WHERE 
                            row_num = 1) AS B
                    ON A.id = B.event_ref_id
                    LEFT JOIN invoice_attachment_logs ial ON ial.invoice_id = A.invoice_id_without_hyphens;"""
        current_date = datetime.now().strftime("%d-%m-%Y")
        chunksize = 200
        file_path = f"/tmp/Kloo-Mis-Transaction_Report_{current_date}.xlsx"
        erp_sync_file_path = (
            f"/tmp/Kloo-MIS-ERP-Invoice-Sync_Report_{current_date}.xlsx"
        )

        logger.info("Writing data to Excel file...")
        write_to_excel(query, conn, chunksize, file_path)

        logger.info("Writing ERP Invoice Sync report to Excel file...")
        write_to_excel(erp_sync_query, conn, chunksize, erp_sync_file_path)

        s3_key = f"Kloo-Mis-Transaction_Report_{current_date}.xlsx"
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
            "deepika.kangne@blenheimchalcot.com",
            "vaibhav.chotaliya@getkloo.com",
            "shrinivas.krishnamurthy@getkloo.com",
            "sven.huckstadt@blenheimchalcot.com",
            "max.kingdon@getkloo.com",
            "tim.baker@getkloo.com",
            "emmanuel.oyemade@getkloo.com",
            "sahil.shaikh@getkloo.com",
            "shahrukh.shaikh@getkloo.com",
            "samarjit.yadav@getkloo.com",
            "atul.kale@getkloo.com",
            "ai@getkloo.com",
            "zeeshan.siddiquie@blenheimchalcot.com",
            "romil.Shah@blenheimchalcot.com",
            "kartike.kumar@blenheimchalcot.com",
            "snehal.engley@getkloo.com",
        ]
        subject = f"Platform.Getkloo.Com: MIS Report Transactions Order {current_date}"
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
            f"Platform.Getkloo.Com: MIS ERP Invoice Sync Report {current_date}"
        )
        erp_sync_body_text = (
            "Please find your ERP Invoice Sync MIS report attached. Download File."
        )

        logger.info("Sending ERP Invoice Sync Status email with attachment...")
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
