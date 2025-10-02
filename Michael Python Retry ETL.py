from tenacity import retry, stop_after_attempt, wait_fixed
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

Email notification function
def send_failure_email(subject, body, to_email):
    from_email = "your_email@example.com"
    password = "your_email_password"
    smtp_server = "smtp.example.com"
    smtp_port = 587

msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(from_email, password)
            server.sendmail(from_email, to_email, msg.as_string())
        print("Failure email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

Retry logic for ETL task
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
def run_etl_task():
    try:
        # Import the notebook execution library
        import papermill as pm

# Execute the notebook
        pm.execute_notebook(
            #point it at an updated version of the code perhaps
            'https://raw.githubusercontent.com/ingwanelabs/etl-pipeline/main/etl/data-enrichment_load.ipynb',
            'output_notebook.ipynb'
        )
        print("ETL task completed successfully.")
    except Exception as e:
        send_failure_email(
            subject="ETL Pipeline Failure",
            body=f"The ETL pipeline failed with error: {e}",
            to_email="recipient@example.com"
        )
        raise

Run the ETL task with retry and notification
try:
    run_etl_task()
except Exception as final_error:
    print(f"ETL task failed after retries: {final_error}")
