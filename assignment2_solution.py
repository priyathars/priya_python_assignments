import logging
import smtplib
import ssl
from email.message import EmailMessage
from logging.handlers import TimedRotatingFileHandler
from configparser import ConfigParser
import os
import sys
import pandas as pd

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(funcName)s : %(lineno)d : %(message)s',
    handlers=[
        logging.handlers.TimedRotatingFileHandler(
            filename=r"Runtime_log.log",
            when="D",
            interval=1,
            backupCount=0,
        )
    ]
)

try:
    config = ConfigParser()
    config.read('properties.ini')
    input_file = config['Input_File']['Input_file_path']
    sender_mail = config['email']['sender_mail']
    receiver_mail = config['email']['receiver_mail']
    subject = "Incident Ticket Details"
    context = ssl.create_default_context()

    logger.info("Input file checking..")
    print("Input file checking")

    if not os.path.exists(input_file):  # Condition to check Input File Existence.
        logger.error("Input file is not provided.")
        print("Input file is not provided")
        sys.exit(1)  # if file not exist it will exit

    else:  # Condition if file Exist then it will run
        input_df = pd.read_excel(input_file, index_col=0)  # Read all the data from excel file

        print("Reading the Input Excel File")
        logger.info("Reading the Input Excel File")

        grouped_df = input_df.query("Status == 'Active'").groupby('Error_description', axis=0)

        body = []  # creating empty list to hold the body data
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 2000)

        for state, frame in grouped_df:
            if state == 'caused because of the database connection error':
                body.append("Database Error")
                body.append("--------------------")
                body.append(frame)
            elif state == 'caused because of the network issue':
                body.append("Network Issue")
                body.append("--------------------")
                body.append(frame)
            elif state == 'caused because of the Access Denied issue':
                body.append("Access Issue")
                body.append("--------------------")
                body.append(frame)

        msg = '\n'.join(map(str, body))

        message = EmailMessage()
        message['from'] = sender_mail
        message['to'] = receiver_mail
        message['subject'] = subject
        message.set_content(msg)

        logger.info("Sending email")
        print("Sending email")

        # Connecting to gmail and sending the email
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp_server:
            smtp_server.ehlo()
            password = input("Enter your password: ")
            smtp_server.login(sender_mail, password)
            smtp_server.sendmail(sender_mail, receiver_mail, message.as_string())
            smtp_server.close()

        logger.info("Mail sent to client")
        print("Message sent!")


except FileNotFoundError as e:
    error_message = f"File is not found, {e}"
    logger.error(error_message)
    print(error_message)

except smtplib.SMTPException as e:
    error_message = f"{e}"
    logger.error(error_message)
    print(error_message)

except Exception as e:
    error_message = f"Error occurred, {e}"
    logger.error(error_message)
    print(error_message)
