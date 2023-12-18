import imaplib
import email
import logging
from logging.handlers import TimedRotatingFileHandler
from configparser import ConfigParser
import pandas as pd
import re
from dateutil import parser

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
    config.read("properties.ini")

    username = config["email"]["username"]
    password = config["email"]["password"]
    output_file = config["output"]["output_file"]

    gmail_host = "imap.gmail.com"
    mail = imaplib.IMAP4_SSL(gmail_host)  # Set connection
    mail.login(username, password)  # login to the mail

    logger.info("Logged into gmail account")

    mail.select("INBOX")

    result, mail_numbers = mail.search(None, "All")

    logger.info("Reading mails..")

    date_df = []  # creating empty list to store dates
    links = []  # creating empty list to store links

    # Reading the mails
    for num in mail_numbers[0].split():
        result, data = mail.fetch(num, '(RFC822)')

        email_message = email.message_from_bytes(data[0][1])

        # print("Subject: ", email_message["Subject"])
        # print("To: ", email_message["to"])
        # print("From: ", email_message["from"])

        x = parser.parse(email_message["date"])
        date = x.strftime("%Y%m%d")
        print("Date: ", date)

        print("-------------------\n")

        logger.info("finding links in the message")

        for part in email_message.walk():
            if part.get_content_type() == "text/plain" or part.get_content_type() == "text/html":
                message = part.get_payload(decode=True)
                # print("Message: \n", message.decode())

                # searching for links in the mail
                link = re.findall("(?P<url>https?://[^\s]+)", message.decode())
                for one_link in link:
                    print(one_link)
                    date_df.append(date)
                    links.append(one_link)

                if not link:  # condition to check no links in the mail
                    print("No links in the mail")

                print("-------------------\n")

                break

    # Creating a DataFrame to insert Dates and Links
    data_df = {"Date": date_df, "Links": links}
    df = pd.DataFrame(data_df)

    logger.info("Inserting the records in to excel file")

    # Inserting data into Excel file
    if not data_df["Links"] == []:
        df.to_excel(output_file, index=False)

    logger.info("Records inserted")

    mail.close()
    mail.logout()

except FileNotFoundError as e:
    error_message = f"File is not found, {e}"
    print(error_message)
    logger.error(error_message)

except imaplib.IMAP4.error as e:
    error_message = f"{e}"
    logger.error(error_message)
    print(error_message)

except Exception as e:
    error_message = f"Error occurred, {e}"
    print(error_message)
    logger.error(error_message)
