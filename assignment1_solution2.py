import threading
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys
from configparser import ConfigParser

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


# Function to insert the unique values to txt file
def get_uniq(unique, h):
    with open(uniq_file, 'w') as f:
        f.write(h)
        for li in unique:
            li = "#".join(li)
            f.writelines(str(li) + '\n')


# Function to insert the duplicates values to txt file
def get_dup(dup, h):
    with open(dup_file, 'w') as f:
        f.write(h)
        for li in dup:
            li = "#".join(li)
            f.writelines(str(li) + '\n')


# Function to sort Salary data and insert those into txt file
def get_sort(unique, h):
    sort_li = sorted(unique, key=lambda val: int(val[-1].replace('â‚¬', '').replace('$', '').replace(',', '')),
                     reverse=True)

    with open(sort_file, 'w') as f:
        f.write(h)
        for li in sort_li:
            li = "#".join(li)
            f.writelines(str(li) + '\n')


if __name__ == "__main__":
    try:
        config = ConfigParser()
        config.read('properties.ini')
        data_file = config['File']['data_file']
        uniq_file = config['File']['uniq_file']
        dup_file = config['File']['dup_file']
        sort_file = config['File']['sort_file']

        if not os.path.isfile(data_file):  # Condition to check Input File Existence.
            logger.error("No input file provided.")
            print("No input file provided!")
            sys.exit(1)  # if file not exist it will exit

        else:  # Condition if file Exist

            file = open(data_file, 'r')  # Read all the data from text file
            header = next(file)

            unique = []
            dup = []
            for line in file:
                line = line.strip()
                line = line.split('#')

                if line not in unique:
                    unique.append(line)
                else:
                    dup.append(line)

            t1 = threading.Thread(target=get_uniq, args=(unique, header,))  # calling the get_uniq function
            t1.start()
            logger.info("uniq.txt file is created and unique records are inserted in the file")

            t2 = threading.Thread(target=get_dup, args=(dup, header,))  # calling the get_dup function
            t2.start()
            logger.info("dup.txt file is created and duplicate records are inserted in the file")

            t3 = threading.Thread(target=get_sort, args=(unique, header,))  # calling the get_sort function
            t3.start()
            logger.info("sort.txt file is created and sorted records are inserted in the file")

    except FileNotFoundError as e:
        error_message = f"File not found: {e}"
        logger.error(error_message)
        print(error_message, flush=True)

    except Exception as e:
        error_message = f"Error occurred: {e}"
        logger.error(error_message)
        print(error_message)
