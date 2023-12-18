import threading
import pandas as pd
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys
from configparser import ConfigParser
import time

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


# Function to filter the unique values from dataframe
def get_uniq(df):
    uniq_df = df.drop_duplicates(keep="first")
    uniq_df.to_csv(uniq_file, sep='#', index=False)


# Function to retrieve the duplicates values from dataframe
def get_dup(df):
    dup_df = df[df.duplicated()]
    # df2 = df.pivot_table(index=['Name', 'age', 'company', 'gender', 'Salary'], aggfunc ='size')
    grouped_dup_df = dup_df.groupby(dup_df.columns.tolist(), as_index=False).size()

    grouped_dup_df.to_csv(dup_file, sep="#", index=False)


# Function to sort Salary data from the dataframe
def get_sort(df):
    uniq_df = df.drop_duplicates(keep="first")
    sorted_df = uniq_df.sort_values(by='Salary', ascending=False,
                                    key=lambda val: val.str.replace('€', '', regex=True).str.replace('$', '',
                                                                                                     regex=True).str.replace(
                                        ',', '', regex=True).astype(float))
    sorted_df.to_csv(sort_file, sep="#", index=False)


# calculate the normal execution time
def calculate_normal_time(t1, t2, t3):
    start_time = time.time()
    th_time = calculate_threading_time(t1, t2, t3)

    t1.join()
    t2.join()
    t3.join()

    f_time = time.time() - start_time

    return th_time, f_time


# Calculate the thread time
def calculate_threading_time(t1, t2, t3):
    start_time = time.time()
    t1.start()
    logger.info("uniq.txt file is created and unique records are inserted in the file")

    t2.start()
    logger.info("dup.txt file is created and duplicate records are inserted in the file")

    t3.start()
    logger.info("sort.txt file is created and sorted records are inserted in the file")

    thread_time = time.time() - start_time
    return thread_time


if __name__ == "__main__":
    try:
        config = ConfigParser()
        config.read('properties.ini')
        data_file = config['File']['data_file']
        uniq_file = config['File']['uniq_file']
        dup_file = config['File']['dup_file']
        sort_file = config['File']['sort_file']
        runcode_time_file = config['File']['runcode_time_file']

        if not os.path.isfile(data_file):  # Condition to check Input File Existence.
            logger.error("No input file provided.")
            print("No input file provided!")
            sys.exit(1)  # if file not exist it will exit

        else:  # Condition if file Exist
            main_df = pd.read_csv(data_file, sep="#", on_bad_lines='skip')  # Read all the data from text file

            t1 = threading.Thread(target=get_uniq, args=(main_df,))  # calling the get_uniq function

            t2 = threading.Thread(target=get_dup, args=(main_df,))  # calling the get_dup function

            t3 = threading.Thread(target=get_sort, args=(main_df,))  # calling the get_sort function

        # Inserting execution time into txt file
        t_time, n_time = calculate_normal_time(t1, t2, t3)
        lines = [f"i) normal execution time: {n_time}\n", f"ii) MultiThreading Code Time: {t_time}"]
        with open(runcode_time_file, 'w') as f:
            for line in lines:
                f.write(line)
        logger.info("Runcode_time records inserted in the txt file")

    except FileNotFoundError as e:
        error_message = f"File not found: {e}"
        logger.error(error_message)
        print(error_message, flush=True)

    except Exception as e:
        error_message = f"Error occurred: {e}"
        logger.error(error_message)
        print(error_message)
