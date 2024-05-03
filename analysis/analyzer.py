import pandas as pd
from math import sqrt, log10
from datetime import datetime
from ftplib import *
import io
import numpy as np
import logging
import argparse
from functools import partial
from tqdm.contrib.concurrent import process_map
from multiprocessing import Manager

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s][%(levelname)s]: %(message)s ",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger(__name__)

####################
# Logan Lu's job:
# rewrite the functionality of this script
# to be able to write these spreadsheets
# to files in SMU Box instead of google
# sheets.
####################

###########################
# By Channel analysis (Each sheet is a channel, each column is a parameter)
# Steps (for each channel):
# 1. Find up to what date we have already analyzed
# 2. For every file equal to or older than this date
# 3. Download, analyze, and add to a new df
# 4. Resample this new df to the daily timeframe
# 5. Append to the existing, analyzed data
# 6. Upload to SMU Box again
###########################
# By Parameter analysis (Each sheet is a parameter, each column is a channel)
# 1. Take parameter from each channel that is already analyzed and append the row
#    to the exisiting file


ftp = FTP_TLS("ftp.box.com")
ftp.login(user="loganlu@mail.smu.edu", passwd="Loganlu20012016!!")
ftp.prot_p()
# -------------------------------------------------------------------------------------------------------------#
## get parameter function
main_path = "optical monitor raw data"
preproc_data_path = "Waveform_data1"
proc_data_path = "Analyzed_data"

# change to the directory so we can have shorter strings
ftp.cwd(main_path)


def get_parameter(filename):  # calculate waveform parameters

    df = pd.read_table(
        filename,
        delim_whitespace=True,
        index_col=0,
        names=["Wavelength", "OpticalPower"],
    )
    # print(df.head())

    x = df.index.to_list()
    y = df["OpticalPower"].to_list()
    y_new = [10 ** (i / 10 - 3) for i in y]

    mu = sum([a * b for a, b in zip(x, y_new)]) / sum(y_new)
    x0 = [i - mu for i in x]
    x0_2 = [i**2 for i in x0]
    sigma_2 = sum([a * b for a, b in zip(x0_2, y_new)]) / sum(y_new)
    sigma = sqrt(sigma_2)

    # print (mu,sigma)

    peak_wavelength = df["OpticalPower"].idxmax(axis=0)
    # print (peak_wavelength)

    peak_power = df["OpticalPower"].max()
    # print (peak_power)

    total_power = sum(y_new) * 0.015
    # print ('total power', total_power)

    total_dBm = 10 * log10(total_power * 1000)
    # print ('total dBm', total_dBm)
    return [mu, sigma, peak_wavelength, peak_power, total_power, total_dBm]


# -------------------------------------------------------------------------------------------------------------#
def get_date_time(filename):
    filename_list = (
        filename.replace("waveform_data_channel", "").replace(".txt", "").split("_")
    )
    # print (filename_list[1:])
    dt_str = "_".join(filename_list[-2:])
    return dt_str


# -------------------------------------------------------------------------------------------------------------#
def getLastAnalyzedDate(channel_num):
    # This should have two scenarios,
    # 1. There is no file, so the oldest date is something really far in the past
    # 2. You do have a file and you get the last row of file to retrieve
    #    the oldest date, assuming that the file is ordered by date
    # Now return this date
    oldest_date = datetime(1900, 1, 1)  # very old date
    fileName = (
        f"channel{channel_num}.csv"  # assuming the existing data is in CSV format
    )

    # Check if the file exists in the Box directory
    ftp.cwd(proc_data_path)
    logging.debug(f"Changed to {proc_data_path}")
    files = ftp.nlst()
    logging.debug(f"Files in {proc_data_path}: {files}")
    if fileName in files:
        logging.debug("found file in list of files")
        # Download the file
        memory_file = readFileFromBox(fileName)
        # Read the last row to get the latest date
        df = pd.read_csv(memory_file)
        memory_file.close()
        last_row = df.iloc[-1]
        last_date_str = last_row["datetime"]  # Assuming there is a 'Date' column
        oldest_date = datetime.strptime(last_date_str, "%Y-%m-%d")
    else:
        logging.debug("using default old date")
    logging.debug(f"Oldest date is {oldest_date}")
    return oldest_date


def getDaysToAnalyze(lastAnalyzedDate, channel_num) -> list:
    # This function will just return a list
    # it will just be a list of all of the files
    # on the current channel we are working on that
    # are older than the date that we found
    ftp.cwd(f"/{main_path}")
    logging.debug(f"in 'getFilesAfterDate' ftp is in {ftp.pwd()}")
    daysAvailableForProc = ftp.nlst(f"{preproc_data_path}/Channel_{channel_num}/*")
    # the first two entries in daysAvailableForProc are '.', '..' so we remove them
    daysAvailableForProc = daysAvailableForProc[2:]
    logging.debug(f"days available to be processed {daysAvailableForProc}")
    logging.debug(f"number of days available {len(daysAvailableForProc)}")

    daysAfterOldestDate = []
    for day in daysAvailableForProc:
        day_dt = datetime.strptime(day, "%Y-%m-%d")
        if day_dt > lastAnalyzedDate:
            daysAfterOldestDate.append(day)
    logging.debug(f"days after oldest date {daysAfterOldestDate}")
    logging.debug(f"number of days after oldest date {len(daysAfterOldestDate)}")

    return daysAfterOldestDate


def getFilesForDate(day, channel_num) -> list:
    # This function will just return a list
    # it will just be a list of all of the files
    # on the current channel we are working on that
    # are older than the date that we found

    filesForDate = []
    # get list of all of the files in this folder
    dayFolder = f"{preproc_data_path}/Channel_{channel_num}/{day}"
    logging.debug(f"Looking in folder {dayFolder}")
    fileNames = ftp.nlst(dayFolder)[2:]
    for f in fileNames:
        filesForDate.append(f"{dayFolder}/{f}")
    logging.debug(f"Have to process {len(filesForDate)} files for {day}")
    return filesForDate


def readFileFromBox(fileName):
    # This actually reads from box
    # Returns an in-memory file object that
    # can be used by pandas
    # memory_file = io.StringIO()
    # ftp.retrbinary(f"RETR {file}", memory_file.write)
    # memory_file.seek(0)
    # string_data = memory_file.getvalue().decode("utf-8")
    # return io.StringIO(string_data)

    memory_file = io.BytesIO()
    ftp.retrbinary(f"RETR {fileName}", memory_file.write)
    memory_file.seek(0)
    return memory_file


def readAndAnalyzeWorker(shared_list, file):

    ### TODO ADD COLUMN FOR DATETIME FROM THE FILE NAME
    dt_str = get_date_time(file)
    dt = datetime.strptime(dt_str, "%Y-%m-%d_%H-%M-%S")
    memory_file = readFileFromBox(file)
    if memory_file is not None:
        analyzed_data = get_parameter(memory_file)
        analyzed_data.insert(0, dt)
        shared_list.append(analyzed_data)


def analyzeFiles(fileNamesToBeProc) -> pd.DataFrame:
    # will take all the file names, read them in
    # analyze them and return back a dataframe
    # that will need to be transformed

    ######
    # HERE IS WHERE YOU CAN DO SOME MULTIPROCESSING
    # USE TQDM.PROCESS_MAP OR SOMETHING LIKE THAT
    #####
    # YOU WILL NEED TO WRAP A FUNCTION TO READ AND ANALYZE THE DATA
    ####
    # CODE SAMPLE:
    # https://stackoverflow.com/questions/67957266/python-tqdm-process-map-append-list-shared-between-processes

    manager = Manager()
    shared_list = manager.list()

    fileNamesToBeProc = fileNamesToBeProc[:5]
    process_map(
        partial(readAndAnalyzeWorker, shared_list), fileNamesToBeProc, max_workers=5
    )

    analyzed = pd.DataFrame(
        list(shared_list),
        columns=[
            "datetime",
            "mu",
            "sigma",
            "peak_wavelength",
            "peak_power",
            "total_power",
            "total_dBm",
        ],
    )
    analyzed.set_index("datetime", inplace=True)
    return analyzed


def resample(allAnalyzedDTs):
    # Take the dataframe with every data point
    # and resample it so we only have a data
    # point for every day
    return allAnalyzedDTs.resample("D").mean()


def combineWithExisting(sampleToDays, channel_num):
    # Take the existing analyzed data (if there is any in Box)
    # and append the new stuff to the old

    # check if there is a file in the analyzed data folder matching the channel number you are on
    # if so, read it in and append the new rows to it
    # return the new combined dataframe
    file_name = f"channel{channel_num}.csv"
    ftp.cwd(proc_data_path)
    files = ftp.nlst()
    if file_name in files:
        with readFileFromBox(file_name) as memory_file:
            existing_data = pd.read_csv(memory_file, index_col='datetime', parse_dates=True)
        combined = pd.concat([existing_data, sampleToDays]).sort_index()
        logging.info(f"Data combined with existing for channel {channel_num}")
    else:
        combined = sampleToDays
        logging.info(f"Starting new data file for channel {channel_num}")

    return combined

    pass


def uploadToBox(dataToBeUploaded):
    # take the final analyzed and joined data and send it
    # to the box directory

    # The below code sample is how you write to box
    # you will have to convert the dataframe to a list
    # each row of the dataframe is a new entry in the list called "lines"
    # data = io.BytesIO("\n".join(lines).encode())
    #    try:
    #        ftp.storlines(
    #            "STOR "
    #            + "Channel_"
    #            + str(mychannel1)
    #            + "/"
    #            + date
    #            + "/"
    #            + "waveform_data_channel%s_%s.txt"
    #            % (mychannel1, timestamp),
    #            fp=data,
    #        )
    #    except Exception as e:
    #        print("Failed box upload", e)

    # Want to return if the upload was successful or not

    pass


def main():

    for channel_num in range(1):

        lastAnalyzedDate = getLastAnalyzedDate(channel_num)
        daysToAnalyze = getDaysToAnalyze(lastAnalyzedDate, channel_num)
        for day in daysToAnalyze:
            fileNamesToBeProc = getFilesForDate(day, channel_num)
            analyzedDay = analyzeFiles(fileNamesToBeProc)
            print(analyzedDay.head())
            # resampling, combining, and uploading after every day to have progress in case
            # program fails, we won't have to run everything again
            sampleToDays = resample(analyzedDay)
            print(sampleToDays)
            dataToBeUploaded = combineWithExisting(sampleToDays, channel_num)
            # uploadStatus = uploadToBox(dataToBeUploaded)

    # print(f"Channel {channel_num} was {uploadStatus}")


if __name__ == "__main__":
    main()