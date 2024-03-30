import pandas as pd
from math import sqrt, log10
from datetime import datetime
from ftplib import *
import io
import numpy as np

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
ftp.login(user="sbillingsley@smu.edu", passwd="Pbex5fFsbfzYEH9!!")
ftp.prot_p()
# -------------------------------------------------------------------------------------------------------------#
## get parameter function
main_path = "optical monitor raw data"
preproc_data_path = "Waveform_data1"
proc_data_path = "Processed_data/Waveform_parameter_"

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
    return filename_list[1:]


# -------------------------------------------------------------------------------------------------------------#
def getLastAnalyzedDate(channel_num):
    # This should have two scenarios,
    # 1. There is no file, so the oldest date is something really far in the past
    # 2. You do have a file and you get the last row of file to retrieve
    #    the oldest date, assuming that the file is ordered by date
    # Now return this date
    pass


def getFilesAfterDate(lastAnalyzedDate) -> list:
    # This function will just return a list ^
    # it will just be a list of all of the files
    # on the current channel we are working on that
    # are older than the date that we found
    return []


def readFileFromBox(file):
    # This actually reads from box
    # Returns an in-memory file object that
    # can be used by pandas
    return io.StringIO


def analyzeFiles(fileNamesToBeProc) -> pd.DataFrame:
    # will take all the file names, read them in
    # analyze them and return back a dataframe
    # that will need to be transformed
    return pd.DataFrame()


def resample(allAnalyzedDTs):
    # Take the dataframe with every data point
    # and resample it so we only have a data
    # point for every day
    pass


def combineWithExisting(sampleToDays):
    # Take the existing analyzed data (if there is any in Box)
    # and append the new stuff to the old
    pass


def uploadToBox(dataToBeUploaded):
    # take the final analyzed and joined data and send it
    # to the box directory
    pass


def main():

    for channel_num in range(64):

        lastAnalyzedDate = getLastAnalyzedDate(channel_num)
        fileNamesToBeProc = getFilesAfterDate(lastAnalyzedDate)
        allAnalyzedDTs = analyzeFiles(fileNamesToBeProc)
        sampleToDays = resample(allAnalyzedDTs)
        dataToBeUploaded = combineWithExisting(sampleToDays)
        uploadStatus = uploadToBox(dataToBeUploaded)

        print(f"Channel {channel_num} was {uploadStatus}")


if __name__ == "__main__":
    main()
