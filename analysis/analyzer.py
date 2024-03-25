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
ftp.login(user="loganlu@mail.smu.edu", passwd="Loganlu20012016!!")
ftp.prot_p()
# -------------------------------------------------------------------------------------------------------------#
## get parameter function
main_path = "Processed_data"
preproc_data_path = "Waveform_parameter_20220705"
proc_data_path = "Processed_data/Waveform_parameter_20220705"

# change to the directory so we can have shorter strings
ftp.cwd(main_path)
ftp.cwd(preproc_data_path)


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
def main():

    box_files = ftp.nlst(preproc_data_path)
    # the first 2 files are '.' and '..' so we skip those
    box_files = box_files[2:]
    
    get_parameter("Channel35_out_parameter.txt")

    # order file by time in name of file
    # file_list = sorted(box_files,  key=lambda x: int(datetime.strptime(x.split(".")[0].split("_")[-2]+"_"+x.split(".")[0].split("_")[-1], '%Y-%m-%d_%H-%M-%S').strftime("%s")))

    for i in range(64):
        print("Channel ", i)
        raw = []
        days = ftp.nlst(preproc_data_path + "/Channel_" + str(i))

        oldest_date = datetime(1970, 1, 1)

        try:
            for day in days[2:]:
                for j in ftp.nlst(preproc_data_path + "/Channel_" + str(i) + "/" + day)[
                    2:
                ]:
                    raw.append(
                        preproc_data_path + "/Channel_" + str(i) + "/" + day + "/" + j
                    )
                    # print(preproc_data_path+"/Channel_"+str(i)+"/"+day+"/"+j)
        except Exception as e:
            print(e)

        lines_for_new_df = [
            [
                "datetime",
                "mu",
                "sigma",
                "peak_wavelength",
                "peak_power",
                "total_power",
                "total_dBm",
            ]
        ]

        file_num = 0
        for file in raw:

            # each file in this list gets written to one line in the channel(i) out parameter text file
            dt = datetime.strptime(
                file.split(".")[0].split("_")[-2]
                + "_"
                + file.split(".")[0].split("_")[-1],
                "%Y-%m-%d_%H-%M-%S",
            )
            if dt.date() >= oldest_date.date():
                file_num += 1
        print("files to analyze ", file_num)
        file_num = 0
        for file in raw:

            # each file in this list gets written to one line in the channel(i) out parameter text file
            dt = datetime.strptime(
                file.split(".")[0].split("_")[-2]
                + "_"
                + file.split(".")[0].split("_")[-1],
                "%Y-%m-%d_%H-%M-%S",
            )
            if dt.date() >= oldest_date.date():
                file_num += 1
                # print(file_num,len(raw))

                date_str = dt.date().strftime("%m/%d/%Y")
                time_str = dt.time().strftime(" %H:%M:%S")
                if file.endswith(".txt"):
                    try:
                        data_to_work_on = []
                        ftp.retrlines("RETR " + file, callback=data_to_work_on.append)

                        fake_file = io.StringIO("\n".join(data_to_work_on))
                        par_list = get_parameter(
                            fake_file
                        )  # return back calculation parameters
                        # print(par_list)

                        date_time_list = [date_str + time_str]  # return back file date
                        # print(date_time_list)
                        full_line = date_time_list + par_list
                        lines_for_new_df.append(full_line)
                    except:
                        print("bad file" + file)
        new_df = pd.DataFrame(lines_for_new_df)
        new_df.columns = new_df.iloc[0]
        new_df.drop(new_df.index[0], inplace=True)
        new_df["datetime"] = pd.to_datetime(
            new_df["datetime"], format="%m/%d/%Y %H:%M:%S"
        )
        new_df.set_index("datetime", inplace=True)


if __name__ == "__main__":
    main()
