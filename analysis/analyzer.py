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
def main():

    box_files = ftp.nlst(preproc_data_path)
    # the first 2 files are '.' and '..' so we skip those
    box_files = box_files[2:]

    # order file by time in name of file
    # file_list = sorted(box_files,  key=lambda x: int(datetime.strptime(x.split(".")[0].split("_")[-2]+"_"+x.split(".")[0].split("_")[-1], '%Y-%m-%d_%H-%M-%S').strftime("%s")))

    for i in range(64):
        print("Channel ", i)
        raw = []
        days = ftp.nlst(preproc_data_path + "/Channel_" + str(i))

        sheet_exists = True
        sheet_exists_but_empty = False
        try:
            wks = sh.worksheet_by_title("Channel " + str(i))
            existing = wks.get_as_df(
                has_header=True,
                include_tailing_empty=False,
                include_tailing_empty_rows=False,
            )
            if existing.empty:
                sheet_exists_but_empty = True
        except pygsheets.exceptions.WorksheetNotFound:
            sheet_exists = False
            print("no worksheet ")

        oldest_date = datetime(1970, 1, 1)
        if sheet_exists and not sheet_exists_but_empty:
            existing["datetime"] = pd.to_datetime(existing["datetime"])
            dt_list = list(existing["datetime"])
            dt_list = [i.to_pydatetime() for i in dt_list]
            oldest_date = max(dt_list)
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
        try:
            try:
                if sheet_exists and not sheet_exists_but_empty:
                    existing.set_index("datetime", inplace=True)
                    print(new_df)
                    df_to_upload = pd.concat([existing, new_df])
                    print("deleting wks")
                    sh.del_worksheet(wks)
                else:
                    df_to_upload = new_df.copy()
                if sheet_exists_but_empty:
                    df_to_upload = new_df.copy()
                    print("deleting wks")
                    sh.del_worksheet(wks)
            except Exception as e:
                print(e)
                print("Couldn't delete worksheet")
            print(df_to_upload)
            df_to_upload.sort_index(inplace=True)
            # drop the last resampled day so its not reconsidered for the mean
            df_to_upload = df_to_upload.iloc[:-1, :]
            df_to_upload.replace("", np.nan, inplace=True)
            # df_to_upload.reset_index(inplace=True)
            # print(df_to_upload)
            final_df = df_to_upload.resample("D").mean()
            final_df.reset_index(inplace=True)
            # final_df['datetime'] = final_df['datetime'].dt.strftime("%m/%d/%Y %H:%M:%S")
            print(final_df)
            final_df.fillna("", inplace=True)
            sh.add_worksheet("Channel " + str(i), rows=100, cols=7, index=i)
            wks = sh.worksheet_by_title("Channel " + str(i))
            wks.set_dataframe(final_df, "A1")
        except Exception as e:
            print(e)
            print("failed trying to delete and set worksheet")
    # Get all of mu and rename the column based on the channel num
    mu_df = pd.DataFrame()
    sig_df = pd.DataFrame()
    peak_wave_df = pd.DataFrame()
    peak_pow_df = pd.DataFrame()
    tot_pow_df = pd.DataFrame()
    tot_dbm_df = pd.DataFrame()
    for i in range(64):
        try:
            wks = sh.worksheet_by_title("Channel " + str(i))
            chan_df = wks.get_as_df(
                has_header=True,
                include_tailing_empty=False,
                include_tailing_empty_rows=False,
            )
            if len(chan_df) > 0:
                chan_df["datetime"] = pd.to_datetime(chan_df["datetime"])
                chan_df.set_index("datetime", inplace=True)

            # mu
            chan_mu = chan_df.drop(
                ["sigma", "peak_wavelength", "peak_power", "total_power", "total_dBm"],
                axis=1,
            )
            chan_mu.columns = ["mu" + str(i)]
            if len(mu_df) > 0:
                mu_df.set_index("datetime", inplace=True)
            mu_df = pd.concat([mu_df, chan_mu], axis=1)
            mu_df.sort_index(inplace=True)
            mu_df.reset_index(inplace=True)
            mu_df.fillna("", inplace=True)
            try:
                mu_wks = sh.worksheet_by_title("mu")
                sh.del_worksheet(mu_wks)
                sh.add_worksheet("mu")
            except pygsheets.exceptions.WorksheetNotFound:
                sh.add_worksheet("mu")
            mu_wks = sh.worksheet_by_title("mu")
            mu_wks.set_dataframe(mu_df, "A1")

            # sig
            chan_sig = chan_df.drop(
                ["mu", "peak_wavelength", "peak_power", "total_power", "total_dBm"],
                axis=1,
            )
            chan_sig.columns = ["sig" + str(i)]
            if len(sig_df) > 0:
                sig_df.set_index("datetime", inplace=True)
            sig_df = pd.concat([sig_df, chan_sig], axis=1)
            sig_df.sort_index(inplace=True)
            sig_df.reset_index(inplace=True)
            sig_df.fillna("", inplace=True)
            try:
                sig_wks = sh.worksheet_by_title("sigma")
                sh.del_worksheet(sig_wks)
                sh.add_worksheet("sigma")
            except pygsheets.exceptions.WorksheetNotFound:
                sh.add_worksheet("sigma")
            sig_wks = sh.worksheet_by_title("sigma")
            sig_wks.set_dataframe(sig_df, "A1")

            # peak wavelength
            chan_wave = chan_df.drop(
                ["mu", "sigma", "peak_power", "total_power", "total_dBm"], axis=1
            )
            chan_wave.columns = ["peak_wave" + str(i)]
            if len(peak_wave_df) > 0:
                peak_wave_df.set_index("datetime", inplace=True)
            peak_wave_df = pd.concat([peak_wave_df, chan_wave], axis=1)
            peak_wave_df.sort_index(inplace=True)
            peak_wave_df.reset_index(inplace=True)
            peak_wave_df.fillna("", inplace=True)
            try:
                peak_wave_wks = sh.worksheet_by_title("peak_wave")
                sh.del_worksheet(peak_wave_wks)
                sh.add_worksheet("peak_wave")
            except pygsheets.exceptions.WorksheetNotFound:
                sh.add_worksheet("peak_wave")
            peak_wave_wks = sh.worksheet_by_title("peak_wave")
            peak_wave_wks.set_dataframe(peak_wave_df, "A1")

            # peak power
            chan_peak_pow = chan_df.drop(
                ["mu", "sigma", "peak_wavelength", "total_power", "total_dBm"], axis=1
            )
            chan_peak_pow.columns = ["peak_power" + str(i)]
            if len(peak_pow_df) > 0:
                peak_pow_df.set_index("datetime", inplace=True)
            peak_pow_df = pd.concat([peak_pow_df, chan_peak_pow], axis=1)
            peak_pow_df.sort_index(inplace=True)
            peak_pow_df.reset_index(inplace=True)
            peak_pow_df.fillna("", inplace=True)
            try:
                peak_pow_wks = sh.worksheet_by_title("peak_power")
                sh.del_worksheet(peak_pow_wks)
                sh.add_worksheet("peak_power")
            except pygsheets.exceptions.WorksheetNotFound:
                sh.add_worksheet("peak_power")
            peak_pow_wks = sh.worksheet_by_title("peak_power")
            peak_pow_wks.set_dataframe(peak_pow_df, "A1")

            # total power
            chan_tot_pow = chan_df.drop(
                ["mu", "sigma", "peak_power", "peak_wavelength", "total_dBm"], axis=1
            )
            chan_tot_pow.columns = ["tot_power" + str(i)]
            if len(tot_pow_df) > 0:
                tot_pow_df.set_index("datetime", inplace=True)
            tot_pow_df = pd.concat([tot_pow_df, chan_tot_pow], axis=1)
            tot_pow_df.sort_index(inplace=True)
            tot_pow_df.reset_index(inplace=True)
            tot_pow_df.fillna("", inplace=True)
            try:
                tot_pow_wks = sh.worksheet_by_title("total_power")
                sh.del_worksheet(tot_pow_wks)
                sh.add_worksheet("total_power")
            except pygsheets.exceptions.WorksheetNotFound:
                sh.add_worksheet("total_power")
            tot_pow_wks = sh.worksheet_by_title("total_power")
            tot_pow_wks.set_dataframe(tot_pow_df, "A1")

            # total dBm
            chan_tot_dbm = chan_df.drop(
                ["mu", "sigma", "peak_power", "total_power", "peak_wavelength"], axis=1
            )
            chan_tot_dbm.columns = ["tot_dbm" + str(i)]
            if len(tot_dbm_df) > 0:
                tot_dbm_df.set_index("datetime", inplace=True)
            tot_dbm_df = pd.concat([tot_dbm_df, chan_tot_dbm], axis=1)
            tot_dbm_df.sort_index(inplace=True)
            tot_dbm_df.reset_index(inplace=True)
            tot_dbm_df.fillna("", inplace=True)
            try:
                tot_dbm_wks = sh.worksheet_by_title("total_dbm")
                sh.del_worksheet(tot_dbm_wks)
                sh.add_worksheet("total_dbm")
            except pygsheets.exceptions.WorksheetNotFound:
                sh.add_worksheet("total_dbm")
            tot_dbm_wks = sh.worksheet_by_title("total_dbm")
            tot_dbm_wks.set_dataframe(tot_dbm_df, "A1")

        except pygsheets.exceptions.WorksheetNotFound:
            print("no worksheet for Channel " + str(i))


# -------------------------------------------------------------------------------------------------------------#
if __name__ == "__main__":
    main()
