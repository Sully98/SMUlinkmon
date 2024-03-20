#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import serial
import datetime
import pyvisa as visa
from ftplib import *
import io

# ========================================================================================#
## plot parameters
lw_grid = 0.5  # grid linewidth
fig_dpi = 800  # save figure's resolution
# ========================================================================================#
freqency = 1000
duration = 1000
"""
@author: Lily Zhang
@date: 2022-05-02
This script is used to control optical switch and optical spectrum analyzer (Model: ANDO AQ6317)
"""


# ========================================================================================#
## dBm to mW conversion
def dBm_to_mW(x):
    return 1 * (10 ** (x / 10.0))


# ========================================================================================#
## square function
def square(x):
    return x**2


# ========================================================================================#
def main():
    timeslot = 600
    command_list1 = [
        b"<OSW01_OUT_01>",
        b"<OSW01_OUT_02>",
        b"<OSW01_OUT_03>",
        b"<OSW01_OUT_04>",
        b"<OSW01_OUT_05>",
        b"<OSW01_OUT_06>",
        b"<OSW01_OUT_07>",
        b"<OSW01_OUT_08>",
        b"<OSW01_OUT_09>",
        b"<OSW01_OUT_10>",
        b"<OSW01_OUT_11>",
        b"<OSW01_OUT_12>",
        b"<OSW01_OUT_13>",
        b"<OSW01_OUT_14>",
        b"<OSW01_OUT_15>",
        b"<OSW01_OUT_16>",
        b"<OSW01_OUT_17>",
        b"<OSW01_OUT_18>",
        b"<OSW01_OUT_19>",
        b"<OSW01_OUT_20>",
        b"<OSW01_OUT_21>",
        b"<OSW01_OUT_22>",
        b"<OSW01_OUT_23>",
        b"<OSW01_OUT_24>",
        b"<OSW01_OUT_25>",
        b"<OSW01_OUT_26>",
        b"<OSW01_OUT_27>",
        b"<OSW01_OUT_28>",
        b"<OSW01_OUT_29>",
        b"<OSW01_OUT_30>",
        b"<OSW01_OUT_31>",
        b"<OSW01_OUT_32>",
    ]
    command_list2 = [
        b"<OSW01_OUT_01>",
        b"<OSW01_OUT_02>",
        b"<OSW01_OUT_03>",
        b"<OSW01_OUT_04>",
        b"<OSW01_OUT_05>",
        b"<OSW01_OUT_06>",
        b"<OSW01_OUT_07>",
        b"<OSW01_OUT_08>",
        b"<OSW01_OUT_09>",
        b"<OSW01_OUT_10>",
        b"<OSW01_OUT_11>",
        b"<OSW01_OUT_12>",
        b"<OSW01_OUT_13>",
        b"<OSW01_OUT_14>",
        b"<OSW01_OUT_15>",
        b"<OSW01_OUT_16>",
        b"<OSW01_OUT_17>",
        b"<OSW01_OUT_18>",
        b"<OSW01_OUT_19>",
        b"<OSW01_OUT_20>",
        b"<OSW01_OUT_21>",
        b"<OSW01_OUT_22>",
        b"<OSW01_OUT_23>",
        b"<OSW01_OUT_24>",
        b"<OSW01_OUT_25>",
        b"<OSW01_OUT_26>",
        b"<OSW01_OUT_27>",
        b"<OSW01_OUT_28>",
        b"<OSW01_OUT_29>",
        b"<OSW01_OUT_30>",
        b"<OSW01_OUT_31>",
        b"<OSW01_OUT_32>",
    ]
    command_list3 = [b"<OSW01_OUT_01>", b"<OSW01_OUT_02>"]

    # ser1 = serial.Serial('COM4')
    # print("Serial port: %s"%ser1.name)
    ser1 = serial.Serial("COM3")
    print("Serial port: %s" % ser1.name)
    ser2 = serial.Serial("COM4")
    print("Serial port: %s" % ser2.name)
    ser3 = serial.Serial("COM5")
    print("Serial port: %s" % ser3.name)

    rm = visa.ResourceManager()
    print(rm.list_resources())
    inst1 = rm.open_resource("GPIB0::5::INSTR")  # GPIB address
    time.sleep(1)
    print(inst1.query("*IDN?"))  # query Instrument ID

    inst1.write("INIT")
    time.sleep(3)
    inst1.write("STAWL835.00")  # set start waveform length = 840 nm
    inst1.write("HD0")
    inst1.write("LSCL10.0")  # y-axis scale 10 dB/D
    inst1.write("STPWL865.00")  # set stop waveform length = 870 nm
    inst1.write("RESLN0.2")  # set x-axis resolution 0.05 nm
    inst1.write("SMPL2000")  # set sample point

    lasttime = datetime.datetime.now()
    while True:

        try:
            # Going to put the ftp connection in the while loop to ensure it will always reconnect
            # sometimes connections can timeout, so persistent reconnection can prevent this
            ftp = FTP_TLS("ftp.box.com")
            ftp.login(user="sbillingsley@smu.edu", passwd="Pbex5fFsbfzYEH9!!")
            ftp.prot_p()
            ftp.cwd("optical monitor raw data/Waveform_data1")

            for sw3_index in range(2):
                ser3.write(
                    command_list3[sw3_index]
                )  # psuedocode. follow format. look at optical_switch_controller.py original code
                print("MUX2TO1", sw3_index)
                # sw1_index = 31
                # for sw2_index in range(32):
                if sw3_index == 0:
                    for sw1_index in range(32):
                        try:
                            ser1.write(command_list1[sw1_index])
                            print("CH1_Switch", sw1_index)
                            # s = ser1.read(14)
                            time.sleep(3)

                            inst1.write("SGL")  # start a single sweep
                            time.sleep(6)

                            xdata = []
                            ydata = []
                            inst1.write("WDATAR1-R2000")  # acquire x-axis data
                            xdata = inst1.query("")[:-2].split(",")[1:]

                            inst1.write("LDATAR1-R2000")  # acquire y-axis data
                            ydata = inst1.query("")[:-2].split(",")[1:]

                            wavelength = []
                            yydata = []

                            timestamp = time.strftime(
                                "%Y-%m-%d_%H-%M-%S", time.localtime(time.time())
                            )
                            date = timestamp.split("_")[0]
                            print(timestamp)

                            lines = []
                            mychannel1 = 32 * sw3_index + sw1_index

                            days_in_channel = ftp.nlst("Channel_" + str(mychannel1))

                            if date not in days_in_channel:
                                ftp.mkd("Channel_" + str(mychannel1) + "/" + date)
                                print("made directory " + date)

                            # with open("C:/Optical Monitor/Waveform_data/waveform_data_channel%s_%s.txt"%(mychannel1, timestamp), 'w') as infile:
                            for i in range(len(xdata)):
                                wavelength += [float(xdata[i])]
                                yydata += [float(ydata[i])]
                                # infile.write('%f %f\n'%(float(xdata[i]), float(ydata[i])))
                                lines.append(
                                    "%f %f\n" % (float(xdata[i]), float(ydata[i]))
                                )

                            # Writing to box through FTPS protocol
                            data = io.BytesIO("\n".join(lines).encode())
                            try:
                                ftp.storlines(
                                    "STOR "
                                    + "Channel_"
                                    + str(mychannel1)
                                    + "/"
                                    + date
                                    + "/"
                                    + "waveform_data_channel%s_%s.txt"
                                    % (mychannel1, timestamp),
                                    fp=data,
                                )
                            except Exception as e:
                                print("Failed box upload", e)
                        except Exception as e:
                            print("Failed second first big loop", e)
                elif sw3_index == 1:
                    # ser2.write[command_list1[j]]
                    for sw2_index in range(32):
                        try:
                            ser2.write(command_list2[sw2_index])
                            print("CH2_Switch", sw2_index)
                            # s = ser2.read(14)
                            time.sleep(3)

                            inst1.write("SGL")  # start a single sweep
                            time.sleep(6)

                            xdata2 = []
                            ydata2 = []
                            inst1.write("WDATAR1-R2000")  # acquire x-axis data
                            xdata2 = inst1.query("")[:-2].split(",")[1:]

                            inst1.write("LDATAR1-R2000")  # acquire y-axis data
                            ydata2 = inst1.query("")[:-2].split(",")[1:]

                            wavelength2 = []
                            yydata2 = []
                            timestamp2 = time.strftime(
                                "%Y-%m-%d_%H-%M-%S", time.localtime(time.time())
                            )
                            date2 = timestamp2.split("_")[0]
                            print(timestamp2)

                            lines = []
                            mychannel2 = 31 * sw3_index + 1 * sw2_index + 1

                            days_in_channel2 = ftp.nlst("Channel_" + str(mychannel2))

                            if date2 not in days_in_channel2:
                                ftp.mkd("Channel_" + str(mychannel2) + "/" + date2)
                                print("made directory " + date)

                            # with open("C:/Optical Monitor/Waveform_data/waveform_data_channel%s_%s.txt"%(mychannel2, timestamp2), 'w') as infile:
                            for i in range(len(xdata2)):
                                wavelength2 += [float(xdata2[i])]
                                yydata2 += [float(ydata2[i])]
                                # infile.write('%f %f\n'%(float(xdata2[i]), float(ydata2[i])))
                                lines.append(
                                    "%f %f\n" % (float(xdata2[i]), float(ydata2[i]))
                                )

                            # Writing to box through FTPS protocol
                            data = io.BytesIO("\n".join(lines).encode())
                            try:
                                ftp.storlines(
                                    "STOR "
                                    + "Channel_"
                                    + str(mychannel2)
                                    + "/"
                                    + date2
                                    + "/"
                                    + "waveform_data_channel%s_%s.txt"
                                    % (mychannel2, timestamp2),
                                    fp=data,
                                )
                            except Exception as e:
                                print("Failed box upload", e)
                        except Exception as e:
                            print("Failed second second big loop", e)
            ftp.close()
            print("Cycle OK")
        except Exception as e:
            print("big loop failed", e)


# ========================================================================================#
if __name__ == "__main__":
    main()
