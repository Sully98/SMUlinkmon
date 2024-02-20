# SMUlinkmon
Repository for monitoring the MTx and MTRx links fabricated in-part by SMU.

## Testbed setup
A collection of 64 optical fiber links consisting of MTx's (transmitter) and MTRx's (receiver).
The goal of the setup is to study the lifetime of the optical links. This is done by constantly 
passing a laser of known wavelength through the channels to simulate use inside the ATLAS detector. 
The state of each link is monitored by a spectrum analyzer. However, only 1 channel can be read 
at a time by the spectrum analyzer so there are 3 sets of "switches" to control which channel's 
light is passedto the spectrum analyzer. Two switches switch across 32 channels each then the third 
switch controls which switch is getting read from. So the spectrum analyzer will read the first 
32 channels sequentially, then third switch will switch to the second set of 32 channels so the 
spectrum analyzer can read the second 32 channnels.

## Requirements
The program works by connecting a laptop to the testbed setup then running a simple python script.
This python script controls which channel is being read by the spectrum analyzer, then reads the 
spectrum analyzer, then writes this information to disk or SMUBox.

To be able to control the switches, one must install the correct drivers. These can be found here
https://www.ni.com/en/support/downloads/drivers/download.ni-488-2.html#442610. Version 21.5. The 
python library pyserial must also be installed but this is done when `pip install -r requirements.txt`
is run.

To be able to read from the spectrum, the python library pyvisa is used. This is in the requirements.txt
file as well.

To write to SMUBox, a connection is established with ftplib and the username and generated password 
is passed to SMUBox for authentication. To be able to write to the folder, one must generate a 
password on SMUBox to allow access to the folder. 

## Code usage

To run the program, make sure you have python >3.8 installed on your machine and run 
`
pip install -r requirements.txt
python Optical_Switch_64CHBox.py
`
This will automatically start switching the channels, reading data, and writing it to SMUBox.
