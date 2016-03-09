#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Exposure Generator: Driver File driven
******************************************************

"""
# Import standard Python packages and read outfile
import getopt
import sys

sys.path.insert(0, r'\\qafile2\TS\Working Data\Shashank\Validation Library\ValidationLib')
import datetime
import warnings

warnings.filterwarnings('ignore')

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a
    print ("Outfile: " + OUTFILE)
OUTFILE = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\provisioning.csv'
if OUTFILE is None:
    print ('Outfile is not passed')
    sys.exit()

# Import standard Python packages and read outfile
import time
import logging
import pandas as pd


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

HANDLER_INFO = logging.FileHandler((OUTFILE[:-4] + '-info.log'))
HANDLER_INFO.setLevel(logging.INFO)
LOGGER.addHandler(HANDLER_INFO)

__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'

def file_skeleton(outfile):
    pd.DataFrame(columns=['Status']).to_csv(outfile, index=False)

# Extract the given arguments
try:

    driver_file = r'\\qafile2\TS\Working Data\MalloryHarper\CSV Import\Boundary\PWC_Reinsurance.csv'
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.INFO)

    HANDLER_INFO = logging.FileHandler(driver_file[:-4] + '-info.log')
    HANDLER_INFO.setLevel(logging.INFO)
    LOGGER.addHandler(HANDLER_INFO)
except:
    sys.exit()

if __name__ == "__main__":

    try:
        start = time.time()

        LOGGER.info('********************************')
        LOGGER.info('**      Touchstone v.3.0      **')
        LOGGER.info('********************************')

        LOGGER.info('\n********** Log header **********\n')
        LOGGER.info('Description:   Location CSV Exposure to UI Format')
        LOGGER.info('Time Submitted: ' + str(datetime.datetime.now()))
        LOGGER.info('Status:                Completed')
        data = pd.read_csv(driver_file, header=None).transpose()
        columns = data.iloc[0].values
        data = data[1:].reset_index(drop=True)
        data.columns = columns
        data = data.ffill()
        data.to_csv(driver_file[:-4] + '-Exposure.csv', index=False)

    except:
        LOGGER.error('Unknown error: Contact code maintainer: ' + __maintainer__, exc_info=True)
        file_skeleton(OUTFILE)
        sys.exit()