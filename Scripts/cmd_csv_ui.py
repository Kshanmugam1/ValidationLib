#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Location Exposure CSV to UI Format
******************************************************

"""
# Import standard Python packages and read outfile
import sys

sys.path.insert(0, r'\\qafile2\TS\Working Data\Shashank\Validation Library\ValidationLib')
import datetime
import warnings

warnings.filterwarnings('ignore')

# Import standard Python packages and read outfile
import time
import logging

# Import internal packages
from ValidationLib.general.main import csv2ui

__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'

# Extract the given arguments
try:
    location_file = sys.argv[1]
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.INFO)

    HANDLER_INFO = logging.FileHandler(location_file[:-4] + '-info.log')
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

        csv2ui(location_file, location_file[:-4] + '_copy.csv')

    except:
        LOGGER.error('Unknown error: Contact code maintainer: ' + __maintainer__)
        sys.exit()
