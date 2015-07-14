__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

# Import external Python libraries
import pandas as pd

def _saveDFCsv(Df, resultPath, filename):

    Df.to_csv(resultPath + '/' + filename + '.csv', index=False)