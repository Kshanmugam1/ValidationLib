import unittest
from financials.Correlation.main import *

__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'


class TestCorrValidation(unittest.TestCase):

    validation = CorrValidation('QAWUDB2\SQL2012')

    def test_negative_analysis_sid(self):

        self.assertRaises(Exception, self.validation._get_correlation_factor, -1)


if __name__ == '__main__':
    unittest.main()