__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'


def set_column_sequence(dataframe, seq):

    '''Takes a dataframe and a subsequence of its columns, returns dataframe with seq as first columns'''
    cols = seq[:] # copy so we don't mutate seq
    for x in dataframe.columns:
        if x not in cols:
            cols.append(x)
    return dataframe[cols]