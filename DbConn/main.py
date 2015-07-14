__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

# Import standard Python packages
import copy

# Import external Python libraries
import pyodbc
import copy

class dbConnection:

    def __init__(self, server):

        # Initializing the connection and cursor
        self.connection = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server)
        self.cursor = self.connection.cursor()

    def _getResultSID(self, AnalysisSID):

        self.cursor.execute('SELECT ResultSID FROM AIRProject.dbo.tAnalysis '
                            'WHERE AnalysisSID = ' + str(AnalysisSID))
        info = copy.deepcopy(self.cursor.fetchall())
        resultSID = info[0][0]

        return resultSID