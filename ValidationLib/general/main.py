__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

import pandas as pd
import numpy as np
import threading
import uuid
import copy

def set_column_sequence(dataframe, seq):

    '''Takes a dataframe and a subsequence of its columns, returns dataframe with seq as first columns'''
    cols = seq[:] # copy so we don't mutate seq
    for x in dataframe.columns:
        if x not in cols:
            cols.append(x)
    return dataframe[cols]

def csv2ui(locationFile, outfile):

    input_file = pd.read_csv(locationFile, encoding='utf-16').fillna('-')
    columns = ['ContractID', 'LocationID', 'LocationName', 'LocationGroup', 'IsPrimary', 'ISOBIN', 'InceptionDate',
               'ExpirationDate', 'Street', 'City', 'SubArea2', 'SubArea', 'Area', 'CRESTA', 'PostalCode',
               'CountryISO', 'GeoMatchLevelCode', 'Latitude', 'Longitude']
    out_file = set_column_sequence(input_file, columns)
    out_file.iloc[:, 0] = out_file.iloc[:, 0] + '_Custom'
    out_file.iloc[:, :19].to_csv(outfile, index=False)

class UnicedeCompare:

    def __init__(self, sourceFile, testFile, resultFile):

        self.resultFile = resultFile
        self.lock = threading.RLock()
        files = [testFile]
        for i in files:
            with open(i,"r") as input:
                with open(i[:-4] + "-updated","w") as output:
                    output.write('AreaTag,PerilCode,LOBCode,Coverage,NumRisks,SumsIns,AvgDed,Prem,PremRate,SubareaName\n')
                    for line in input:
                        if '*' not in line:
                            line = "".join(line.split()) + '\n'
                            line = line.split(':')
                            if len(line) > 1:
                                line[1] = line[1].replace(',','-')
                            output.write(''.join(line))
            input.close()
            output.close()
        self.keys = ['AreaTag', 'PerilCode', 'LOBCode']
        self.columns = ['AreaTag', 'PerilCode', 'LOBCode', 'Coverage', 'NumRisks', 'SumsIns', 'AvgDed', 'Prem',
                        'PremRate', 'SubareaName']
        self.columns_CT_default = ['AreaTag', 'LOBCode', 'PerilCode', 'SubareaName', 'SumsIns', 'NumRisks',
                                   'Prem', 'PremRate', 'AvgDed']
        self.columns_CT = ['AreaTag', 'PerilCode', 'LOBCode','NumRisks', 'SumsIns', 'AvgDed', 'Prem',
                        'PremRate', 'SubareaName']


        mappedDF = self.map_files(sourceFile, testFile[:-4] +"-updated")
        testFileDF = mappedDF.reindex_axis(sorted(mappedDF.columns), axis=1)[0:].reset_index().drop('index', axis=1)

        testFileDF.loc[:, 'SumsIns'] = testFileDF.loc[:, 'SumsIns']
        try:

            testFileDF.loc[:, 'SumsIns'] = testFileDF.loc[:, 'SumsIns'].apply(np.round)
        except:
            testFileDF.loc[1:, 'SumsIns'] = testFileDF.loc[1:, 'SumsIns'].apply(np.round)

        sourceFileDF = pd.DataFrame(pd.read_csv(sourceFile, sep=',', header=0)).iloc[:, 8:17].fillna(0)
        sourceFileDF.columns = self.columns_CT_default
        sourceFileDF = set_column_sequence(sourceFileDF, self.columns_CT)

        self.compareDF(sourceFileDF, testFileDF)

    def map_files(self, sourceFile_updated, testFile_updated):
        print('---------------------------------------------')
        print(sourceFile_updated)

        sourceFileDF = pd.DataFrame(pd.read_csv(sourceFile_updated, sep=',', header=0)).iloc[:, 8:17].fillna(0)
        print('---------------------------------------------')
        sourceFileDF.columns = self.columns_CT_default
        sourceFileDF = set_column_sequence(sourceFileDF, self.columns_CT)

        testFileDF = pd.DataFrame(pd.read_csv(testFile_updated, sep=',', header=0)).fillna('-').drop('Coverage', axis=1)
        testFileDF_temp = copy.deepcopy(testFileDF)
        testFileDF_temp['Result_Found'] = ''

        sourceFileDF_updt = pd.DataFrame(columns=sourceFileDF.columns.values)
        testFileDF_updt = pd.DataFrame(columns=testFileDF.columns.values)

        count = 0
        for i in range(len(sourceFileDF)):
            test_index = testFileDF[self.keys][testFileDF[self.keys]
                                      == sourceFileDF[self.keys].iloc[i]].dropna().index.values
            if test_index.size == 0:

                line = pd.DataFrame(columns=sourceFileDF.columns.values, index=[count])
                line.loc[:, :] = sourceFileDF.loc[i, :].values
                sourceFileDF_updt = pd.concat([sourceFileDF_updt, line]).reset_index(drop=True)

                line = pd.DataFrame(columns=testFileDF_temp.columns.values, index=[count])
                line.loc[:, :] = '-'
                line.loc[:, 'Result_Found'] = 'Not in a test file'
                testFileDF_updt = pd.concat([testFileDF_updt, line]).reset_index(drop=True)

                count += 1
            else:

                for j in range(0, (test_index.size)):

                    testFileDF_temp.loc[test_index[j], 'Result_Found'] = 'Found'
                    line = pd.DataFrame(columns=testFileDF_temp.columns.values, index=[count])
                    line.loc[:, :] = testFileDF_temp.loc[test_index[j], :].values
                    testFileDF_updt = pd.concat([testFileDF_updt, line]).reset_index(drop=True)

                    if not j > 0:

                        line = pd.DataFrame(columns=sourceFileDF.columns.values, index=[count])
                        line.loc[:, :] = sourceFileDF.loc[i, :].values
                        sourceFileDF_updt = pd.concat([sourceFileDF_updt, line]).reset_index(drop=True)
                    else:
                        line = pd.DataFrame(columns=sourceFileDF.columns.values, index=[count])
                        line.loc[:, :] = '-'
                        sourceFileDF_updt = pd.concat([sourceFileDF_updt, line]).reset_index(drop=True)

                    count += 1

        testFileDF_updt = pd.concat([testFileDF_updt,
                                     testFileDF_temp.loc[testFileDF_temp.Result_Found=='', :]]).reset_index(drop=True)
        testFileDF_updt.loc[testFileDF_updt.Result_Found == '', 'Result_Found'] = 'Not in a Catrader file'
        testFileDF_updt = set_column_sequence(testFileDF_updt, self.columns_CT)
        testFileDF_updt['Filename'] = sourceFile_updated
        mappedDF = pd.concat(dict(Catrader=sourceFileDF_updt, TouchStone=testFileDF_updt), axis=1).fillna('-')

        self.lock.acquire(blocking=1)
        mappedDF.to_csv(self.resultFile + '/Log-Mapped/' + str(uuid.uuid4()) + '-Log-Mapping.csv', mode='wb')
        self.lock.release()

        return(testFileDF_updt.groupby(['AreaTag','PerilCode','LOBCode','SubareaName', 'Filename'],
                                       as_index=False).aggregate(np.sum).drop('Result_Found', axis=1))

    def compareDF(self, sourceFileDF, testFileDF):

        testFileDF_temp = copy.deepcopy(testFileDF)
        testFileDF_temp['Result_Aggregated'] = ''
        testFileDF_temp['MappingType'] = ''
        sourceFileDF_updt = pd.DataFrame(columns=sourceFileDF.columns.values)
        testFileDF_updt = pd.DataFrame(columns=testFileDF.columns.values)
        compare_columns = ['NumRisks', 'SumsIns', 'AvgDed', 'Prem', 'PremRate']

        for i in range(len(sourceFileDF)):

            test_index = testFileDF[self.keys][testFileDF[self.keys]
                                                    == sourceFileDF[self.keys].iloc[i]].dropna().index.values
            if test_index.size == 0:

                line = pd.DataFrame(columns=sourceFileDF.columns.values, index=[i])
                line.loc[:, :] = sourceFileDF.loc[i, :].values
                sourceFileDF_updt = pd.concat([sourceFileDF_updt, line]).reset_index(drop=True)

                line = pd.DataFrame(columns=testFileDF_temp.columns.values, index=[i])
                line.loc[:, :] = '-'
                line.loc[:, 'Result_Aggregated'] = 'Not in a test file'
                testFileDF_updt = pd.concat([testFileDF_updt, line]).reset_index(drop=True)

            else:

                line = pd.DataFrame(columns=sourceFileDF.columns.values, index=[i])
                line.loc[:, :] = sourceFileDF.loc[i, :].values
                sourceFileDF_updt = pd.concat([sourceFileDF_updt, line]).reset_index(drop=True)
                try:
                    if ((testFileDF.loc[test_index, compare_columns].values.astype(np.float).flatten() - sourceFileDF.loc[i, compare_columns].values.astype(np.float).flatten()) <0.02).all():
                        testFileDF_temp.loc[test_index, 'Result_Aggregated'] = 'Values Match'
                    else:
                        testFileDF_temp.loc[test_index, 'Result_Aggregated'] = 'Values Mismatch'
                except:
                    testFileDF_temp.loc[test_index, 'Result_Aggregated'] = 'Values Mismatch'

                if (testFileDF_temp.loc[test_index, 'NumRisks'] > 1).all():

                    testFileDF_temp.loc[test_index, 'MappingType'] = 'One to Many'
                else:
                    testFileDF_temp.loc[test_index, 'MappingType'] = 'One to One'

                line = pd.DataFrame(columns=testFileDF_temp.columns.values, index=[test_index])
                line.loc[:, :] = testFileDF_temp.loc[test_index, :].values
                testFileDF_updt = pd.concat([testFileDF_updt, line]).reset_index(drop=True)

        testFileDF_updt = pd.concat([testFileDF_updt, testFileDF_temp.loc[testFileDF_temp.Result_Aggregated=='',
                                                      :]]).reset_index(drop=True)
        testFileDF_updt.loc[testFileDF_updt.Result_Aggregated == '', 'Result_Aggregated'] = 'Not in a Source file'
        testFileDF_updt.loc[testFileDF_updt.MappingType == '', 'MappingType'] = '-'
        testFileDF_updt = set_column_sequence(testFileDF_updt, self.columns_CT)

        FinalDF = pd.concat(dict(Source=sourceFileDF_updt, Test=testFileDF_updt), axis=1).fillna('-')
        self.lock.acquire(blocking=1)
        FinalDF.to_csv(self.resultFile + '/Log-Aggregated/' + str(uuid.uuid4()) + '-Log-Aggregated.csv', encoding='utf-8', mode='wb')
        self.lock.release()