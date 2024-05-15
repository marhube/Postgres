import os
import sys
#**********
import pandas as pd
import numpy as np
import datetime as dt
from math import floor
#********

#Forsøk på  systematisk testing
from Datatype import Datatype
from TestData.TestData import generate_test_data
import Postgres.PrepData as PrepData
from Postgres.PrepData import PreprocessData



import unittest



class TestPrepData(unittest.TestCase):       
    
    def test_sql_insert_into(self):
        print('Er nå inne i TestPrepData.test_sql_insert_into som ikke er ferdig implementert')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        test_data['date_only_na'] = test_data['datetime_only_na']
        data_types = Datatype.robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        str_insert_into = preprocess.sql_insert_into()
        print("str_insert_into er")
        print(str_insert_into)
      
    # Tester kun at konvererter til datetime.date
    def test_preprocess_data(self):
        print('Er nå inne i TestPrepData.test_preprocess_data')
        table_name = "my_table"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True,include_str_date = True)
        test_data['date_datetime'] = test_data['datetime_with_na']
        #
        data_types = Datatype.robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('str_date_with_na')
        date_cols.append('str_datetime_with_na')
        date_cols.append('date_only_na')
        date_cols.append('date_datetime')
        timestamp_cols = ['str_datetime_with_na']
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols,timestamp_cols = timestamp_cols) 
        data_to_insert = preprocess.preprocess_data(test_data)

        date_col_indices = []
        #Memo to self: The last column is a bit special
        comparison_data = test_data.copy()
        comparison_data['date_datetime'] = comparison_data['date_datetime'].map(lambda x: x.date())
        comparison_data['str_date_with_na'] = comparison_data['str_date_with_na'].map(lambda x: x[0:10] if not pd.isna(x) else None)        
        comparison_data['str_datetime_with_na'] = comparison_data['str_datetime_with_na'].map(lambda x: x[0:10] if not pd.isna(x) else None)
        #
        for enum,col in enumerate(comparison_data.columns):
            if col in date_cols:
                date_col_indices.append(enum)
        #
        subresults = [] 
        #Memo to self: The case where original data is
        for row,rowdata in enumerate(data_to_insert):           
            for ind in date_col_indices:           
                subresult = (
                    (rowdata[ind] is None and pd.isna(comparison_data.iloc[row,ind])) or 
                    (isinstance(rowdata[ind],str) and 
                     rowdata[ind] == str(comparison_data.iloc[row,ind])
                    )                     
                )
                             
                subresults.append(subresult) 
     
        #        
               
        self.assertTrue(pd.Series(subresults).all()) 