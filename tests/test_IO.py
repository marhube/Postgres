import os
import sys
#**********
import pandas as pd
import numpy as np
import datetime as dt
from math import floor
#********

#Forsøk på  systematisk testing


#********* Start internimport
from TestData.TestData import generate_test_data

from Datatype.Datatype import find_date_columns,get_data_type_from_values
from Postgres.PrepData import PreprocessData
from Postgres.Postgres_Connect import Connect
from Postgres.Execute import drop_user_table_if_exists,Exec,TableOps
from Postgres.IO import ImportData,ExportData

#********* Slutt internimport

import unittest

def value_comparison(df: pd.DataFrame,df_comparison: pd.DataFrame) -> dict:
    subresults_dict = {}
    for col in df_comparison:
        single_value_comparisons = []
        for enum,value in enumerate(df_comparison[col].tolist()):
            single_value_comparison = False
            if (pd.isna(value) and pd.isna(df[col][enum]) ) or (df[col][enum] == value):
                single_value_comparison = True
        #
            single_value_comparisons.append(single_value_comparison)
        #   
        subresults_dict[col] = single_value_comparisons
    #
    
    return subresults_dict
        

def compare_data_types(df: pd.DataFrame,df_comparison: pd.DataFrame) -> dict:
    subresults_dict = {}
    for col in df.columns:
        if len(df_comparison.dropna()) > 0:
            subresults_dict[col] = (get_data_type_from_values(df[col]) == get_data_type_from_values(df_comparison[col]))
        else:
            subresults_dict[col] =  (str(df.dtypes[col]) == str(df_comparison.dtypes[col]))
    #    
    return subresults_dict

def compare_db_data_types(data_types_dict: dict,data_types_comparison_dict: dict):
    subresults_dict = {}
    equivalence_class_float = ['DOUBLE PRECISION','NUMERIC']

    for col in data_types_dict:
        if data_types_comparison_dict[col].upper() in equivalence_class_float:
            subresults_dict[col] = (data_types_dict[col].upper() in equivalence_class_float)
        elif data_types_comparison_dict[col].upper().startswith('TIMESTAMP'):
            subresults_dict[col] = (data_types_dict[col].upper().startswith('TIMESTAMP'))
        else:
            subresults_dict[col] = (data_types_dict[col].upper() == data_types_comparison_dict[col].upper())
        #
    #    
    return subresults_dict
        



class TestIO(unittest.TestCase):
        
    #Memo til selv: Bør utvide test til også å sjekke datatyper
    def test_create_user_table(self):
        print('Er nå inne i TestIO.test_create_user_table')
        #table_name = "ORA$PTT_MY_TABLE"
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        date_cols = find_date_columns(test_data)
        date_cols.append('date_only_na')
        conn = Connect().get_connection()
        exec = Exec(conn)
        drop_user_table_if_exists(table_name = table_name, conn = conn)
        exists_before = exec.exists_user_table(table_name)
        prep_data = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        import_data = ImportData(prep_data,conn)             
        import_data.create_user_table()
        exists_after = exec.exists_user_table(table_name) 
        tableOps = TableOps(table_name,conn)
        nrows = tableOps.nrows()
        # Sjekker også datatype
        comparison_data_types = prep_data.map2db_dtype()
        data_types = tableOps.get_data_types_of_user_table()
        #Rydd til slutt opp
        tableOps.drop_user_table()
        conn.close()
        subresults = [not exists_before,exists_after,nrows == 0,len(comparison_data_types) == len(data_types)]
        #Sammenligner datatyper i database med hva det er meningen at det skal være
        data_type_comparisons = compare_db_data_types(data_types,comparison_data_types)
        for col in data_type_comparisons.keys():
            subresults.append(data_type_comparisons[col])
        #    
        self.assertTrue(pd.Series(subresults).all()) 
       
    
    def test_insert_data(self):
        print('Er nå inne i TestImport.test_insert_data')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        date_cols = find_date_columns(test_data)
        date_cols.append('date_only_na')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols,timestamp_format = 'YYYY-MM-DD HH24:MI:SS')
        comparison_data_types = preprocess.map2db_dtype()
        #
        first_half = floor(test_n/2)
        test_data1 = test_data.copy().iloc[0:first_half,:]
        #MOBSSSSSSSSSS: Må resette indeks for at eksempel skal virke
        test_data2 = test_data.copy().iloc[first_half:test_n,:].reset_index(drop=True)
        test_data2.reset_index(inplace=True,drop=True)
        #
        conn = Connect().get_connection()
        drop_user_table_if_exists(table_name = table_name, conn = conn)
        import_data = ImportData(preprocess,conn) 
        import_data.create_or_replace_table_and_insert_data(test_data1)
        tableOps = TableOps(table_name,conn)
        nrows1 = tableOps.nrows()
        import_data.insert_data(test_data2)
        nrows2 = tableOps.nrows()
        #sammenligner datatyper
        data_types = tableOps.get_data_types_of_user_table()  
        #
        #Rydd til slutt opp
        tableOps.drop_user_table()
        conn.close()
        #
        subresults = [nrows1 ==first_half ,nrows2 == test_n]  
        #sammenligner datatyper
        data_type_comparisons = compare_db_data_types(data_types,comparison_data_types)
        for col in data_type_comparisons.keys():
            subresults.append(data_type_comparisons[col])
        #    
        self.assertTrue(pd.Series(subresults).all()) 
        

        
class TestExport(unittest.TestCase):
    
    def test_get_table(self):
        print('Er nå inne i TestExport.test_get_table som ikke er ferdig implementert')  
        #Memo to self: First need to import some data
        table_name = "ORA$PTT_MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        date_cols = find_date_columns(test_data)
        date_cols.append('date_only_na')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        #
        conn = Connect().get_connection()
        import_data = ImportData(preprocess,conn)
        import_data.create_or_replace_table_and_insert_data(test_data)
        #      table_name : str, conn: oracledb.Connection,postprocess: bool =True
        export_data = ExportData(table_name = table_name,conn=conn,postprocess_data = False) 
        #
        df = export_data.get_table()
        conn.close()
        subresults = [isinstance(df,pd.DataFrame),df.shape[0] == test_n]
        self.assertTrue(pd.Series(subresults).all())  
    
    #Memo til selv: Må bytte ut denne testem med å teste på egenprodusert (og importert) data!!!
    # nr
    def test_clean_export(self):
        print('Er nå inne i TestExport.test_get_table som ikke er implementert')  
        table_name = "ORA$PTT_MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        date_cols = find_date_columns(test_data)
        date_cols.append('date_only_na')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        #
        conn = Connect().get_connection()
        import_data = ImportData(preprocess,conn)
        import_data.create_or_replace_table_and_insert_data(test_data)
        #      table_name : str, conn: oracledb.Connection,postprocess: bool =True
        export_data = ExportData(table_name = table_name,conn=conn,postprocess_data = True) 
        df = export_data.get_table()
        conn.close()
        print('df.head() er')
        print(df.head())
        #Sammenligner
        data_type_comparisons = compare_data_types(df,test_data)
        value_comparisons = value_comparison(df,test_data)
        subresults = []
        for col in data_type_comparisons.keys():
            subresults.append(data_type_comparisons[col])
            subresults.append(pd.Series(value_comparisons[col]).all())
        #    
        self.assertTrue(pd.Series(subresults).all())         
    

        
    