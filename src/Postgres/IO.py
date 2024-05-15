#******** Start type hinting
#******** Slutt type hinting

import os
import sys # For testing
#
import pandas as pd
#********** Start databaserelatert
import psycopg2
#********** Slutt databaserelatert

#******* Start internimport
import Postgres.SQL as SQL
from Postgres.SQL import SQL_Create
from Postgres.PrepData import PreprocessData,PostprocessDataFrame
from Postgres.Execute import drop_user_table_if_exists,Exec
#******* Slutt internimport


class ImportData(Exec): 
    #
    def __init__(self, 
                 preprocess: PreprocessData,
                 conn: psycopg2.extensions.connection
                 ):
        self.conn = conn
        self.preprocess = preprocess        
        self.table_name = preprocess.table_name
    
    
    #Memo to self: "set_table_name" is 
    def set_table_name(self) -> 'ImportData':
        table_name = self.preprocess.table_name
        if super().exists_user_table(table_name):
            table_name = super().get_exact_user_table_name(table_name)
        
        self.preprocess.table_name = table_name       
        self.table_name = table_name
        return self
    
    
    def create_user_table(self) -> None:
        sql_create = SQL_Create(self.preprocess)
        str_create_table = sql_create.sql_create_table()
        super().exec_and_commit(str_create_table)
        
    def create_or_replace_user_table(self) -> None:
        drop_user_table_if_exists(self.table_name,self.conn)
        self.create_user_table()
    
    
    
    #Memo to self: table_name might be modified
    def insert_data(self,df: pd.DataFrame) -> 'ImportData':
        self.set_table_name()
        str_insert_into =  self.preprocess.sql_insert_into()
        print('Er inne i insert_data der str_insert_into er')
        print(str_insert_into)
        data_to_insert = self.preprocess.preprocess_data(df=df)
        print('og data_to_insert er')
        print(data_to_insert)
        super().exec_insert(str_insert_into, data_to_insert)
        return self
    



    def create_or_replace_table_and_insert_data(self,df: pd.DataFrame) -> 'ImportData':
        self.create_or_replace_user_table()
        self.insert_data(df)
        return self
    
    def create_table_if_not_exists_and_insert_data(self,df: pd.DataFrame) -> 'ImportData':
        if not super().exists_user_table(self.table_name):
            self.create_user_table()
        #
        self.insert_data(df)
        return self
#    
    
    
class ExportData(Exec):
    def __init__(self, table_name : str, conn: psycopg2.extensions.connection,postprocess_data: bool =True):
        self.conn = conn
        self.table_name = super().get_exact_user_table_name(table_name)
        self.postprocess = postprocess_data          
    
    # 
    
    def get_table(self) -> pd.DataFrame:
        # Sjekker om 
        sql_output_table = SQL.sql_select(self.table_name)
        df = super().exec_fetch(sql_output_table)
        if self.postprocess: 
            postprocess = PostprocessDataFrame(df,table_name = self.table_name,conn = self.conn)
            postprocess.postprocess_dataframe()
            df = postprocess.df
        #
        
        return df
    #
        