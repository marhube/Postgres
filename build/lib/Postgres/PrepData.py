#******** Start for testing
import os
import sys # 
# ********** Slutt for testing
#*******
import pandas as pd
import numpy as np
import datetime as dt
#*****

#**********
from typing import Optional
#********

#********** Start databaserelatert
import psycopg2
#********** Slutt databaserelatert

#****** Start internimport
from Datatype import Datatype
from Postgres.SQL import SQL_Generator
from Postgres.Execute import TableOps
from dbmisc.dbmisc import datetime2date,replace_time_format
#****** Slutt internimport



  


# Memo to self: Converting Oracle dates to Python datetime.date (export table from Oracle to Python )



# The class PreprocessData does not have its own init.
class PreprocessData(SQL_Generator):    
    # Lager først en teknisk hjelpefunkjson som først og fremst er ment å håndtere datokolonner (resten blir "as is")
    #Memo til selv: Indekser i Python begynner på 0, mens indekser i Oracle begynner på 1. Må derfor legge til 1.
    # Hivs hardcode_value" er True så settes selve verdien inn i "insert-into"- setningen.
 
  
    #OBSSSSS value kan ha mange ulike datatyper. Må gjøre en uttømmende sjekk av hvilke
    #  Responsen til "conditional_col_transform" kan kun bli datetime hvis enten self.datetime2date er False
    # eller hvis map2db_dtype er satt til å mappe Python datetime til Oracle timestamp.
    def conditional_col_transform(self,
                                  col: str,
                                  ) -> str:
       
        #Memo til selv: Må ha ekstra fnutter rundt hvis er streng
        # Memo til selv: Trenger også å vite hvilken kolonne det er snakk om
        col_element = "%s"

        if self.db_dtypes[col].upper() == "DATE":       
            col_element = f"TO_DATE({col_element}, '{self.date_format}')"
        elif self.db_dtypes[col].upper() == "TIMESTAMP":       
            col_element = f"TO_TIMESTAMP({col_element}, '{self.timestamp_format}')"
        elif self.db_dtypes[col].upper() in ["INTEGER","DOUBLE PRECISION"]:
            col_element = f"CAST({col_element} AS {self.db_dtypes[col].upper()})"   
        #
        return col_element
    #Memo til selv: Har skilt ut første del av "INSERT" i en egen funksjon slik at denne
    # delen også skal kunne brukes av "sql_hardcoded_insert" lenger ned
    #argumentet "include_VALUES" er for å inkludere "VALUES" også med tenke på hardkoding.
    def insert_into_header(self,include_values: bool =False) -> str:
        col_list = ', '.join([f'"{col}"' for col in self.cols])
        collapsed_col_list = f"({col_list})"
        if include_values:
            collapsed_col_list = ' '.join([collapsed_col_list,'VALUES'])
        #
        sql_before_values = f'''INSERT INTO "{self.table_name}"
            {collapsed_col_list}
        '''
        #
        return sql_before_values
    # Memo til selv: Den genererte SQL-koden fra "sql_insert_into" skal typisk kjøres fra Python.
    # "semicolon" er derfor som default her satt til False
    def sql_values_part(self):
        values_inner_part = ', '.join([self.conditional_col_transform(col) for col in self.cols])
        values_part = f"VALUES ({values_inner_part})"
        return values_part
   
 
    @staticmethod
    def tabulate(sql_before: str,values_part: str) -> str:
        merged_string = ''.join([sql_before,values_part])
        str_sql_insert_into = '\n\t'.join([line.strip() for line in merged_string.split("\n")])
        return str_sql_insert_into
        
       
    #Memo to self: "sql_insert_into" is now changed so that it no longer (optionnally)
    # adds semicolons to each statement.
    def sql_insert_into(self) -> str:
        #Lager først første del av "insert into" (før "Values" begynner)
        sql_before_values = self.insert_into_header()
        #Memo til selv: Skal ha poisi
        values_part = self.sql_values_part()
        #Memo til selv: Når har laget en streng med trippelfnutter så blir det "\n" som separator når man joiner denne
        # strengen med en annen streng når man koder "".join(..)
        # Gjør til slutt kodestrengen litt penere med bedre tabulering
        str_sql_insert_into =  PreprocessData.tabulate(sql_before_values,values_part)    
        return str_sql_insert_into
     
    # 

      
    def preprocess_time_values(self,time_values: pd.Series, percent_format: str) -> pd.Series:
        time_values = time_values.map(
            lambda x: x if isinstance(x,str) else (x.strftime(percent_format) if not pd.isna(x) else None)            
                    )
        #
        return time_values    
 
        
    # If a date value is a datetime string then keep only the "date part" of the string
    def preprocess_date_values(self,date_values: pd.Series) -> pd.Series:
        date_values = date_values.map(lambda x: x[0:len(self.date_format)] if isinstance(x,str) else x)        
        date_values = datetime2date(date_values)
        percent_format = replace_time_format(self.date_format)
        date_values = self.preprocess_time_values(date_values,percent_format)
        return date_values
    
    def preprocess_datetime_values(self,datetime_values: pd.Series) -> pd.Series:
        percent_format = replace_time_format(self.timestamp_format)
        datetime_values = self.preprocess_time_values(datetime_values,percent_format)
        return datetime_values

    def preprocess_obj_values(self,obj_values: pd.Series) -> pd.Series:
        print('Er nå inne i preprocess_obj_values der obj_values først er')
        print(obj_values)
        obj_values = pd.Series([str(x) if not pd.isna(x) else None for x in obj_values.to_list()])
        print('Før avlevering så er obj_values ')
        print(obj_values)
        return obj_values

    #Memo to self: data_series.name extracts colname
    def preprocess_column(self,data_series: pd.Series) ->  pd.Series:
        col = data_series.name
        if self.db_dtypes[col].upper() == "DATE":
            data_series = self.preprocess_date_values(data_series)
        elif self.db_dtypes[col].upper() == "TIMESTAMP":
            data_series = self.preprocess_datetime_values(data_series)
        else:
            data_series = self.preprocess_obj_values(data_series)
        #
        print('Før avlevering fra preprocess_column så er data_series')
        print(data_series)
        return data_series
    
    def preprocess_data(self,df: pd.DataFrame ,copy: bool =True) -> list: 
        print('Er nå inne i preprocess_data der df er')
        print(df)            
        new_df = None
        if copy:
            new_df = df.copy()
        else:
            new_df = df
        # Integers må derfor gjøres om til "int32" hvis er "int64" Får ellers
        # oracledb.exceptions.NotSupportedError: DPY-3002: Python value of type "int64" is not supported
        
        #
        for col in list(df.columns):
            print(f' col er {col} og new_df[col]  er først')
            print(new_df[col] )
            new_df[col] = self.preprocess_column(new_df[col])
            print('new_df[col]  er deretter')
            print(new_df[col] )
        #
        data_to_insert = [tuple(row) for row in new_df.itertuples(index=False)]    
        return data_to_insert


class PostprocessDataFrame(TableOps):
    def __init__(self, df: pd.DataFrame,table_name: str, conn: psycopg2.extensions.connection):
        self.df = df
        self.conn = conn
        self.table_name = super().get_exact_user_table_name(table_name)       
        self.db_data_types = super().get_data_types_of_user_table()
    
    def postprocess_dataframe(self) -> 'PostprocessDataFrame':
     
        for col in self.df.columns:
            postprocess_col = self.PostprocessColumn(self.df[col],self.db_data_types[col])
            postprocess_col.postprocess_column()            
            self.df[col] = postprocess_col.values   
        #
        
        return self
    
    class PostprocessColumn:
        def __init__(self,values: pd.Series ,db_data_type: str):
            #current_block: - Temporary list for the current control structure block
            # execution_order<: List to hold statements in execution order
            self.values = values
            self.db_data_type = db_data_type
    
        #Utilty functiona for "postprocess_column". All null is a special case
        def postprocess_date(self) -> 'PostprocessDataFrame.PostprocessColumn':
            self.values = datetime2date(self.values)

            return self
        
        def postprocess_integer(self) -> 'PostprocessDataFrame.PostprocessColumn':
            new_values = self.values.map(lambda x: np.int64(round(x)) if not pd.isna(x) else pd.NA)
            #Memo to self: Need to explicity specify dtype
            self.values = pd.Series(new_values,dtype = "Int64")
            return self
        
        def postprocess_float(self) -> 'PostprocessDataFrame.PostprocessColumn':            
            self.values = self.values.map(lambda x: float(x) if not pd.isna(x) else np.nan)
            return self  

        def postprocess_timestamp(self) -> 'PostprocessDataFrame.PostprocessColumn':
            self.values = self.values.map(lambda x: x if not pd.isna(x) else pd.NaT)
            return self
        
        def postprocess_column(self) -> 'PostprocessDataFrame.PostprocessColumn':
            # Special case if all values are missing

            if self.db_data_type.upper() == "DATE":
                self.postprocess_date()
            elif self.db_data_type.upper() == "INTEGER":
                self.postprocess_integer()
            elif self.db_data_type.upper() in ['DOUBLE PRECISION','NUMERIC','REAL']:
                self.postprocess_float()
            elif self.db_data_type.upper().startswith("TIMESTAMP"):
                self.postprocess_timestamp()  
            #
            return self
#
    
    
    

 