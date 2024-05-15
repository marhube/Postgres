
#************** Start importing python modules for testing and degbugging
import sys # For sys.exit()
import time
import os # For os.listdir()
#*********** End importing python modules for testing and degbugging


import pandas as pd
import numpy as np
#********** Start databaserelatert
import psycopg2
#********** Slutt databaserelatert


#*********** Start internimport
import Postgres.SQL as SQL
from Postgres.SQL import Parser

#*********** Slutt internimport

class PostgresError(Exception):
    """Custom exception class."""
    def __init__(self, message):
        super().__init__(message)


class Exec:
    def __init__(
        self,
        conn: psycopg2.extensions.connection
        ):
        self.conn = conn       
    #
    def exec_and_commit(self,query: str) -> None:
        crsr = self.conn.cursor()
        crsr.execute(query)
        self.conn.commit()
        crsr.close()
  
    def exec_insert(self,sql_insert: str, data_to_insert: list) -> None:
        crsr = self.conn.cursor()
        crsr.executemany(sql_insert,data_to_insert)
        self.conn.commit()
        crsr.close()
    
    def exec_fetch(self,table_query: str) ->  pd.DataFrame:
        print(f'Er nå inne i exec_fecth der table_query er')
        print(table_query)
        crsr = self.conn.cursor()
        crsr.execute(table_query)        
        rows = crsr.fetchall()
        # Necessary to have a "raise exception"-clause for mypy to be happy
        if crsr.description is None: 
            raise PostgresError("Seeminigly no table is created.")
        #
        
        columns = [column[0] for column in crsr.description]
        crsr.close()
        df = pd.DataFrame((tuple(row) for row in rows), columns=columns)
        return df
        

    def exists_user_table(self,table_name: str) -> bool:        
        str_search_for_table  =  SQL.sql_search_user_table(table_name)
        df = self.exec_fetch(str_search_for_table)
               
        return df.shape[0] > 0
    

    def get_exact_user_table_name(self,table_name: str) -> str:
        if not self.exists_user_table(table_name):
            raise PostgresError(f"Table {table_name} does not exist")
        #
        str_get_exact_table_name = SQL.sql_search_user_table(table_name)
        #Memo to self: Need to enclose with "str()" for mypy to accept that 
        #exact_tbale_names actually is a string        
        exact_table_name = self.exec_fetch(str_get_exact_table_name).iloc[0,0]
        if not isinstance(exact_table_name,str):
            raise PostgresError("Fetch result is not a string")
        return exact_table_name

    # Kjører Postgres-sql kode
    def run_sql_from_file(self,path_sql: str,sql_query=None) -> None: 
        print(f'Er nå inne i run_sql_from_file der sql-koden som skal kjøres er fra {path_sql}')       
        parsed_sql_query = Parser(path_sql,sql_query=sql_query).parse_sql_code()
        ## Commit the transaction 
        for statement in parsed_sql_query: 
            self.exec_and_commit(statement)
         #
        print('Klarte forhåpentlig å gjennomføre hele kjøringen')
        
    def run_if_not_exists(self,table_name: str,path_sql: str):
        #Kjør opp tabell, hvis ikke finnes
        if not self.exists_user_table(table_name):
            self.run_sql_from_file(path_sql = path_sql)

    
    def get_list_of_user_tables(self,sql_query: str) -> list:
        list_of_user_tables_as_frame = self.exec_fetch(sql_query)
        list_of_user_tables = list_of_user_tables_as_frame['table_name'].tolist()
        return list_of_user_tables

    
    def get_list_of_persistent_user_tables(self) -> list:
        str_sql_list_all_user_tables = SQL.sql_list_all_user_tables() 
        list_of_user_tables = self.get_list_of_user_tables(str_sql_list_all_user_tables)
        return list_of_user_tables
    
    def get_list_of_temporary_user_tables(self) -> list:
        sql_list_temporary_user_tables = SQL.sql_list_user_temp_tables() 
        list_of_temp_tables = self.get_list_of_user_tables(sql_list_temporary_user_tables)
        return list_of_temp_tables
    
    
    def get_list_of_all_user_tables(self) -> list:
        sql_list_all_tables = SQL.sql_list_all_user_tables() 
        list_all_user_tables = self.get_list_of_user_tables(sql_list_all_tables )
        return list_all_user_tables
    
    def get_version_info(self) -> pd.DataFrame:
        str_sql_get_version = SQL.sql_get_version() 
        version_info_frame = self.exec_fetch(str_sql_get_version)
        if not isinstance(version_info_frame,pd.DataFrame):
            raise PostgresError("Attempt to obtain information about Postgres version failed.")
        return version_info_frame
#    

# Memo to self: The class "TableOps" requires the table "table_name" to exist
class TableOps(Exec):
    def __init__(self, table_name: str, conn: psycopg2.extensions.connection):
        self.conn = conn
        self.table_name = super().get_exact_user_table_name(table_name)  

    def nrows(self) -> int:
        str_sql_nrows = SQL.sql_nrows(self.table_name)
        nrows_frame = super().exec_fetch(str_sql_nrows)
        #
        nrows_int =  nrows_frame.iloc[0,0]
        #
        if isinstance(nrows_int,int) or isinstance(nrows_int,np.int64):
            nrows_int = int(nrows_int)
        else:
            raise PostgresError(f" Output from query {str_sql_nrows} is not an integer.")
        return nrows_int    
     

    def get_data_types_of_user_table(self) -> dict:
        # Only works for persistent tables
        str_sql_data_types_user_table = SQL.sql_data_types_user_table(self.table_name)
        db_data_types_frame = super().exec_fetch(str_sql_data_types_user_table)
        db_data_types_dict = dict(zip(db_data_types_frame.iloc[:,0], db_data_types_frame.iloc[:,1]))

        return db_data_types_dict

    def drop_user_table(self) -> None:
        str_drop_user_table = SQL.sql_drop_user_table(self.table_name)
        super().exec_and_commit(str_drop_user_table)
    # 

               

def drop_user_table_if_exists(table_name: str,conn: psycopg2.extensions.connection) -> None:
    # Sjekk om tabellen finnes
    exec = Exec(conn)
    if exec.exists_user_table(table_name):
        tableOps = TableOps(table_name = table_name,conn=conn)
        tableOps.drop_user_table()
    #    