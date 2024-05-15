import os
import sys
import re
#*''
import pandas as pd

#********** Start databaserelatert
#******** Slutt databaserelatert

#Import intern kode
from Postgres.Postgres_Connect import Connect
from Postgres.Execute import Exec,drop_user_table_if_exists
import Postgres.SQL as SQL


import unittest

class TestExecute(unittest.TestCase):
    def test_exec_and_commit(self):
        print('Er nå inne i TestExecute.test_exec_and_commit')
        table_name = "MY_TABLE"
        str_create_temp_table = f"""
        CREATE TEMPORARY TABLE "{table_name}" (
        my_int_column integer,
        my_string_column text
        )
        ;
        """
        conn = Connect().get_connection()
        exec = Exec(conn=conn)
        #Memo til selv!   Mange tester her som skal fjernes etter hvert!!!!!!!!!!!
        drop_user_table_if_exists(table_name = table_name,conn = conn)

        str_drop_user_table_if_exists = SQL.sql_drop_user_table_if_exists(table_name)
        exec.exec_and_commit(str_drop_user_table_if_exists)
        #
        table_exists_before = exec.exists_user_table(table_name) 
        exec.exec_and_commit(str_create_temp_table)
        #
        str_sql_data_types_user_table = SQL.sql_data_types_user_table(table_name)
        data_types =  exec.exec_fetch(str_sql_data_types_user_table)
            

        #
        table_exists_after_create = exec.exists_user_table(table_name) 
        str_drop_user_table = SQL.sql_drop_user_table(table_name)
        exec.exec_and_commit(str_drop_user_table)
        table_exists_after_drop = exec.exists_user_table(table_name) 


        #
        subresults = [
            not table_exists_before,
            table_exists_after_create,
            not table_exists_after_drop
        ]
        #

        self.assertTrue(pd.Series(subresults).all()) 
      

    
    def test_exec_insert(self):
        print('Er nå inne i TestPostgresConnect.test_exec_insert(') 
        table_name = "MY_TABLE"
        #
        str_create_temp_table = f'''
        CREATE TEMP TABLE "{table_name}" (
        my_int_column integer,
        my_string_column text
        )
        ;
        '''

        conn = Connect().get_connection()
        exec = Exec(conn=conn)
        exec.exec_and_commit(str_create_temp_table)
        data_to_insert = [
        (1, 'Data 1'),
        (2, 'Data 2'),
        (3, 'Data 3')
        ]
        # Add as many tuples as needed
        insert_query = f'INSERT INTO "{table_name}" (my_int_column, my_string_column) VALUES (%s, %s);'
        exec.exec_insert(insert_query,data_to_insert)
        # Clean up
        str_drop_user_table = SQL.sql_drop_user_table(table_name)
        exec.exec_and_commit(str_drop_user_table)
        #
        conn.close()

     
    def test_get_exact_table_name(self):
        print('Er nå inne i TestExec.test_get_exact_table_name(')
        #Lager først tabellen (uten å populere den)
        table_name =  "ORA$PTT_blabla_mixedCase"
        str_create_temp_table = f"""
        CREATE TEMP TABLE "{table_name}" (
        my_int_column integer,
        my_string_column text
        )
        ;
        """

        conn = Connect().get_connection()
        exec = Exec(conn=conn)
        exec.exec_and_commit(str_create_temp_table)
        #
        exact_table_name = exec.get_exact_user_table_name(table_name) 
        # Clean up
        str_drop_user_table = SQL.sql_drop_user_table(table_name)
        exec.exec_and_commit(str_drop_user_table)
        #

        conn.close()
        self.assertEqual(exact_table_name,table_name) 

    
    
  
        
        
    