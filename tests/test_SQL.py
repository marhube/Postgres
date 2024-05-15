import os
import sys
import re
#*''
import pandas as pd

#********** Start databaserelatert
#******** Slutt databaserelatert

#************ Start import intern kode
from TestData.TestData import generate_test_data
from Datatype.Datatype import robust_get_data_type_from_values

import Postgres.SQL as SQL
from Postgres.SQL import SQL_Generator,SQL_Create,Parser

#************** Slutt import intern kode

import unittest


def get_lines_of_parsed_sql_code(sql_file_path):
    sql_parser = Parser(sql_file_path)
    sql_statements = sql_parser.parse_sql_code()
    sql_statements_lines = [item for  statement in sql_statements for item in statement.split('\n')]
    return sql_statements_lines

def read_stripped_lines(input_file):
    parsed_sql_file_path = input_file
    list_stripped_lines = []
    # Open the file for reading
    with open(parsed_sql_file_path, "r") as file:
        for line in file:
            # Add each line to the list, stripping the newline character
            list_stripped_lines.append(line.strip())

    return list_stripped_lines




class TestSQL(unittest.TestCase):
    
    def test_generate_select(self):
        print('Er nå inne i TestSQL.test_generate_select')  
        table_name = 'MY_TABLE'
        str_select_table = SQL.sql_select(table_name)
        str_comparison = f'SELECT *  FROM "{table_name}";'
        self.assertEqual(str_select_table,str_comparison)


    def test_remove_comments_and_empty_lines(self):
        print('Er nå inne i TestSQL.test_remove_comments_and_empty_lines som ikke er ferdig implementert')


    def test_parse_sql_code(self):
        print('Er nå inne i TestSQL.test_parse_sql_code som ikke er ferdig implementert')
    

    def test_init_SQL_Generator(self):
        print('Er nå inne i TestSQL.test_init_SQL_Generator')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        sql_generator = SQL_Generator(test_data.dtypes,table_name=table_name,date_cols = date_cols)
        subresults = [
            sql_generator.date_cols == date_cols,
            isinstance(sql_generator,SQL_Generator)
            ]
             
        
        self.assertTrue(pd.Series(subresults).all())
    #
    
    def test_clean_name(self):
        print('Er nå inne i TestSQL.test_clean_name som ikke er implementert')
        
    
    def test_clean_table_name(self):
        print('Er nå inne i TestSQL.test_clean_table_name som ikke er implementert')
    
    def test_map2db_dtype(self):
        print('Er nå inne i TestSQL.test_map2db_dtype som ikke er ferdig implementert')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        test_data['date_only_na'] = test_data['datetime_only_na']
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        sql_generator = SQL_Generator(test_data.dtypes,table_name=table_name,date_cols = date_cols)        
        db_data_types = sql_generator.map2db_dtype()
        #Må lage en fasit
        db_data_types_comparison = {}
        db_data_types_comparison['float_with_na']  = 'DOUBLE PRECISION'
        db_data_types_comparison['float_no_na']  = 'DOUBLE PRECISION'
        db_data_types_comparison['int_with_na']  = 'INTEGER'
        db_data_types_comparison['int_no_na']  = 'INTEGER'
        db_data_types_comparison['datetime_with_na']  = 'TIMESTAMP'
        db_data_types_comparison['datetime_no_na']  = 'TIMESTAMP'
        db_data_types_comparison['date_with_na']  = 'DATE'
        db_data_types_comparison['date_no_na']  = 'DATE'
        db_data_types_comparison['str_with_na']  = 'TEXT'
        db_data_types_comparison['str_no_na']  = 'TEXT'
        db_data_types_comparison['float_only_na']  = 'DOUBLE PRECISION'
        db_data_types_comparison['int_only_na']  = 'INTEGER'
        db_data_types_comparison['datetime_only_na']  = 'TIMESTAMP'
        db_data_types_comparison['date_only_na']  = 'DATE'
        db_data_types_comparison['str_only_na']  = 'TEXT'
        
        self.assertEqual(db_data_types,db_data_types_comparison)
        
    def test_init_SQL_Create(self):
        print('Er nå inne i TestSQL.test_init_SQL_Create')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = test_data = generate_test_data(test_n,include_all_missing = True)
        sql_generator = SQL_Generator(test_data.dtypes,table_name=table_name)
        sql_create = SQL_Create(sql_generator)       
        self.assertIsInstance(sql_create,SQL_Create)
        
        
    def test_sql_create_table(self):
        print('Er nå inne i TestSQL.test_sql_create_table')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n)
        date_cols = ["date_with_na","date_no_na"]
        sql_generator = SQL_Generator(
            test_data.dtypes,
            table_name=table_name,
            table_type = "TEMPORARY",
            date_cols = date_cols
            )
        sql_create = SQL_Create(sql_generator)
        sql_create_table = sql_create.sql_create_table()
        print('sql_create_table er')
        print(sql_create_table)
        #
        sql_comparison = f'''
        CREATE TEMPORARY TABLE "{table_name.lower()}" (
        "float_with_na" DOUBLE PRECISION,
        "float_no_na" DOUBLE PRECISION,
        "int_with_na" INTEGER,
        "int_no_na" INTEGER,
        "datetime_with_na" TIMESTAMP,
        "datetime_no_na" TIMESTAMP,       
        "date_with_na" DATE,
        "date_no_na" DATE,
        "str_with_na" TEXT,
        "str_no_na" TEXT
        )
        ;      
        '''
        sub_replaces = [("\n"," "),("\s+"," ") ]
        stripped_sql_create = sql_create_table.strip()
        stripped_sql_comparison = sql_comparison.strip()
        for sub_replace in sub_replaces:
            stripped_sql_create = re.sub(sub_replace[0],sub_replace[1],stripped_sql_create)
            stripped_sql_comparison= re.sub(sub_replace[0],sub_replace[1],stripped_sql_comparison)

        self.assertEqual(stripped_sql_create,stripped_sql_comparison)   
    