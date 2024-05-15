import os
import sys
import re
#*''
from dotenv import load_dotenv
import pandas as pd

#**********
import psycopg2
import sqlalchemy
import jaydebeapi
#********

#Forsøk på  systematisk testing
from Postgres.Postgres_Connect import Connect

import unittest

class TestPostgresConnect(unittest.TestCase):
    
    def test_replace_env_variables(self):
        print('Er nå inne i TestPostgresConnect.test_replace_env_variables') 
        conn_obj = Connect()
        str_path_home = "Path is: ${PATH}, and home is: ${HOME}"
        str_host_port = "Host is: ${PG_HOST}, and port is: ${PG_PORT}"
        #
        load_dotenv()       
        path = os.environ.get("PATH")
        home = os.environ.get("HOME")
        host = os.environ.get("PG_HOST")
        port = os.environ.get("PG_PORT")
        str_path_home_comparison = f"Path is: {path}, and home is: {home}"
        str_path_home_replaced = conn_obj.replace_env_variables(str_path_home)
        str_host_port  = "Host is: ${PG_HOST}, and port is: ${PG_PORT}"
        str_host_port_replaced = conn_obj.replace_env_variables(str_host_port)
        str_host_port_comparison = f"Host is: {host}, and port is: {port}"

        subresults = [
            (str_path_home_replaced ==  str_path_home_comparison),
            (str_host_port_replaced== str_host_port_comparison)
            ]
        
        self.assertTrue(pd.Series(subresults).all()) 


    
    
    def test_get_connection(self):
        print('Er nå inne i TestOracleConnect.test_get_connection')
        subresults = []
        conn_psycopg2 = Connect().get_connection()
        subresults.append(isinstance(conn_psycopg2 ,psycopg2.extensions.connection))
        conn_psycopg2.close()
        #
        conn_sqlalchemy = Connect(connection_type = "sqlalchemy").get_connection()
        subresults.append(isinstance(conn_sqlalchemy,sqlalchemy.engine.base.Connection))
        conn_sqlalchemy.close()
        #
        conn_jdbc = Connect(connection_type = "jdbc").get_connection()
        subresults.append(isinstance(conn_jdbc,jaydebeapi.Connection))
        conn_jdbc.close()

        # Check if every "sub-test" is passed 
        self.assertTrue(pd.Series(subresults).all())
        #
    #
    def test_jdbc_connection_string(self):
        print('Er nå inne i TestOracleConnect.test_jdbc_connection_string')
        conn_obj = Connect(connection_type = "jdbc")
        jdbc_connection_string =  conn_obj.create_pg_connection_string()
        comparison_string = f"jdbc:postgresql://${{PG_HOST}}:${{PG_PORT}}/{conn_obj.db_name}"
        self.assertEqual(jdbc_connection_string,comparison_string)
        

    
    def test_get_engine(self):
        print('Er nå inne i  TestOracleConnect.test_get_engine')
        conn_obj  = Connect(connection_type = "sqlalchemy")
        engine = conn_obj.get_engine()
        self.assertIsInstance(engine,sqlalchemy.engine.base.Engine)    
        engine.dispose() 
        
        
    