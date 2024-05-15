#************** Start importing python modules for testing and degbugging
import sys # For sys.exit()
import time
import os # For os.listdir()
#*********** End importing python modules for testing and degbugging
from typing import Optional

import pandas as pd
import re
#************
import psycopg2
import sqlalchemy
import jaydebeapi # type: ignore
#*********  Start databaserelatert
from dotenv import load_dotenv
from decouple import config
#*********  Slutt databaserelatert



#essage)
# Definerer min egen "exception" slik at bare trenger å definere "exception-beskjeden" en gang
class PostgresError(Exception):
    #Custom exception class
    def __init__(self, message):
        super().__init__(message)
                         
def raise_unsupported_connection_type() -> None:
    raise PostgresError('Connection type has to be one of "psycopg2", "sqlalchemy".' or "jdbc") 

#
class Connect:
    def __init__(
        self,
        user = 'mhunting',
        db_name = 'test_database',
        connection_type = "psycopg2", 
        jdbc_driver_path = "/home/m01315/Postgres/Jars/postgresql-42.7.1.jar"
        ):
        if connection_type not in ["psycopg2","sqlalchemy","jdbc"]:
            raise_unsupported_connection_type()
        #
        self.user = user
        self.db_name = db_name
        self.connection_type  = connection_type 
        self.jdbc_driver_path = jdbc_driver_path

       
    def get_connection(self) -> psycopg2.extensions.connection|sqlalchemy.engine.base.Connection|jaydebeapi.Connection|None:
        load_dotenv()
        conn = None
        password = config('_'.join(['PG_PASSWORD',self.user]))                
        #
        if self.connection_type ==  "psycopg2":
            conn = psycopg2.connect(
            dbname = self.db_name,
            user = self.user,
            password = password,
            host = config('PG_HOST') 
            )
        elif self.connection_type ==  "sqlalchemy":
            conn = self.get_engine().connect()
        elif self.connection_type ==  "jdbc":
            # Start the JVM with the JDBC driver
            conn_string = self.create_pg_connection_string()
            driver = config('PG_DRIVER')
            conn = jaydebeapi.connect(jclassname=driver,
                          url=self.replace_env_variables(conn_string),
                          driver_args=[self.user, password],
                          jars=self.jdbc_driver_path,
                          libs=None)
        else:
            raise_unsupported_connection_type()
        return conn

    
    def replace_env_variables(self,s: str) -> str:
        # This function will find all occurrences of ${VAR_NAME} in the string
        # and replace them with the value of the environment variable VAR_NAME.
        # Define a pattern to match ${ANYTHING_HERE}
        load_dotenv()
        pattern = re.compile(r'\$\{(.+?)\}')
        # Replace each found pattern with the corresponding environment variable
        def replace(match):
            var_name = match.group(1)
            #Passord er avhengig av brukernavn og derfor et spesialtilfelle
            if var_name == 'PG_PASSWORD':
                var_name = '_'.join([var_name,self.user])
            #

            return os.environ.get(var_name, '')  # Replace with env variable value
        #
        return pattern.sub(replace, s)



    def create_pg_connection_string(self) -> str:
        conn_string = ''
        if self.connection_type == "psycopg2":   
            conn_string = f"dbname={self.db_name} user={self.user} password=${{PG_PASSWORD}} host=${{PG_HOST}} port=${{PG_PORT}}"
        elif self.connection_type == "sqlalchemy":
            conn_string = f"postgresql+psycopg2://{self.user}:${{PG_PASSWORD}}@${{PG_HOST}}:${{PG_PORT}}/{self.db_name}" 
        elif self.connection_type == "jdbc":
            #No user name nor password, so it made explicit
            conn_string = f"jdbc:postgresql://${{PG_HOST}}:${{PG_PORT}}/{self.db_name}" 
        else:
            raise_unsupported_connection_type()
        
        return conn_string
    
    def create_jdbc_properties_dict(self) -> dict:
        properties = {
            "user": self.user,
            "password": "${PG_PASSWORD}",
            "driver": "${PG_DRIVER}"
        }
        return properties            
 #




    
    def get_engine(self):
        if self.connection_type !=  "sqlalchemy":
            raise Exception("Can only create engines with connection type sqlalchemy")
        #
        conn_string_implicit = self.create_pg_connection_string()
        conn_string = self.replace_env_variables(conn_string_implicit)
        engine = sqlalchemy.create_engine(conn_string)
        return engine
        




    #
   
#

