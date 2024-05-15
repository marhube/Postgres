#********** For testing
import os
import sys
#*******
import re
import pandas as pd
#**********
#********

#******* Start internimport
from Datatype import Datatype
#******* Slutt internimport
#Memo til selv: Har funnet ut at det er trygt å ha "" rundt tabellnavn borssett fra
# inne i where-betingelser i spørringer mot "USER_TABLES" eller "USER_PRIVATE_TEMP_TABLES"

def remove_comments_and_empty_lines(sql_code: str) -> str:
        # Regular expression to match comments starting with "--"
        comment_pattern = re.compile(r'--[^\n]*')
        # Split the code into lines
        lines = sql_code.split('\n')        
        cleaned_lines = []
        #
        for line in lines:            
            line = re.sub(comment_pattern, '', line) # Remove comments from the line
            line = line.strip() # Remove leading and trailing spaces from the line
            # Add the cleaned line to the list if it's not empty
            if line:
                cleaned_lines.append(line)
        # Join the cleaned lines with line breaks
        cleaned_code = '\n'.join(cleaned_lines)
        return cleaned_code

# Note to self: Small utility function
def remove_semicolon(sql_code: str) -> str:
    no_semicolon = re.sub(";","",sql_code)
    return no_semicolon
    
    


def sql_select(table_name: str) -> str:
    select_str = """SELECT *  """ + f'FROM "{table_name}";'
    return select_str


def user_table_schema(temporary: bool =False) -> str:
    schema = 'public'
    if temporary:
        schema = 'pg_temp_%'
    #
        
    return schema


def sql_list_user_persistent_tables() -> str:
    schema = user_table_schema(False)
    #
    str_sql_list_persistent_tables = f"""
    SELECT table_name 
    FROM information_schema.tables
    WHERE table_schema = '{schema}'
    ;
    """
    return remove_comments_and_empty_lines(str_sql_list_persistent_tables)

def sql_list_user_temp_tables() -> str:
    schema = user_table_schema(True)

    str_sql_list_temp_tables = f'''
    SELECT table_name 
    FROM information_schema.tables
    WHERE table_schema LIKE '{schema}'
    ;
    '''
    return remove_comments_and_empty_lines(str_sql_list_temp_tables)



def sql_list_all_user_tables() -> str:
    persistent_schema = user_table_schema(False)
    temp_schema = user_table_schema(True)

    str_sql_list_all_tables = f"""
    SELECT table_name 
    FROM information_schema.tables
    WHERE (table_schema = '{persistent_schema}' OR
     table_schema LIKE '{temp_schema}');
    """

    return remove_comments_and_empty_lines(str_sql_list_all_tables)

def sql_nrows(table_name: str) -> str:
    str_nrows = f'SELECT COUNT(*) FROM "{table_name}"'
    return str_nrows


def sql_search_user_table(table_name: str) -> str:
    sql_main_part_raw = sql_list_all_user_tables()
    sql_main_part_no_semicolon = remove_semicolon(sql_main_part_raw)
    sql_main_part = remove_comments_and_empty_lines(sql_main_part_no_semicolon)
    sql_last_part = f"UPPER(table_name) = '{table_name.upper()}';";
    str_search_table  =  " AND ".join([sql_main_part,sql_last_part])
    return remove_comments_and_empty_lines(str_search_table)


# Memo to self: sql_data_types_user_table only works for persistent tables
# Is now changed so that "NUMBER" with length 0 is outputted as "INTEGER", 
def sql_data_types_user_table(table_name: str) -> str:
    str_data_types_user_table = remove_semicolon(sql_search_user_table(table_name))
    persistent_schema = user_table_schema(False)
    temp_schema = user_table_schema(True)
    #
    str_data_types  = f"""
    SELECT column_name,data_type
    FROM information_schema.columns
    WHERE (table_schema = '{persistent_schema}' OR table_schema LIKE '{temp_schema}') AND
    table_name = ({str_data_types_user_table});
    """    
    return remove_comments_and_empty_lines(str_data_types)


def sql_drop_user_table(table_name: str) -> str:
    str_drop_user_table = f'DROP TABLE "{table_name}";'
    return str_drop_user_table
        
def sql_drop_user_table_if_exists(table_name: str) -> str:
    str_drop_user_table_if_exists = f'DROP TABLE IF EXISTS "{table_name}";'
    return str_drop_user_table_if_exists
        

# Lager så en klasse "SQL_Generator"
class SQL_Generator:
    #Variabelen "datetime2date" sier om datatypen "datetime" i pandas skal settes som
    #"DATE" eller  som timestamp.  Motivasjonen bak å sette datetime til DATE er at 
    #tidfested informajon i Folkeregisteret kun angir dato.
    #Er nå valgfritt om man vil ha med ";" på slutten eller ikke. For å kjøre i Python (oracledb) 
    # kan ";" ikke være med.
    # MEmo til selv: "semicolon" brukes av både "SQL_Create" og "SQL"
    def __init__(
        self,
        dtypes: pd.core.series.Series,
        table_name: str,
        table_type: str|None = None,
        datetime2date: bool = False,
        date_format: str = "YYYY-MM-DD",
        timestamp_format: str = 'YYYY-MM-DD HH24:MI:SS',
        #"date_cols" is for mandating certain columns to have date format inside Oracle and to specify
        date_cols: list = [],
        timestamp_cols: list = [],
        # In Postgres unless the table name is surrounded by double quotes ("") they are converted to lower case, not upper case.
        lowercase: bool = True 
        ):        
        if table_type is None:
            table_type = "TEMPORARY"
        else:
            table_type = table_type.upper()
        
        permitted_data_types = ['TEMPORARY','PERSISTENT'] 
        if table_type not in permitted_data_types:
            raise Exception(f"table_type has to be one of {permitted_data_types}.")
        #Navn på "PRIVATE"-tabeller må i Oracle av en eller annen grunn starte med "ORA$PPT_"
        self.date_cols = date_cols
        self.timestamp_cols = timestamp_cols
        self.table_type = table_type
        self.datetime2date = datetime2date
        self.orig_table_name = table_name
        self.table_name = self.clean_table_name()
        if lowercase:
            self.table_name = self.table_name.lower()
        #        
        self.cols = list(dtypes.index)
        self.str_dtypes =  [str(dtype) for dtype in dtypes]
        self.dtypes = Datatype.get_classes_from_dtypes(dtypes)
        print(f'self.dtypes er {self.dtypes}')
        # Merk: Mappingen er nå en dictionary
        self.db_dtypes =  self.map2db_dtype() 
        #Memo til selv: Datoformatet i databasen er satt opp til å være "DD.MM.YY"
        self.date_format = date_format
        self.timestamp_format = timestamp_format
        #   
    
    
    #Funksjon for å omgjøre "potensielt problematiske navn på tabell". Fjerner punktum og mellomrom
    #
    @staticmethod
    def clean_name(name: str) -> str:
        patterns = ["\.+","\s+"]
        for pattern in patterns:
            name = re.sub(pattern=pattern,repl='_',string=name)
        #Fjerner til slutt eventuelle "etterfølgende underscores"
        name = re.sub('_+',repl="_",string=name)
        return name
    #Memo til selv: Kolonnenavn kan "omsluttes" med dobbeltfnutter slik at de kan inneholde omtrent hva
    # som helst av tegn, men det samme er ikke tilfelle for tabellnavn.
    def clean_table_name(self) -> str:
        table_name = SQL_Generator.clean_name(self.orig_table_name)
        return table_name
    #
    def map2db_dtype(self) -> dict:
        #Hvis ønskelig (som forklart i init over) så mappes datetime til DATE
        pg_datetime = "TIMESTAMP"
        if self.datetime2date:
            pg_datetime = 'DATE'
        #
        dtype_mapping = {
            'numpy.int32': 'INTEGER',
            'numpy.int64': 'INTEGER',
            'numpy.float64': 'DOUBLE PRECISION',
            'numpy.object_': 'TEXT',
            'numpy.datetime64': pg_datetime
        }
        #
        pg_dtypes = {}
        
        for key,value in self.dtypes.items():
            if key in self.date_cols:
                pg_dtypes[key] = "DATE"
            elif key in self.timestamp_cols:
                pg_dtypes[key] = "TIMESTAMP"
            else:
                pg_dtypes[key] = dtype_mapping[value]
        #
        
        return pg_dtypes
    #
#
    
#Lager så en klasse "SQL_Create"
class SQL_Create:
    #
    def __init__(self, 
                 sql_generator: SQL_Generator,
                 semicolon: bool = True):
        self.sql_generator = sql_generator
        self.semicolon = semicolon
    #
    def sql_create_table(self) -> str:
        print('Er nå inne i sql_create_table der self.sql_generator.dtypes er')
        print(self.sql_generator.dtypes)
        print('og self.sql_generator.db_dtypes er')
        print(self.sql_generator.db_dtypes)
        # Memo til selv: Må ha med "innskutt tabulator (\t) for at det skal se pent ut"
        col_coltype_list = ',\n\t'.join(
            [' '.join([f'"{col}"',self.sql_generator.db_dtypes[col]]) for col in self.sql_generator.cols])
                    
        #       
        create_table_subparts = ['CREATE',"TABLE",f'"{self.sql_generator.table_name}"']
        table_post_option = ""
        #Memo til selv: Må ha et ekstra mellomrom på slutten for at "TEMPORARY" ikke skal "kollidere" med "TABLE".
        #Memo til selv: FRa https://stackoverflow.com/questions/51272128/trying-to-create-a-temp-table-in-oracle for forklaring
        # om "OM COMMIT PRESERVE DEFINITION"
        #Memo til selv: For å kunne fungere som en "commit" i Python må man ikke ha med semikolon.
        # Memo til selv: For "GLOBAL TEMPORARY TABLE" så er det flere valgmuligheter enn bare "ON COMMIT PRESERVE DEFINITION",
        if self.sql_generator.table_type == "TEMPORARY":
            create_table_subparts.insert(1,' TEMPORARY')

        #
        # Memo til selv: Må settes opp slik for at det skal se "riktig" ut tabulatormessig.
        create_table_part = ' '.join(create_table_subparts)
        statement = f"""{create_table_part} ( 
        {col_coltype_list}
        ) {table_post_option}
        """
        if self.semicolon:
            statement = statement + ";"
        return statement
    #
#




class Parser:
    def __init__(
        self,
        sql_file_path = None,
        sql_query = None    
        ):
        self.sql_file_path = sql_file_path
        # Hvis ønskelig les sql-koden inn fra fil
        if self.sql_file_path is not None:            
            self.sql_query = self.get_sql_code()
        else:
            self.sql_query = sql_query
    #
    def get_sql_code(self):
        sql_code = None
        # Get lines of code
        with open(self.sql_file_path, 'r', encoding="latin-1") as sql_file:
            sql_code = sql_file.readlines()
        #
        sql_code = " ".join(sql_code)
        return sql_code
    # 
    def parse_sql_code(self):
        # Define a regular expression pattern to match comments and comment blocks
        comment_pattern = r'(--[^\n]*)|(/\*[\s\S]*?\*/)'
        # Remove comments and comment blocks using regex
        sql_query = re.sub(comment_pattern, '', self.sql_query)
        # Split the input SQL query into lines
        lines = sql_query.splitlines()
        # Initialize variables
        statements = []
        current_statement = []
        # Iterate through lines
        for line in lines:
            # Remove leading and trailing whitespaces
            line = line.strip()
            # Check if the line is not empty
            if len(line) > 0:
                # Append the current line to the current statement
                current_statement.append(line)
            # Check if the line ends with a semicolon (indicating the end of a statement)
            if line.endswith(";"):
                # Remove the semicolon and join the lines to form a complete statement
                statement = " ".join(current_statement)
                statements.append(statement[:-1])  # Remove the last character (the semicolon)
                current_statement = []
        # Append any remaining lines as the last statement
        if current_statement:
            statement = " ".join(current_statement)
            statements.append(statement)
        #
        return statements
    #


#Additional sql for obtaining information about the system
    
def sql_get_version() -> str:
    str_sql_get_version = "SELECT version();"
    return str_sql_get_version 
#
