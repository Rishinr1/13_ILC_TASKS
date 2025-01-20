

import polars as pl
import pyodbc

inputfile = input("Enter the location of the input file ")
#"input\THEQ_SAMPLE_6.csv"
df = pl.read_csv(inputfile, separator='\t') # give the location file path here
print(df)
currency="USD"
undate = "9999-12-31 00:00:00"
Idate = "12/3/2024 12:00:00 AM"
Edate = "12/2/2025 12:00:00 AM"
locations_per_split =10000
accgrp_id = 1


# Global list to store newly created database names
new_databases = []
created_databases=[]

def count_rows_in_dataframe(df):
    return len(df)


perilno = 1



def get_columns_to_unpivot(perilno):
    if perilno == 1:
        return ["EQCV1VAL", "EQCV2VAL", "EQCV3VAL"]
    elif perilno == 2:
        return ["WSCV1VAL", "WSCV2VAL", "WSCV3VAL"]    



# Get columns to unpivot based on perilno
columns_to_unpivot = get_columns_to_unpivot(perilno)

# Define id_vars
id_vars = [col for col in df.columns if col not in columns_to_unpivot]

# Unpivot the DataFrame
unpivoted_df = df.unpivot(
    index=id_vars,
    on=columns_to_unpivot,
    variable_name="losstype",
    value_name="Value"
)

# Map losstype to corresponding values
columns_to_unpivot_mapping = {
    "EQCV1VAL": 1,
    "EQCV2VAL": 2,
    "EQCV3VAL": 3,
    
}

unpivoted_df = unpivoted_df.with_columns([
    pl.col("losstype").map_elements(lambda x: columns_to_unpivot_mapping.get(x, None), return_dtype=pl.Int32).alias("losstype")
])

# Add COVGMODE column
unpivoted_df = unpivoted_df.with_columns([
    pl.when(pl.col("losstype") == 2).then(3).otherwise(0).alias("COVGMODE")
])

# Define the mapping from losstype to labelid
losstype_to_labelid = {
    1: 7,
    2: 8,
    3: 9
}

# Add the labelid column based on the losstype column
unpivoted_df = unpivoted_df.with_columns([
    pl.col("losstype").map_elements(lambda x: losstype_to_labelid.get(x, None), return_dtype=pl.Int32).alias("labelid")
])

# Sort the DataFrame by LOCNUM
unpivoted_df = unpivoted_df.sort("LOCNUM")

# Add the LOCCVGID column
unpivoted_df = unpivoted_df.with_columns([
    pl.arange(1, unpivoted_df.height + 1).alias("LOCCVGID")
])


# In[294]:

import pyodbc






#########################################################################################################3




# Class to manage SQL connection
class SQLConnection:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.connection = None
        self.cursor = None

    def open(self):
        # Open a persistent connection
        self.connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            "Trusted_Connection=yes;"
        )
        self.cursor = self.connection.cursor()

    def execute(self, sql_command):
        try:
            self.cursor.execute(sql_command)
            self.connection.commit()
        except Exception as e:
            print(f"Error executing command: {sql_command}\nException: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Function to get table columns
def get_table_columns(cursor, table_name):
    try:
        sql_command = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching columns for table {table_name}: {e}")
        return []

# Function to get foreign key columns
def get_foreign_key_columns(cursor, table_name):
    try:
        sql_command = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_NAME = '{table_name}' AND CONSTRAINT_NAME IN (
            SELECT CONSTRAINT_NAME 
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
        )
        """
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching foreign key columns for table {table_name}: {e}")
        return []

# Server details
server = "localhost"

table_mappings = {
    "loccvg": {"VALUEAMT":"Value","LOSSTYPE":"losstype","LABELID":"labelid","LOCID":"LOCNUM"},
    

}

# Define behavior for unspecified columns
unspecified_column_behavior = {
    "loccvg": {
        "default": "0",  # General default value for unspecified columns
        "null_columns": [],  # Columns where value should be NULL
        "blank_columns": [], # Columns where value should be a blank space
        "zero_columns": [],  # Columns where value should be 0
        "specific_defaults":  {
        "ISVALID": "1",
        "PERIL": perilno,
        "LIMITCUR": currency,
        "DEDUCTCUR": currency,
        "VALUECUR": currency,
        "NONRANKINGDEDUCTCUR": currency,
        "BIPOI":12
    }
    },
}

loccvg_id_counter = 1
#loc_id_counter_counter = 4


chunks = [unpivoted_df[i:i + (locations_per_split*3)] for i in range(0, len(unpivoted_df), (locations_per_split*3))]

# Open the SQL connection once
sql_conn = SQLConnection(server, created_databases[0])
sql_conn.open()

# Populate each chunk into the corresponding database
for i, chunk in enumerate(chunks):
    if i < len(created_databases):
        database = created_databases[i]
        print(f"Populating database: {database}")

        # Switch database if necessary
        if sql_conn.database != database:
            sql_conn.close()
            sql_conn = SQLConnection(server, database)
            sql_conn.open()

        # Delete existing rows from the Property table
        sql_conn.execute("DELETE FROM loccvg ")
        print(f"All rows deleted from loccvg table in database {database}.")

        for row in chunk.iter_rows(named=True):
            for table_name, column_mapping in table_mappings.items():
                all_columns = get_table_columns(sql_conn.cursor, table_name)
                foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, table_name)

                mapped_columns = []
                mapped_values = []

                # Add mapped columns from the DataFrame
                for table_col in all_columns:
                    if table_col == 'LOCCVGID':
                        mapped_columns.append(table_col)
                        mapped_values.append(f"{loccvg_id_counter}")
                    # elif table_col == 'LOCID':
                    #     mapped_columns.append(table_col)
                    #     mapped_values.append(f"{loc_id_counter_counter}")
                    
                    elif table_col in column_mapping:
                        df_col = column_mapping[table_col]
                        if df_col in row:
                            mapped_columns.append(table_col)
                            mapped_values.append(f"'{row[df_col]}'")
                    elif table_col not in foreign_key_columns:
                        default_behavior = unspecified_column_behavior.get(table_name, {"default": "0"})
                        null_columns = default_behavior.get("null_columns", [])
                        blank_columns = default_behavior.get("blank_columns", [])
                        zero_columns = default_behavior.get("zero_columns", [])
                        specific_defaults = default_behavior.get("specific_defaults", {})

                        if table_col in null_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("NULL")
                        elif table_col in blank_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("' '")
                        elif table_col in zero_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("0")
                        elif table_col in specific_defaults:
                            mapped_columns.append(table_col)
                            mapped_values.append(f"'{specific_defaults[table_col]}'")
                        else:
                            mapped_columns.append(table_col)
                            mapped_values.append(default_behavior.get("default", "0"))

                # Increment the loccvg id counter counters
                if 'LOCCVGID' in mapped_columns:
                    loccvg_id_counter += 1
                # if 'LOCID' in mapped_columns:
                #     loc_id_counter_counter += 1
                
                

                # Ensure the number of columns matches the number of values
                if len(mapped_columns) == len(mapped_values):
                    sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                    sql_conn.execute(sql_command)
                else:
                    print("Mismatch in columns and values, skipping row.")

sql_conn.close()

print("Data population completed in loccvg table.")






# Class to manage SQL connection
class SQLConnection:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.connection = None
        self.cursor = None

    def open(self):
        # Open a persistent connection
        self.connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            "Trusted_Connection=yes;"
        )
        self.cursor = self.connection.cursor()

    def execute(self, sql_command):
        try:
            self.cursor.execute(sql_command)
            self.connection.commit()
        except Exception as e:
            print(f"Error executing command: {sql_command}\nException: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Function to get table columns
def get_table_columns(cursor, table_name):
    try:
        sql_command = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching columns for table {table_name}: {e}")
        return []

# Function to get foreign key columns
def get_foreign_key_columns(cursor, table_name):
    try:
        sql_command = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_NAME = '{table_name}' AND CONSTRAINT_NAME IN (
            SELECT CONSTRAINT_NAME 
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
        )
        """
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching foreign key columns for table {table_name}: {e}")
        return []

server = "localhost"



if perilno==1:

    table_mappings = {
        "eqdet": {"EQDETID":"LOCNUM","LOCID": "LOCNUM"}
    }

    # Define behavior for unspecified columns
    unspecified_column_behavior = {
        "eqdet": {
            "default": "0",  # General default value for unspecified columns
            "null_columns": [],  # Columns where value should be NULL
            "blank_columns": ['MMI_VERSION', "RMSCLASS", "ISOCLASS", "ATCCLASS", "FIRECLASS", "USERCLASS",
                            "ATCOCC", "SICOCC", "ISOOCC", "IBCOCC", "USEROCC"],  # Columns where value should be a blank space
            "zero_columns": [],  # Columns where value should be 0
            "specific_defaults": {  # Columns with specific default values
                "PCNTCOMPLT": "100", "NONRANKINGSITEDEDCUR": currency, "NONRANKINGCOMBINEDDEDCUR": currency, "SITEDEDCUR": currency, "ISVALID": "1", "DI": "-1", "STARTDATE": undate, "YEARUPGRAD": undate, "YEARSPNKLR": undate, "COMPDATE": undate,
                "COMBINEDLIMCUR": currency, "COMBINEDDEDCUR": currency, "SITELIMCUR": currency,
            },
        },
    }

    eqdet_counter = 1
    loc_id_counter = 1 

    # Split the DataFrame into chunks
    chunks = [df[i:i + locations_per_split] for i in range(0, len(df), locations_per_split)]

    # Open the SQL connection once
    sql_conn = SQLConnection(server, created_databases[0])
    sql_conn.open()

    # Populate each chunk into the corresponding database
    for i, chunk in enumerate(chunks):
        if i < len(created_databases):
            database = created_databases[i]
            print(f"Populating database: {database}")

            # Switch database if necessary
            if sql_conn.database != database:
                sql_conn.close()
                sql_conn = SQLConnection(server, database)
                sql_conn.open()

            # Delete existing rows from the Property table
            sql_conn.execute("DELETE FROM eqdet ")
            print(f"All rows deleted from eqdet table in database {database}.")

            for row in chunk.iter_rows(named=True):
                for table_name, column_mapping in table_mappings.items():
                    all_columns = get_table_columns(sql_conn.cursor, table_name)
                    foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, table_name)

                    mapped_columns = []
                    mapped_values = []

                    # Add mapped columns from the DataFrame
                    for table_col in all_columns:
                        if table_col == 'EQDETID':
                            mapped_columns.append(table_col)
                            mapped_values.append(f"{eqdet_counter}")
                        elif table_col == 'LOCID':
                            mapped_columns.append(table_col)
                            mapped_values.append(f"{loc_id_counter}")
                        
                        elif table_col in column_mapping:
                            df_col = column_mapping[table_col]
                            if df_col in row:
                                mapped_columns.append(table_col)
                                mapped_values.append(f"'{row[df_col]}'")
                        elif table_col not in foreign_key_columns:
                            default_behavior = unspecified_column_behavior.get(table_name, {"default": "0"})
                            null_columns = default_behavior.get("null_columns", [])
                            blank_columns = default_behavior.get("blank_columns", [])
                            zero_columns = default_behavior.get("zero_columns", [])
                            specific_defaults = default_behavior.get("specific_defaults", {})

                            if table_col in null_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("NULL")
                            elif table_col in blank_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("' '")
                            elif table_col in zero_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("0")
                            elif table_col in specific_defaults:
                                mapped_columns.append(table_col)
                                mapped_values.append(f"'{specific_defaults[table_col]}'")
                            else:
                                mapped_columns.append(table_col)
                                mapped_values.append(default_behavior.get("default", "0"))

                    # Increment the AddressID, LOCID, and PRIMARYLOCID counters
                    if 'EQDETID' in mapped_columns:
                        eqdet_counter += 1
                    if 'LOCID' in mapped_columns:
                        loc_id_counter += 1
                    

                    # Ensure the number of columns matches the number of values
                    if len(mapped_columns) == len(mapped_values):
                        sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                        sql_conn.execute(sql_command)
                    else:
                        print("Mismatch in columns and values, skipping row.")

    # Close the SQL connection after everything is completed
    sql_conn.close()

    print("Data population completed in eqdet table.")

else:
    print("PERIL NOT VALID")


