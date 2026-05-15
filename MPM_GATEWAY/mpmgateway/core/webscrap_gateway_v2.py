"""
This script contains class and all the methods that can achieve below functionality
1. connecting to a database
2. replicating a database 
3. connecting to a server via ssh tunneling with local port forwarding
4. To fetch data from a db and process the data using etl logic and store in the selected database.

Returns:
    log : store the log in the log file.
"""

import pyodbc
import pandas as pd
import numpy as np
import importlib.util
import logging
import time
import datetime
import paramiko, sys
from mpmgateway.helper.forward import forward_tunnel
from mpmgateway.helper.rforward import reverse_forward_tunnel
import threading
import re
import mpmgateway.helper.odbc_helper as helper
import os
import requests
from bs4 import BeautifulSoup,Comment
import  pandas as pd
import importlib.util

class Scrapper():
    """This class is used to connect to db (in localhost ,in servers via ssh tunneling) and process the 
    collected data and store it in a selected database
    """
    def __init__(self,config,etl_config = None):
        """This init function initialize all the variables from the config

        :param config: _description_
        :type config: _type_
        :param etl_config: _description_, defaults to None
        :type etl_config: _type_, optional
        """
        self.log_folder = config['log_file_path']
        self.scrapping_urls = config["scrapping"]["scrapping_urls"]
        self.user = None
        self.password = None
        if config["scrapping"]["cred"]:
            self.user = config["scrapping"]["cred"]["user"]
            self.password = config["scrapping"]["cred"]["password"]

        self.tunnel_thread = None
        self.is_tunnel_running = False
        # Database connection strings
        self.sdb_conn_str = ";".join([f"{key}={value}" for key, value in config['source_database']['cred'].items()])

        # source table
        self.source_table = config['source_database']['table_name']
        self.source_table_schema = config['source_database']['schema']
        self.columns_to_fetch = config['source_database']['columns_needed']
        self.filter_id = config['source_database']['filter_id']
        self.file_keyword = config['source_database']['file_keyword']


        # Database connection strings
        self.sdb_conn_str2 = ";".join([f"{key}={value}" for key, value in config['source_database']['cred'].items()])

        # source table
        self.source_table2 = config['source_database']['table_name']
        self.source_table_schema2 = config['source_database']['schema']
        self.columns_to_fetch2 = config['source_database']['columns_needed']
        self.filter_id2 = config['source_database']['filter_id']
        self.file_keyword2 = config['source_database']['file_keyword']


        self.sandman_id_logger_table_name = config['id_logger_sandman_table']['name']
        self.sandman_id_logger_schema = config['id_logger_sandman_table']['schema']
        self.sandman_id_logger_fk = config['id_logger_sandman_table']['fetch_id']

        # destination table
        self.ddb_conn_str = ";".join([f"{key}={value}" for key, value in config['destination_database']['cred'].items()])
        self.destination_table = config['destination_database']['table_name']
        self.destination_table_name = config['destination_database']['table_name']
        self.destination_table_schema = config['destination_database']['schema']
        self.ssh_connection = config['destination_database']["SSH"]["connection"]
        self.ssh_cred = config['destination_database']["SSH"]["cred"]
        # self.filter_id_dest = config['destination_database']['filter_id']
        self.etl = config['etl']
        self.etl_path = config['etl_file_path']
        self.etl_config = etl_config

        self.files_to_process = None
        self.data_freq = config["data_freq(in secs)"]

    def etl_process(self,etl_file_path, arg1):
        """it sends the data fetched through the database to the etl file for processing

        :param etl_file_path: etl file path
        :type etl_file_path: str
        :param arg1: data fetched from the database(source)
        :type arg1: pd.DataFrame
        :return: result (processed dataframe)
        :rtype: pd.DataFrame
        """
        function_name = 'etl'
        spec = importlib.util.spec_from_file_location('my_module', etl_file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        result = getattr(module, function_name)(arg1)
        return result
    
    def dict_to_create_query(self,table,schema):
        """It formulates create statement to be executed based on the table name and schema

        :param table: Table name
        :type table: str
        :param schema: Table schema
        :type schema: dict
        :return: Create statement with the given table name and schema
        :rtype: str
        """
        create_query = f"CREATE TABLE {table} ("
        for column_name, data_type in schema.items():
            create_query += f"`{column_name}` {data_type}, "
        create_query = create_query.rstrip(', ')
        create_query += ")"
        return create_query

    def ssh_connect(self):
        paramiko.util.log_to_file(self.log_filepath)
        paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)
        remote_host = self.ssh_cred["remote_host"]
        remote_port = self.ssh_cred["remote_port"]
        local_port  = self.ssh_cred["local_port"]
        ssh_host    = self.ssh_cred["ssh_host"]
        ssh_port    = self.ssh_cred["ssh_port"]
        ssh_key_path = self.ssh_cred["ssh_key_path"]
        rsa_key = paramiko.RSAKey(filename=ssh_key_path)


        self.transport = paramiko.Transport((ssh_host, ssh_port),disabled_algorithms = dict(pubkeys=["rsa-sha2-512", "rsa-sha2-256"]))
        self.transport.connect(hostkey  = None,
                        username = 'root',
                        pkey     = rsa_key)
        self.stop_event = threading.Event()

        def start_tunnel(stop_event):
            try:
                forward_tunnel(local_port, remote_host, remote_port, self.transport,stop_event)
            except Exception as e:
                print ('Port forwarding stopped.')
                print(e)

        self.tunnel_thread = threading.Thread(target=start_tunnel, args=(self.stop_event,))
        self.tunnel_thread.start()


    def check_tunnel(self):
        while True:
            if not self.is_tunnel_running:
                print('Tunnel is down. Restarting...')
                self.ssh_connect()
            else:
                break
    def connect_to_db(self,connection_string):
        """create connection to the database

        :param connection_string: connection string
        :type connection_string: str
        :return: connection object
        :rtype: pyodbc.Connection 
        """
        conn = pyodbc.connect(connection_string)
        return conn

    def table_exists(self,cursor, table_name):
        """checks if a table with the given table name exists

        :param cursor: cursor object of the needed database
        :type cursor: pyodbc.Cursor
        :param table_name: table name to be searched
        :type table_name: str
        :return: if True table exists else table doesn't exist
        :rtype: bool
        """
        try:
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            return cursor.fetchone() is not None    
        except Exception as e:
            logging.error("Error while checking if the table exists [table name : {table_name}] , error : {e}")
    
    def id_logger_create_query(self, table_name,schema_dict):
        """it create a table that stores the id or processed files that are processed.

        :param table_name: name of the table to be created
        :type table_name: str
        :param schema_dict: schema for the table to create
        :type schema_dict: dict
        :return: create statement
        :rtype: str
        """
        try:
            create_query = f"CREATE TABLE {table_name} ("
            for column_name, data_type in schema_dict.items():
                create_query += f"`{column_name}` {data_type}, "
            create_query = create_query.rstrip(', ')
            create_query += ")"
            logging.info(f"Created id logger create statement")
        except Exception as e:
            logging.error(f"Error while creating id logger create statement : {e}")

        return create_query
        # logging.info(f'created idlogger table : {table_name}')

    def create_table(self,cursor,create_statement):
        """creates the statement

        :param cursor: cursor object of the given database connection
        :type cursor: pyodbc.Cursor
        :param create_statement: create statement string
        :type create_statement: str
        """
        try:
            cursor.execute(create_statement)
        except Exception as e:
            logging.error(f"Table creation failed [query : {create_statement}] : {e}")
            raise e
        
    def fetch_last_id_from_idlogger(self):
        """fetch last id from the id logger

        :return: last_id, last_updated_time
        :rtype: str or int , datetime
        """
        try:
            self.storing_db_cursor.execute(f"SELECT * FROM {self.logger_id_table} ORDER BY processed_time DESC LIMIT 1")
            result = self.storing_db_cursor.fetchone()
        except Exception as e:
            logging.error("Error occured while fetching last id from id logger table : {e}")
        if result:
            id ,last_id, last_updated_time = result
            logging.info(f'Last id for filtering data is fetched : Last ID: {last_id}, Last Updated Time: {last_updated_time}')
            # pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{3,6})?'
            # if re.match(pattern, str(last_id)):
            #     last_id = f"'{last_id}'"                
            return last_id, last_updated_time
        else:
            return None, None

    def get_max_id(self):
        """Get the max id of the table

        :return: max id
        :rtype: str or int
        """
        return max(self.data[self.filter_id])
    
    def store_last_id_in_idlogger(self):
        """Store the last id in idlogger table
        """
        current_time = pd.Timestamp.now()
        logging.info(f'Storing Last ID: {self.last_id} at Time: {current_time}')
        if isinstance(self.last_id,datetime.datetime):
            self.last_id = self.last_id.strftime('%Y-%m-%d %H:%M:%S.%f')
        try:
            self.storing_db_cursor.execute(f"INSERT INTO {self.logger_id_table} (last_id) VALUES (?)", self.last_id )
            logging.info(f"successfully inserted the last id in the id logger table : last id {self.last_id}, id logger table {self.logger_id_table}")
        except Exception as e:
            logging.error(f"Error occured while logging the data in id logger table : {self.logger_id_table},error : {e}")

    def fetch_ids_need_processing(self):
        """it fetchs ids that need processing

        :return: list of files to process
        :rtype: list
        """
        try:
            self.fetching_db_cursor.execute(f"SELECT DISTINCT Date({self.filter_id}) FROM {self.source_table}")
            all_files = set(row.__getattribute__(f"Date({self.filter_id})") for row in self.fetching_db_cursor.fetchall())
        except Exception as e:
            logging.error(e)
            raise e
        
        try:
            self.fetching_db_cursor.execute(f"SELECT DISTINCT Date({self.sandman_id_logger_fk}) FROM {self.logger_id_table} WHERE current_iteration = false")
            files_processed = set(row.__getattribute__(f"Date({self.sandman_id_logger_fk})") for row in self.fetching_db_cursor.fetchall())
        except Exception as e:
            logging.error(e)
            raise e
        
        files_to_process = list(all_files - files_processed)
        
        self.current_day_file = []

        current_date = datetime.date.today()
        current_date = datetime.date(2023,10,31)
        prev_date = current_date - datetime.timedelta(days = 1)
        self.current_day_file = [current_date,prev_date]
        
        self.files_to_process = files_to_process

        logging.info(f"Date of files that need processing : {self.files_to_process}")
        logging.info(f"Date of files that are under current iteration : {self.current_day_file}")

        return files_to_process
    
    def extract_data_from_db_v2(self):
        """extracts data from the source database using list of ids that need processing

        :return:  data , column names
        :rtype: 2d array, list
        """
        columns_str = ', '.join(f"`{i}`"for i in self.columns_to_fetch)
        ids = self.fetch_ids_need_processing()
        if len(ids) == 0:
            logging.info(f'No new data to store in the destination table')
            return None,None
        else:
            fetch_query = f"SELECT {columns_str} FROM {self.source_table} WHERE {self.filter_id} IN ({', '.join(['?'] * len(ids))})"
            try:
                result = self.fetching_db_cursor.execute(fetch_query,list(ids))
                data = result.fetchall()
                if not data:
                    return None
                column_names = [desc[0] for desc in result.description]
                logging.info(f'Data Extracted from the source table')
                return data,column_names
            except Exception as e:
                logging.info(f'Error while extracting data : {e}')
                raise e


    def extract_data(self):
        """extracts data from the database based on either ids or timestamps

        :return: data fetched from the table in database
        :rtype: 2d array
        """
        # variable initialization
        self.data = None
        columns_str = '`,`'.join(self.columns_to_fetch)
        self.last_id,_ = self.fetch_last_id_from_idlogger()
        self.get_data()
        # if last_id:

        #     sql_query = f"SELECT `{columns_str}` FROM `{self.source_table}` WHERE `{self.filter_id}` > {last_id}"
        # else:
        #     sql_query = f"SELECT `{columns_str}` FROM `{self.source_table}`"
        try:
            data = self.data
            # result = self.fetching_db_cursor.execute(sql_query)
            # data = result.fetchall()
            if data.empty:
                logging.info("No new data to extract from the source table")
                return None
            logging.info("Data extracted from the source table")
            return data
        except Exception as e:
            logging.error(f"Error while extracting data from the source table : {e}")
            return None
    

    def delete_files(self,local_cursor):
        """it deletes two day data

        :param local_cursor: It is the cursor object of the selected database.
        :type local_cursor: pyodbc.Cursor
        """
        try:
            files_to_delete = self.files_to_process
            delete_query = f"DELETE FROM {self.storing_table} WHERE DATE({self.filter_id_dest}) IN ({', '.join(['?' for _ in range(len(files_to_delete))])})"
            local_cursor.execute(delete_query, files_to_delete)
            logging.info(f"Data deletion successfull : (step : deleting files that has same date as in the processed data)")
        except Exception as e:
            logging.error(f"Error while storing data (step : deleting files that has same date as in the processed data) : {e}")
            raise e

    def send_data_to_db_v2(self):
        """it sends the processed data or the replicated data to the database
        """
        try:
            df = self.data_processed
            # self.delete_files(self.storing_db_cursor)
            insert_query = f"INSERT INTO {self.storing_table} ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ("
            for i, row in enumerate(df.itertuples(index=False)):
                try:
                    # row = tuple(None if pd.isna(value) else value for value in row)

                    self.storing_db_cursor.execute(insert_query, row)
                except Exception as e:
                    logging.error(f"Error inserting row {i + 1} : {row}: {e}")
                    pass
            logging.info(f'Data inserted into destination DB')
        except Exception as e:
            logging.error(f"An error occurred while inserting data into destination: {e}")
            raise e
        
    def send_data_to_db(self):
        """it sends the processed data or the replicated data to the database
        """
        self.columns_to_fetch = self.data.columns
        insert_query = f"INSERT INTO {self.storing_table} ({', '.join([f'`{col}`' for col in self.columns_to_fetch])}) VALUES ({', '.join(['?'] * len(self.columns_to_fetch))})"
        try:
            for row in self.data.iterrows():
                self.storing_db_cursor.execute(insert_query,list(map(str,list(row[1]))))
            logging.info("stored the data in destination table successfully")
        except Exception as e:
            logging.error("Error while storing the data in the destination table")
        try:
            self.last_id = self.get_max_id()
            logging.info("Fetched the last id from the new data to be stored in the idlogger")
        except Exception as e:
            logging.error("Error occured while get max id form the new data")
        self.store_last_id_in_idlogger()




    def get_dataframe(self):
        """Transform the 2d array to dataframe 

        :return: dataframe
        :rtype: pd.DataFrame
        """
        df = pd.DataFrame(np.array(self.data),columns = self.column_names)
        return df

    def etl_process(self,etl_file_path, arg1,arg2):
        """it sends the data fetched through the database to the etl file for processing

        :param etl_file_path: etl file path
        :type etl_file_path: str
        :param arg1: data fetched from the database(source)
        :type arg1: pd.DataFrame
        :return: result (processed dataframe)
        :rtype: pd.DataFrame
        """
        function_name = 'etl'
        spec = importlib.util.spec_from_file_location('my_module', etl_file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        result = getattr(module, function_name)(arg1,arg2)
        return result

    def insert_processed_file_record_new(self,local_cursor, processed_file_table):
        """insert the processed file name in the sandman id logger

        :param local_cursor: cursor of the selected database connection
        :type local_cursor: pyodbc.Cursor
        :param processed_file_table: dataframe that contains the processed files
        :type processed_file_table: pd.DataFrame
        """
        try:
            df = processed_file_table
            insert_query = f"INSERT INTO {self.sandman_id_logger_table_name} ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({', '.join(['?'] * len(df.columns))})"
            update_query = f"UPDATE {self.sandman_id_logger_table_name} SET processed_time = CURRENT_TIMESTAMP, current_iteration = ? WHERE date_id = ?"
            for i, row in enumerate(df.itertuples(index=False)):
                try:
                    local_cursor.execute(f"SELECT * FROM {self.sandman_id_logger_table_name} WHERE date_id = ?", (row.date_id,))
                    existing_record = local_cursor.fetchone()
                    if existing_record:
                        query = update_query
                        local_cursor.execute(query, (row.current_iteration,row.date_id,))
                    else:
                        query = insert_query
                        local_cursor.execute(query, row)
                except Exception as e:
                    logging.error(f"Error inserting row {i + 1}: {e}")
                    pass
            logging.info(f"Logged the dates belonging to the processed data")
        except Exception as e:
            logging.error(f"An error occurred while inserting a record into the {self.processed_files_table_name} table: {e}")

    def log_processed_files(self):
        """it convert the list of data processed into dataframe
        """
        df = pd.DataFrame([],columns = ['date_id','current_iteration'])
        df['date_id'] = pd.Series(self.files_to_process)
        df.loc[df['date_id'].isin(self.current_day_file),'current_iteration'] = True
        df['current_iteration'] = df['current_iteration'].fillna(False)
        local_cursor = self.fetching_db_cursor
        if not df.empty:
            self.insert_processed_file_record_new(local_cursor,df)
        else:
            print('No files to log in the id logger table')

    def etl_server_to_sandman(self):
        """it contains steps to read data from a database and process the data and sends it to the sandman server
        """
        logging.info('-'*100)
        logging.info('Gateway via odbc - started')
        start_time = time.time()
        try:
            # connection creation (pyodbc.Connection)
            try:
                ddb_conn = self.connect_to_db(self.ddb_conn_str)
                logging.info(f"Connection successfull destination database : {self.ddb_conn_str}")
            except Exception as e:
                logging.error(e)
                raise e
            
            try:
                sdb_conn = self.connect_to_db(self.sdb_conn_str)
                logging.info(f"Connection successfull source database : {self.sdb_conn_str}")
            except Exception as e:
                logging.error(e)
                raise e
    
            # cursor object creation
            ddb_cursor = ddb_conn.cursor()
            sdb_cursor = sdb_conn.cursor()

            # common variable initialization
            self.storing_table = self.destination_table_name
            self.logger_id_table = self.sandman_id_logger_table_name
            self.logger_id_schema = self.sandman_id_logger_schema
            self.storing_db_cursor = ddb_cursor
            self.storing_db_cursor_id = sdb_cursor
            self.fetching_db_cursor = sdb_cursor

            # creating db in local/destination if not exists
            if not self.table_exists(self.storing_db_cursor_id,self.logger_id_table):
                create_query_for_id_logger = self.id_logger_create_query(self.logger_id_table,self.logger_id_schema)
                self.create_table(self.storing_db_cursor_id,create_query_for_id_logger)
                logging.info(f'created id_logger table [for keeping track of the data send to sandman server] : {self.logger_id_table}')

            if not self.table_exists(self.storing_db_cursor,self.storing_table):
                create_query_for_duplicate_db = self.dict_to_create_query(self.storing_table,self.destination_table_schema)
                self.create_table(self.storing_db_cursor,create_query_for_duplicate_db)
                logging.info(f'created table [destination table in sandman server] : {self.storing_table}')

            # extract data from the source database
            self.extract_data()

            # processing and sending data to the server
            if self.data:

                self.send_data_to_db_v2()
            else:
                logging.info('No data to process and store in the destination database')

            # storing the processed files's dates in the id logger
            if self.files_to_process:
                try:
                    self.log_processed_files()
                except Exception as e:
                    logging.error(f'Error while storing the processed data details : {e}')
            else:
                logging.info('No data to log in the id logger table')

            ddb_conn.commit()
            sdb_conn.commit()

        except Exception as e:
            logging.error(e)
            raise e

        finally:
            end_time = time.time()
            logging.info(f'Gateway runtime: {end_time - start_time}')
            logging.info('-'*100)
            os._exit(0)

    def get_data(self):
        session = requests.Session()
        login_url =  self.scrapping_urls["login"]
        payload = {
            "cust_uname": self.user,
            "cust_upwd": self.password,  
            "submit1": "Login" 
        }
        login_response = session.post(login_url, data=payload)
        if login_response.ok:
            logging.info("Logged in successfully!")

            protected_url = self.scrapping_urls["data"]
            protected_response = session.get(protected_url)

            if protected_response.ok:
                soup = BeautifulSoup(protected_response.text, 'html.parser')
                self.data = self.etl_process(self.etl_config,soup,self.last_id)

    def scrapper_to_local(self):
        """contains steps to replicate data from one database to another database
        """
        # connection creation (pyodbc.Connection)
        while True:
            # Create a new log file path with the current date

            log_filename = datetime.datetime.now().strftime("%Y-%m-%d.log")
            log_filepath = os.path.join(self.log_folder, "web_scraping_v2_" + log_filename)

            # Ensure the log directory exists
            os.makedirs(os.path.dirname(log_filepath), exist_ok=True)

            # Set up a new logger instance
            logger = logging.getLogger()
            logger.setLevel(logging.NOTSET)

            # Remove any existing handlers
            if logger.hasHandlers():
                logger.handlers.clear()

            # Set up file handler with the new log file path
            file_handler = logging.FileHandler(log_filepath)
            file_handler.setLevel(logging.NOTSET)
            formatter = logging.Formatter(
                "%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            logging.info('-' * 100)
            logging.info(f'Gateway via smb - started')
            try:
                start_time = time.time()

                if self.ssh_connection:
                    self.ssh_connect()

                try:
                    ddb_conn = self.connect_to_db(self.ddb_conn_str)
                    logging.info(f"Connection to destination database [connection string : {self.ddb_conn_str}] : successful")
                except Exception as e:
                    logging.error(f"Connection to destination database failed [connection string : {self.ddb_conn_str}] ,error : {e}")



                ddb_cursor = ddb_conn.cursor()
                

                # common variable initialization
                self.storing_table = self.destination_table_name

                # # idlogger table
                self.logger_id_table = self.sandman_id_logger_table_name
                self.logger_id_schema = self.sandman_id_logger_schema

                # db cursor
                self.storing_db_cursor = ddb_cursor

                # self.send_data_to_db()
                
                # creating db in local/destination if not exists
                if not self.table_exists(self.storing_db_cursor,self.logger_id_table):
                    create_query_for_mirror_id_logger = self.id_logger_create_query(self.logger_id_table,self.logger_id_schema)
                    self.create_table(self.storing_db_cursor,create_query_for_mirror_id_logger)
                    logging.info("Id logger table successfully created")
                if not self.table_exists(self.storing_db_cursor,self.storing_table):
                    try:
                        create_query_for_duplicate_db = self.dict_to_create_query(self.storing_table,self.destination_table_schema)
                        logging.info(f"Destination create statement is created successfully : [create statement : {create_query_for_duplicate_db}]")
                    except Exception as e:
                        logging.error(f"Destination table creation is unsuccessful : {e}")
                    self.create_table(self.storing_db_cursor,create_query_for_duplicate_db)
                    logging.info("Destination table successfully created")

                self.data_v2 = self.extract_data()
                if not self.data.empty:
                # updating data in local/destination
                    self.send_data_to_db()
                else:
                    logging.info("No data to update in the destination database")

                # commit the changes
                ddb_conn.commit()

            except Exception as e:
                logging.error(e)
            finally:
                ddb_cursor.close()
                ddb_conn.close()
                
                # sdb_cursor.close()
                # sdb_conn.close()
                if self.ssh_connection:
                    self.stop_event.set()
                    self.tunnel_thread.join()
                    self.transport.close()
                end_time = time.time()
                logging.info(f'Gateway runtime: {end_time - start_time}')
                logging.info('-'*100)
                time.sleep(self.data_freq)

                # os._exit(0)
        
if __name__ == "__main__":
    pass