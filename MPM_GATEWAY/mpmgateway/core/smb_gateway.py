"""
This script contains class and all the methods that can achieve below functionality
1. connecting to a file sharing folder using smb protocol
2. connecting to a database and storing the data in the database.

Returns:
    log : store the log in the log file.
"""

import subprocess
import os
import time
import pyodbc
import pandas as pd
import datetime
import warnings 
import logging
warnings.filterwarnings("ignore")
import mpmgateway.helper.smb_helper as helper

class gateway_smb():
    """This class is used to connect to the file sharing folder and read the data and store it in the database
    """
    def __init__(self,config):
        """This init function initialize all the variables from the config

        :param config: contains all the necessary parameters needed for the connection to file sharing and database
        :type config: dict
        """
        if not isinstance(config, dict):
            raise ValueError('Config must be provided in dictionary format.')

        log_folder =  config['log_file_path']
        log_filename = datetime.datetime.now().strftime("%Y-%m-%d.log")
        log_filepath = os.path.join(log_folder, "smb_connection_" + log_filename )

        logging.basicConfig(
            filename=log_filepath,
            level=logging.NOTSET,
            format="%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s",
        )

        self.smb_share = config['SMBShare']['smb_share']
        self.local_mount_point = config['SMBShare']['local_mount_point']
        self.sudo_password = config['SMBShare']['sudo_password']
        self.share_username = config['SMBShare']['share_username']
        self.share_password = config['SMBShare']['share_password']
        
        self.local_db_conn_str  = ";".join([f"{key}={value}" for key, value in config['storing_database']['cred'].items()])
        self.processed_files_table_name  = config['processed_files_table']
        self.file_type = config['file_details']['file_type']
        self.file_keyword = config['file_details']['file_keyword']
        self.header = config['file_details']['file_header']
        self.columns_needed = config['file_details']['columns_to_fetch']

        self.local_db_table = config['storing_database']['table_name']
        self.local_db_table_schema = config['storing_database']['schema']

        self.data_freq = config["data_freq(in secs)"]

    def is_mounted_linux(self):
        """This checks whether the given folder is mounted or not

        :return: bool,False -> not mounted, True -> mounted
        :rtype: boolean
        """
        with open("/proc/mounts", "r") as f:
            for line in f:
                if self.local_mount_point in line:
                    return True
        return False
    
    def is_mounted_win(self):
        """This checks whether the given folder is mounted or not

        :return: bool,False -> not mounted, True -> mounted
        :rtype: boolean
        """
        return os.path.exists(self.local_mount_point)
    

    def mount_folder_win(self):
        """This function if not mounted it mounts the folder to the specified location
        """
        print(f"Checking if the {self.local_mount_point} is mounted")
        if self.is_mounted_win():
            print(f"{self.local_mount_point} is already mounted.")
            print(os.listdir(self.local_mount_point))
        else:
            print(f"{self.local_mount_point} is not mounted and processing to mount the file sharing folder ....")
            try:
                if self.share_username == '' and self.share_password == '':
                    command = ["net", "use", self.local_mount_point, self.smb_share]
                else:
                    command = [
                        "net", "use", self.local_mount_point, self.smb_share,
                        f"/user:{self.share_username}", self.share_password
                    ]
                print(" ".join(command))
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                stdout, stderr = process.communicate()

                if process.returncode == 0:
                    print(f"Mounted {self.smb_share} to {self.local_mount_point}.")
                    print(stdout)
                else:
                    print(f"Failed to mount {self.smb_share} to {self.local_mount_point}.")
                    print(f"Error message: {stderr}")
                    raise Exception(stderr)
            except Exception as e:
                print(f"Exception occurred: {str(e)}")
                raise



    def mount_folder_linux(self):
        """This function if not mounted it mounts the folder to the specified location
        """
        logging.info(f"checking if the {self.local_mount_point} is mounted")
        if self.is_mounted_linux():
            logging.info(f"{self.local_mount_point} is already mounted.")
        else:
            logging.info(f"{self.local_mount_point} is not mounted and processing to mount the file sharing folder ....")
            if self.share_username == '' and self.share_password == '':
                process = subprocess.Popen(
                ["sudo", "-S", "mount.cifs", self.smb_share, self.local_mount_point],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,  
            )
            else:
                process = subprocess.Popen(
                    ["sudo", "-S", "mount.cifs", self.smb_share, self.local_mount_point, "-o", f"username={self.share_username},password={self.share_password}"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,  
                )
            process.stdin.write(self.sudo_password + '\n')
            process.stdin.flush()
            stdout, stderr = process.communicate()
            logging.info(stdout)
            if process.returncode == 0:
                logging.info(f"Mounted {self.smb_share} to {self.local_mount_point}.")
            else:
                logging.error(f"Failed to mount {self.smb_share} to {self.local_mount_point}.")
                logging.error(f"Error message: {stderr}")
                raise stderr
        
    def connect_to_db(self,conn_str):
        """it connects to the database

        :param conn_str: connection string needed to connect to the database
        :type conn_str: string
        :return: connection object
        :rtype: class pyodbc
        """
        try:
            conn = pyodbc.connect(conn_str)
            return conn
        except Exception as e:
            logging.error(f"connection to database failed [connection string : {conn_str}] : {e}")
            raise e

    def does_table_exist(self,cursor, table_name):
        """it checks whether the given table exists or not in the specified database

        :param cursor: cursor object of the database
        :type cursor: pyodbc.cursor object
        :param table_name: Name of the table
        :type table_name: string
        :return: True -> if it exists ,False -> not exists
        :rtype: boolean
        """
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return cursor.fetchone() is not None

    def create_processed_files_table(self,cursor, table_name):
        """it creates logger table of the datastorage

        :param cursor: pyodbc cursor object
        :type cursor: Pyodbc.cursor
        :param table_name: table name of the cursor object
        :type table_name: string
        """
        try:
            create_query = f"CREATE TABLE {table_name} ("
            create_query += "`id` INT AUTO_INCREMENT PRIMARY KEY, "
            create_query += "`file_name` VARCHAR(255) NOT NULL, "
            create_query += "`ProdIndex` Int NOT NULL,"
            create_query += "`processed` VARCHAR(255) NOT NULL, "
            create_query += "`processed_datetime` DATETIME DEFAULT CURRENT_TIMESTAMP, "
            create_query += "`current_iteration` VARCHAR(255) NOT NULL"
            create_query += ")"
            cursor.execute(create_query)
            logging.info(f"Created id logger table name : {table_name}")
        except Exception as e:
            logging.error(f"An error occurred while creating the {table_name} table: {e}")
            raise e
        
    def get_files_not_processed(self,cursor):
        """it gets the files that are not processed yet or needs processing

        :param cursor: cursor object of the logger id table
        :type cursor: pyodbc.cursor
        """
        table_name = self.processed_files_table_name
        all_file_names = self.excel_files
        try:
            self.files_not_processed = []
            for file_name in all_file_names:
                cursor.execute(f"SELECT * FROM {table_name} WHERE file_name = ? AND current_iteration = False", (file_name,))
                if cursor.fetchone() is None:
                    self.files_not_processed.append(file_name)
            if self.currently_iterated_files:
                self.files_not_processed.extend(self.currently_iterated_files)
            cursor.execute(f"SELECT ProdIndex FROM {table_name} ORDER BY id DESC LIMIT 1;")
            self.last_prod_index =  cursor.fetchone()[0]
            logging.info(f"Extracted the files names that needs to be processed : {self.files_not_processed}")
            logging.info(f"Extracted the files names that are under current iteration : {self.currently_iterated_files}")
            logging.info(f"Extracted the Last ProdIndex that needs to be processed : {self.last_prod_index}")

        except Exception as e:
            logging.error(f"An error occurred while checking if files have been processed: {e}")
            raise e

    def list_excel_files(self):
        """This function stored the all the excel file names in the mounted folder in self.excel_files and 
        stores if the current day and previous day file exists in the folder in the self.currently_iterated_files
        """

        self.excel_files = []
        current_date = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
        # current_date = datetime.datetime(2023,10,30)
        prev_date = current_date - datetime.timedelta(days = 1)

        self.currently_iterated_files = []
        try:
            for root, dirs, files in os.walk(self.local_mount_point):
                self.root = root
                for file in files:
                    if file.endswith("."+self.file_type) and self.file_keyword in file:
                        date = helper.file_name_logic(file,self.file_keyword)
                        if date in [current_date]:
                            self.currently_iterated_files.append(file)
                            # self.root = root

                            continue
                        self.excel_files.append(file)
            logging.info(f"File names are read successfully from the mounted folders {self.local_mount_point}")
        except Exception as e:
            logging.error(f"Failed to read the file names in the mounted folder {self.local_mount_point} : {e}")
            raise e

    def get_files_to_process(self,local_cursor):
        """This function does a 2 step process to extract the file names that needs to be processed
        Step 1: By calling the function list_excel_files it extracts all the file names that has the specified key word.
        Step 2: By calling the function get_files_not_processed it filters the file names that are not processed yet and are under current iteration.
                It store the files that need processing in self.files_not_processed

        :param local_cursor: _description_
        :type local_cursor: _type_
        """

        self.list_excel_files()
        if not self.does_table_exist(local_cursor, self.processed_files_table_name):
            self.create_processed_files_table(local_cursor, self.processed_files_table_name)
        self.get_files_not_processed(local_cursor)

    def read_data(self):
        """This read all the files in self.files_not_processed(it contains the files that are yet to be processed and current_iteration_files) and converts
        to a dataframe 
        NOTE : it also stores the files that can't be processed in self.files_not_able_to_process
        """
        df_list = []
        self.files_not_able_to_process = []
        self.final_df = None
        try:
            for file in self.files_not_processed:
                try:
                    if self.file_type == 'xlsx':
                        df = pd.read_excel(os.path.join(self.root,file),usecols=self.columns_needed,header = self.header)
                        df.columns = df.columns.str.strip()
                    elif self.file_type == 'csv':
                        df = pd.read_csv(os.path.join(self.root,file),usecols=self.columns_needed,header = self.header)
                        df.drop([0,1,2],inplace = True)
                        df= df[df['ProdIndex'].astype(int) > self.last_prod_index]
                        if not df.empty:
                            df['StartMonth'] = df['StartMonth'].astype(int)
                            df['StartDay'] = df['StartDay'].astype(int)
                            df['StartHour'] = df['StartHour'].astype(int)
                            df['StartMinute'] = df['StartMinute'].astype(int)

                            df['StartMonth'] = df['StartMonth'].astype(str).str.zfill(2).str.strip()
                            df['StartDay'] = df['StartDay'].astype(str).str.zfill(2).str.strip()
                            df['StartHour'] = df['StartHour'].astype(str).str.zfill(2).str.strip()
                            df['StartMinute'] = df['StartMinute'].astype(str).str.zfill(2).str.strip()

                            df['start_datetime'] = pd.to_datetime(df['StartYear'].astype(str).str.strip() + '-' +df['StartMonth'] + '-' +df['StartDay'] + ' ' +df['StartHour'] + ':' +df['StartMinute'],format='%Y-%m-%d %H:%M')
                            
                            df['StopMonth'] = df['StopMonth'].astype(int)
                            df['StopDay'] = df['StopDay'].astype(int)
                            df['StopHour'] = df['StopHour'].astype(int)
                            df['StopMinute'] = df['StopMinute'].astype(int)

                            df['StopMonth'] = df['StopMonth'].astype(str).str.zfill(2).str.strip()
                            df['StopDay'] = df['StopDay'].astype(str).str.zfill(2).str.strip()
                            df['StopHour'] = df['StopHour'].astype(str).str.zfill(2).str.strip()
                            df['StopMinute'] = df['StopMinute'].astype(str).str.zfill(2).str.strip()

                            df['Stop_datetime'] = pd.to_datetime(df['StopYear'].astype(str).str.strip() + '-' +df['StopMonth'] + '-' +df['StopDay'] + ' ' +df['StopHour'] + ':' +df['StopMinute'],format='%Y-%m-%d %H:%M')
                            
                            
                            # df['stop_datetime']=df[['StopYear', 'StopMonth', 'StopDay']].astype(int).apply(lambda x :x.str.strip().zfill(2)).agg('-'.join, axis=1) + " "+df[[ 'StopHour', 'StopMinute']].astype(int).apply(lambda x :x.str.strip().zfill(2)).agg(':'.join, axis=1)
                            
                            df.drop(columns = ["StartYear", "StartMonth", "StartDay","StartHour", "StartMinute", "StopYear", "StopMonth", "StopDay","StopHour", "StopMinute"],inplace = True)
                        else:
                            logging.info(f"No new data in the file [file_name : {file}]")
                            break
                    df['file_name'] = file
                    df['date_id'] = helper.file_name_logic(file,self.file_keyword)
                    self.last_id_prod = max(df['ProdIndex'].astype(int))
                    df_list.append(df)
                except Exception as e:
                    logging.error(f"Error while reading the data from the file [file_name : {file}] : {e}")
                    self.files_not_able_to_process.append(file)
            if len(df_list) != 0:
                self.final_df = pd.concat(df_list)
                logging.info('Data extracted successfully from the files that needs processing')
        except Exception as e:
            logging.error(f'Error while reading data : {e}')
            raise e


    def create_destination_table(self,cursor, table_name, schema_dict):
        """it creates the specified table

        :param cursor: cursor object of the specified database
        :type cursor: pyodbc cursor
        :param table_name: Name of the table in database
        :type table_name: string
        :param schema_dict: schema in the form of dictionary
        :type schema_dict: dict
        """
        try:
            create_query = f"CREATE TABLE {table_name} ("
            for column_name, data_type in schema_dict.items():
                create_query += f"`{column_name}` {data_type}, "
            create_query = create_query.rstrip(', ')
            create_query += ")"
            cursor.execute(create_query)
            cursor.commit()
            logging.info(f"Destination table created successfully : {table_name}")
        except Exception as e:
            logging.error(f"Error while creating destination table ({table_name}) [config : {schema_dict}]: {e}")
            raise e
    def delete_current_iteration_files(self,local_cursor):
        """it deletes the current iteration files related data for further updates. it is the part of the process

        :param local_cursor: cursor object of the specified table
        :type local_cursor: cursor object
        """
        try:
            files_to_delete = list(self.final_df['file_name'].unique())
            delete_query = f"DELETE FROM {self.local_db_table} WHERE file_name IN ({', '.join(['?' for _ in range(len(files_to_delete))])})"
            local_cursor.execute(delete_query, files_to_delete)
            logging.info(f"Successfull deletion of current iteration files : Files_deleted {files_to_delete}")
        except Exception as e:
            logging.error(f"Error while deleting current iteration files : {e}")
            raise e
    def store_required_data(self,local_cursor):
        """store the required data in the database specified

        :param local_cursor: cursor object
        :type local_cursor: pyodbc.cursor
        """
        if not self.does_table_exist(local_cursor, self.local_db_table):
            self.create_destination_table(local_cursor, self.local_db_table, self.local_db_table_schema)
        try:
            df = self.final_df
            self.delete_current_iteration_files(local_cursor)
            insert_query = f"INSERT INTO {self.local_db_table} ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({', '.join(['?'] * len(df.columns))})"
            for i, row in enumerate(df.itertuples(index=False)):
                try:
                    local_cursor.execute(insert_query, row)
                except Exception as e:
                    logging.error(f"Error inserting row {i + 1}: {row} ,error: {e}")
        except Exception as e:
            logging.error(f"An error occurred while inserting into destination: {e}")
            raise e

    def insert_processed_file_record_new(self,local_cursor, processed_file_table):
        """stores the file name in the logger id table

        :param local_cursor: cursor object of the specified database
        :type local_cursor: pyodbc.cursor object
        :param processed_file_table: the row that needs to be updated in the logger id table
        :type processed_file_table: pd.DataFrame
        """
        try:
            df = processed_file_table
            insert_query = f"INSERT INTO {self.processed_files_table_name} ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({', '.join(['?'] * len(df.columns))})"
            update_query = f"UPDATE {self.processed_files_table_name} SET processed = ?,current_iteration = ?, processed_datetime = CURRENT_TIMESTAMP WHERE file_name = ?"
            for i, row in enumerate(df.itertuples(index=False)):
                try:
                    local_cursor.execute(f"SELECT * FROM {self.processed_files_table_name} WHERE file_name = ?", (row.file_name,))
                    existing_record = local_cursor.fetchone()
                    if existing_record:
                        query = update_query
                        local_cursor.execute(query, (row.processed, row.current_iteration, row.file_name,))
                    else:
                        query = insert_query
                        local_cursor.execute(query, row)
                except Exception as e:
                    logging.error(f"Error inserting row {i + 1}: {e}")
                    raise e
            logging.info('Logging the processed files in the id logger : Successful')
        except Exception as e:
            logging.error(f"An error occurred while inserting a record into the {self.processed_files_table_name} table: {e}")
            raise e
        
    def log_processed_files(self,local_cursor):
        """it logs the processed files in the specified database

        :param local_cursor: cursor object of the specified logger table
        :type local_cursor: pyodbc.cursor object
        """
        df = pd.DataFrame([],columns = ['file_name','processed'])
        df['file_name'] = pd.Series(self.files_not_processed)
        df.loc[df['file_name'].isin(self.files_not_able_to_process),'processed'] = False
        df.loc[df['file_name'].isin(self.currently_iterated_files),'current_iteration'] = True
        df['processed'].fillna(True,inplace = True)
        df['current_iteration'] = df['current_iteration'].fillna(False)
        df['ProdIndex'] = self.last_id_prod

        if not df.empty:
            self.insert_processed_file_record_new(local_cursor,df)
        else:
            logging.info('No files to log in the id logger table')

    def start(self):
        """it does the entire data flow for filetransfer to the database. It connects with the file sharing folder and reads the data and stores in the database.
        """
        while True:
            logging.info('-'*100)
            logging.info(f'Gateway via smb - started')
            start_time = time.time()
            try:
                # mounting file folder that is been shared
                self.mount_folder_win()

                # connecting to the destination database
                local_conn = self.connect_to_db(self.local_db_conn_str)
                logging.info(f'connection to database successfull [connection_string : {self.local_db_conn_str}]')
                local_cursor = local_conn.cursor()

                # get the files that need processing
                self.get_files_to_process(local_cursor)

                # read the files that are selected for processing
                self.read_data()

                if isinstance(self.final_df,pd.DataFrame):
                    if not self.final_df.empty:
                        self.store_required_data(local_cursor)
                        self.log_processed_files(local_cursor)
                    else:
                        logging.info('No data to store in the destination database')
                        logging.info('No data to store in the id logger table')
                else:
                    logging.info('No data to store in the destination database')
                    logging.info('No data to store in the id logger table')
                local_cursor.commit()
            except Exception as e:
                logging.error(e)
                os._exit(0)
            finally:
                end_time = time.time()
                logging.info(f'Gateway runtime : {end_time - start_time}')
                logging.info(f'-'*100)
                time.sleep(self.data_freq)
