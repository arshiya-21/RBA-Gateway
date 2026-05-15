"""
This script contains class and all the methods that can achieve the below functionality
1. connecting to plc that uses SLCDriver via Ethernet/ip protocol
2. storing the data in the database

:raises e: 
:return: log : store the log in the log file
:rtype: log file
"""
import snap7
from snap7.util import *
import pandas as pd
import pyodbc
import logging
import time
import random
from mpmgateway.core.odbc_gateway import ODBC_gateway 
import datetime
import os

class S7_protocol(ODBC_gateway):
    """This class uses the pycomm3 module to read data from the plc and store it in the local database


    :param odbc_gateway: class that have methods needed for the database connectivity
    :type odbc_gateway: class
    """
    def __init__(self,config,etl_config = None):

        """This methods declare all the attributes that are needed for the PLC connection and Database connection

        :param config: dictionary containing all the values needed to assign to these variables
        :type config: dict
        """
        self.PLC_IP_ADDRESS = config["PLC"]["cred"]["ip"]
        self.PLC_RACK = config["PLC"]["cred"]["rack"]
        self.PLC_SLOT = config["PLC"]["cred"]["slot"]
        self.driver = config["Database"]["cred"]["driver"]
        self.read_instructions = config["PLC"]["address_access"]['read']
        self.write_instructions = config["PLC"]["address_access"]['write']
        self.write_data = config["PLC"]['write_data']
        self.counter_value = None
        self.counter_address = None
        self.counter_name = None
        self.use_counter = None
        self.data_freq = config["PLC"]['data_reading_freq(in secs)']
        if self.use_counter:
            self.data_freq = 0

        self.log_folder =  config['log_path']

        self.db_connection_str = ";".join([f"{key}={value}" for key, value in config['Database']['cred'].items()])
        self.destination_table_name = config['Database']['table_name']
        self.destination_table_schema = config['Database']['schema']

    def write_slc_data(self,plc, address, value):
        """it writes data to the given address in the plc

        :param plc: plc object
        :type plc: class plc
        :param address: write address
        :type address: string
        :param value: value to be written
        :type value: int
        """
        try:
            plc.write((address, value))

        except Exception as e:
            print(f"Error writing to address {address}: {e}")

    def get_counter_value(self):
        # read the counter value from the local database and store it in self.counter_value
        pass

    def get_data(self):
        """it connects with plc and reads the data ,stores in the dictionary in self.data
        """
        # with SLCDriver(self.PLC_IP_ADDRESS, self.PLC_SLOT,port=self.PLC_PORT) as self.plc:


        data_dict = {}
        if self.write_data:
            pass
        else:
            self.instructions = self.read_instructions
        try:
            client = snap7.client.Client()
            plc_ip = self.PLC_IP_ADDRESS  # Replace with your PLC's IP address
            rack = self.PLC_RACK
            slot = self.PLC_SLOT
            client.connect(plc_ip, rack, slot)
            if client.get_connected():
                print("Connected to siemens PLC")
            else:
                print("Failed to connect to the PLC.")
            for instruction in self.instructions:
                if "value_to_write" in instruction.keys():
                    value_to_write = instruction["value_to_write"]
                    self.write_slc_data(self.plc, instruction["address"], value_to_write)
                    print(f"Data written to {instruction['address']}")
                else:
                    if self.instruction['storage'] == "DB":
                        DB_NUMBER = self.instruction["DB_no"]
                        START_ADDRESS = self.instruction["address"]
                        SIZE = self.instruction["size"]
                        data = client.read_area(snap7.type.Areas.DB, DB_NUMBER, START_ADDRESS, SIZE)

                    elif self.instruction['storage'] == "MK":
                        START_ADDRESS = self.instruction["address"]
                        SIZE = self.instruction["size"]
                        data = client.plc.read_area(snap7.type.Areas.MK, 0, START_ADDRESS, SIZE)

                    if self.instruction["type"] == "int":
                        data = get_int(data,0)
                    elif self.instruction["type"] == "real":
                        data = get_real(data,0)


                    data_dict[instruction['content']] = data
                    print(f"Data at {instruction['address']}: {data}")
            self.data = data_dict
        except Exception as e:
            print(f"Error: {e}")
        finally:
            print("Connection closed")

    def send_data_to_db(self):
        """it sends the processed data or the replicated data to the database
        """
        insert_query = f"INSERT INTO {self.storing_table} ({', '.join([f'`{col}`' for col in self.data.keys()])}) VALUES ({', '.join(['?'] * len(self.data.keys()))})"
        try:
            self.storing_db_cursor.execute(insert_query,list(map(str,list(self.data.values()))))
            logging.info("stored the data in destination table successfully")
        except Exception as e:
            logging.error("Error while storing the data in the destination table")

    def generate_data(self):
        """for testing purpose

        :return: data_dict
        :rtype: dict
        """
        data = ['WEIGHT OF CASTING', 'CONTENT OF INOCULANT', 'POURING TIME', 'SPECIFIC GRAVITY', 'WEIGHT OF INOCULATION', 'FLOW OF INOCULANT', 'NUMBER OF DOSING']
        result_dict = {}
        for entry in data:
            random_float = round(random.uniform(1.0, 100.0), 2)  # Adjust the range as needed
            result_dict[entry] = random_float

        return result_dict

###
    def plc_to_db(self):
        """This function orchestrates the entire data flow where it read the data from the plc and stores it in the local database
        """
        while True:
            
            log_filename = datetime.datetime.now().strftime("%Y-%m-%d.log")
            log_filepath = os.path.join(self.log_folder, "smb_connection_" + log_filename)

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
                # self.data = self.generate_data()
                self.get_data()
                if not self.data:
                    continue
                try:
                    ddb_conn = self.connect_to_db(self.db_connection_str)
                    logging.info(f"Connection to destination database [connection string : {self.db_connection_str}] : successful")
                except Exception as e:
                    logging.error(f"Connection to destination database failed [connection string : {self.db_connection_str}] ,error : {e}")

                ddb_cursor = ddb_conn.cursor()
                self.storing_table = self.destination_table_name
                self.storing_db_cursor = ddb_cursor

                if not self.table_exists(self.storing_db_cursor,self.storing_table):
                    try:
                        create_query_for_duplicate_db = self.dict_to_create_query(self.storing_table,self.destination_table_schema)
                        logging.info(f"Destination create statement is created successfully : [create statement : {create_query_for_duplicate_db}]")
                    except Exception as e:
                        logging.error(f"Destination table creation is unsuccessful : {e}")
                    self.create_table(self.storing_db_cursor,create_query_for_duplicate_db)
                    logging.info("Destination table successfully created")

                if self.data:
                    self.send_data_to_db()
                else:
                    logging.info("No data to update in the destination database")
                ddb_conn.commit()
            except Exception as e:
                logging.error(e)
                end_time = time.time()
                logging.info(f'Gateway runtime: {end_time - start_time}')
                logging.info('-'*100)
                print('error')
                print(e)
                break
            finally:
                end_time = time.time()
                logging.info(f'Gateway runtime: {end_time - start_time}')
                logging.info('-'*100)
                time.sleep(self.data_freq)
