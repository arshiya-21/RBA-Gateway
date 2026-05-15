"""
This script contains class and all the methods that can achieve the below functionality
1. connecting to plc that uses SLCDriver via Ethernet/ip protocol
2. storing the data in the database

:raises e: 
:return: log : store the log in the log file
:rtype: log file
"""
from pycomm3 import SLCDriver,LogixDriver
import pandas as pd
import pyodbc
import logging
import time
import random
from mpmgateway.core.odbc_gateway import ODBC_gateway 
import datetime
import os
import requests
from bs4 import BeautifulSoup,Comment
import  pandas as pd
import importlib.util


class Scrapper(ODBC_gateway):
    """This class uses the pycomm3 module to read data from the plc and store it in the local database


    :param odbc_gateway: class that have methods needed for the database connectivity
    :type odbc_gateway: class
    """



    def __init__(self,config,etl_config = None):

        """This methods declare all the attributes that are needed for the web scrapping and Database connection

        :param config: dictionary containing all the values needed to assign to these variables
        :type config: dict
        """

        # scrapping details

        self.scrapping_urls = config["scrapping"]["scrapping_urls"]
        self.user = None
        self.password = None
        if config["scrapping"]["cred"]:
            self.user = config["scrapping"]["cred"]["user"]
            self.password = config["scrapping"]["cred"]["password"]

        self.log_folder =  config['log_path']
        self.log_filename = datetime.datetime.now().strftime("%Y-%m-%d.log")
        self.log_filepath = os.path.join(self.log_folder , "plc(Ethernet_ip)_connection_" + self.log_filename )
        self.etl_config = etl_config
        logging.basicConfig(
            filename=self.log_filepath,
            level=logging.DEBUG,
            format="%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s",
        )

        self.db_connection_str = ";".join([f"{key}={value}" for key, value in config['Database']['cred'].items()])
        self.destination_table_name = config['Database']['table_name']
        self.destination_table_schema = config['Database']['schema']

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

    def dict_to_datetime(self,date_dict):
        """Convert dictionary with date and time info to a datetime object."""
        try:
            return datetime(
                year=date_dict['Year'],
                month=date_dict['Month'],
                day=date_dict['Day'],
                hour=date_dict['Hour'],
                minute=date_dict['Minuttes'],  # Correcting the spelling here
                second=date_dict['Seconds'],
                microsecond=date_dict['MicroSeconds']
            )
        except KeyError as e:
            raise ValueError(f"Missing required field: {e}")
    def get_counter_value(self,plc):
        # Reading data
        if self.driver == "slc":
            data = plc._read_tag(self.counter_address)
            return data
        elif self.driver == "logix":
            tag_value = plc.read(self.counter_address).value
            self.counter_value = tag_value["CIM4"]["Productivity_last"]["ProductionStart"]
            self.counter_value = self.dict_to_datetime(self.counter_value)

            # process tag_value
            return data
    def get_previous_counter_from_db(self):
        """Retrieve the previous counter value from the database using pyodbc."""
        query = f"SELECT {self.counter_name} FROM {self.destination_table_name} order by desc limit 1"  # Modify the query as per your DB schema
        self.storing_db_cursor.execute(query)
        result = self.storing_db_cursor.fetchone()
        # get the result

        if result:
            return result[0] 
        else:
            return None 

    def connect_to_plc(self):
        """Establishes connection to the PLC based on the driver type (SLC or Logix)."""
        if self.driver == "slc":
            return SLCDriver(self.PLC_IP_ADDRESS, self.PLC_SLOT, port=self.PLC_PORT)
        elif self.driver == "logix":
            return LogixDriver(self.PLC_IP_ADDRESS, init_tags=True) 
        else:
            raise ValueError("Unsupported driver type. Use 'slc' or 'logix'.")

    def read_write_data(self, plc):
        """Reads and writes data to the PLC based on the instructions."""
        data_dict = {}
        instructions = self.write_instructions + self.read_instructions if self.write_data else self.read_instructions

        try:
            for instruction in instructions:
                if "value_to_write" in instruction.keys():
                    # Writing data
                    value_to_write = instruction["value_to_write"]
                    if self.driver == "slc":
                        self.write_slc_data(plc, instruction["address"], value_to_write)
                        print(f"Data written to {instruction['address']} (SLC)")
                    elif self.driver == "logix":
                        plc.write(instruction["address"], value_to_write)
                        print(f"Data written to {instruction['address']} (Logix)")

                else:
                    # Reading data
                    if self.driver == "slc":
                        data = plc._read_tag(instruction["address"])
                        data_dict[instruction['content']] = data[1]
                        print(f"Data at {instruction['address']} (SLC): {data}")
                    elif self.driver == "logix":
                        tag_value = plc.read(instruction["address"]).value
                        data_dict["Start_datetime"] = self.dict_to_datetime(tag_value["ProductionStart"])
                        data_dict["Stop_datetime"] = self.dict_to_datetime(tag_value["ProductionStop"])
                        data_dict["PatternNumber"] = tag_value["Info"]["PatternNo"]
                        data_dict["MoldsProduced"] = tag_value["Info"]["ProducedMolds"]

                        print(f"Data at {instruction['address']} (Logix): {tag_value}")

            self.data = data_dict
        except Exception as e:
            print(f"Error: {e}")
        finally:
            print("Connection closed")

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
                self.data = self.etl_process(self.etl_config,soup)


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
            try:
                logging.basicConfig(
                    filename=self.log_filepath,
                    level=logging.DEBUG,
                    format="%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s",
                )
                logging.info('-'*100)
                logging.info('Gateway via PLC to DB - started')
                # start_time = time.time()
                # # self.data = self.generate_data()
                # try:
                #     ddb_conn = self.connect_to_db(self.db_connection_str)
                #     logging.info(f"Connection to destination database [connection string : {self.db_connection_str}] : successful")
                # except Exception as e:
                #     logging.error(f"Connection to destination database failed [connection string : {self.db_connection_str}] ,error : {e}")
                # ddb_cursor = ddb_conn.cursor()
                # self.storing_table = self.destination_table_name
                # self.storing_db_cursor = ddb_cursor

                # if not self.table_exists(self.storing_db_cursor,self.storing_table):
                #     try:
                #         create_query_for_duplicate_db = self.dict_to_create_query(self.storing_table,self.destination_table_schema)
                #         logging.info(f"Destination create statement is created successfully : [create statement : {create_query_for_duplicate_db}]")
                #     except Exception as e:
                #         logging.error(f"Destination table creation is unsuccessful : {e}")
                #     self.create_table(self.storing_db_cursor,create_query_for_duplicate_db)
                #     logging.info("Destination table successfully created")

                self.get_data()

                # if self.data:
                #     self.send_data_to_db()
                # else:
                #     logging.info("No data to update in the destination database")
                # ddb_conn.commit()
            except Exception as e:
                logging.error(e)
            #     end_time = time.time()
            #     logging.info(f'Gateway runtime: {end_time - start_time}')
            #     logging.info('-'*100)
            #     print('error')
            #     print(e)
            #     break
            # finally:
            #     end_time = time.time()
            #     logging.info(f'Gateway runtime: {end_time - start_time}')
            #     logging.info('-'*100)
            #     time.sleep(self.data_freq)


