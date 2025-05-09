from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, FloatType, BooleanType, ShortType,LongType, MapType, ArrayType, TimestampType ,DateType
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQueryListener
from kafka import KafkaProducer
import time
from datetime import datetime
import random
import string
import os
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import ta  # Technical Analysis library: pip install ta



class SparkUtils:
    
    
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        '''
        method to create a correct spark dataframe schema taking a list of tuples of names and datatypes
        possible datatypes: string, double, int, float, long, bool, date, timestamp
                            array, map, struct, short
        
        Args:
            columns_info (list):             contains tuples like ("name","sting")
        Returns:
            StructType (object):             Spark dataframe StructType for given input 
        '''
    
    
        # defining dict to map simplified field types passed by user to actual spark types
        type_dict = {
                "string": StringType(),
                "double": DoubleType(),
                "integer": IntegerType(),
                "float": FloatType(),
                "bool": BooleanType(), 
                "date": DateType(),
                "timestamp": TimestampType(),
                "array": lambda element_type: ArrayType(element_type),
                "map": lambda key_type, value_type: MapType(key_type, value_type),
                "struct": lambda fields: StructType(fields),
                "long": LongType(),
                "short": ShortType()
            }
        

        #create list of different columns in dataframe
        schema_list = [ StructField(  tuple_arg[0], type_dict[tuple_arg[1]], nullable =True  ) for tuple_arg in columns_info ]
    
    
        return StructType(schema_list)
    
    
    
    @staticmethod
    def clean_df(df, correction_dict):
        '''
        Method to clean Null values from a df
        
        Args:
            df (DataFrame):         The PySpark DataFrame to clean     
            correction_dict (dict): Dictionary containing the relevant information as 
                                    - key = ColumnName
                                    - value = ReplaceValue
                                    
        Returns:
            corrected DataFrame   
        '''
        return df.fillna(correction_dict)
        
            
        
            
            
            
class Producers:
    
    def __init__(self, ticker: string, kafka_server: string, kafka_topic: string, publ_interval: float = 2.0, start_date: string = "2025-01-01", end_date: str = None):
        '''
        This class creates random log files from a server where the log messages contains
            date time , Topic , info1 , info2 , info3
        The user can set the interval between two log messages or specify 'random'. Not set is 2 seconds.
        And the user can specify how many log messages per log file should be written. Not set is 10 seconds. 
        
        Args:
            publ_interval (int, optional):               int or 'random', interval between log messages, default is 2 seconds

        Returns:
            
        '''
        
        self.run_producer   = False         #bool for active logging      
        self.publ_interval  = publ_interval      #how often to publish to kafka
        self.ticker         = ticker        #id of stock
        self.kafka_topic    = kafka_topic 
        self.kafka_server   = kafka_server
        self.start_date     = start_date    #for stock download
        self.end_date       = end_date      #for stock download
        self.hist           = None          #downloaded history of stock
        self.price_iterator = None          #iterator for calculated prices
        self.last           = None          #last close price from previously simulated prices
        
        
        
    def __get_historical_data(self):
        """
            Downloads historical close prices for a given ticker using yfinance.

            Args:
                ticker (str): Stock ticker symbol (e.g., "AAPL").
                start_date (str): Start date in "YYYY-MM-DD" format.
                end_date (str): End date in "YYYY-MM-DD" format. Defaults to today.

            Returns:
                pd.DataFrame: DataFrame with "Close" prices.
        """
        
        
        if self.end_date is None:
            self.end_date = datetime.today().strftime("%Y-%m-%d")

        return yf.download(self.ticker, start=self.start_date, end=self.end_date)[["Close"]]
    
    
    
    
    def __simulate_gbm_prices(self, r, sigma, n_periods, period_seconds):
        """
            Simulates stock prices using the Geometric Brownian Motion (GBM) model.

            Args:
                
                r (float):              Risk-free interest rate (annualized).
                sigma (float):          Volatility of the stock (annualized).
                n_periods (int):        Number of time steps to simulate.
                period_seconds (int):   Time window between each simulated price in seconds.

            Returns:
                List[float]: Simulated stock prices including the initial price.
        """
        
        dt = period_seconds / (365 * 24 * 60 * 60)
        prices = [self.last]        #init start price

        for _ in range(n_periods):
            z = np.random.normal()
            S_new = prices[-1] * np.exp((r - 0.5 * sigma ** 2) * dt + sigma * np.sqrt(dt) * z)
            prices.append(S_new)
    
        return prices
    
    
    
    
    
    def __Download_Stock_Hist(self):
        '''
            init download of stock price history
        '''
        
        try:
            self.hist = self.__get_historical_data()
        except Exception as e:
            print(f"Error downloading {self.ticker}: {e}")
            exit()

        if self.hist.empty:
            print(f"No data for {self.ticker}. Skipping.")
            exit()
            
            
        
        

    def __simulate_prices(self, window_seconds=10, n_periods=50, r=0.01, sigma=0.2):
        """
            Simulates future stock prices using GBM for each ticker.

            Args:
                
                window_seconds (int):       Time window in seconds between each simulated price.
                n_periods (int):            Number of future prices to simulate.
                r (float):                  Risk-free interest rate (annualized).
                sigma (float):              Volatility (annualized).

            Returns:
                pd.DataFrame:               DataFrame with timestamps as index and ticker columns.
        """
        

        now = datetime.now(datetime.timezone.utc)
        #df_final = pd.DataFrame()

        times = [now + timedelta(seconds=i * window_seconds) for i in range(n_periods + 1)]
        prices = self.__simulate_gbm_prices(r=r, sigma=sigma, n_periods=n_periods, period_seconds=window_seconds)

        df_prices = pd.DataFrame({'timestamps':pd.to_datetime(times), 'price': prices})
        #df_final = pd.concat([df_final, df], axis=1)
            
        return df_prices




    
    def __resample_and_aggregate(self, df ,new_window):
        """
           
            Processes simulated prices into OHLC format

            Args:
                df (pd.DataFrame):  DataFrame with timestamps as index and ticker symbols as columns.
                new_window (str):   Pandas-compatible resampling window (e.g., '5min', '15min').

            Returns:
                List[pd.DataFrame]: List of dataframes (one per ticker) with:
                    - OHLC prices
        """
        
        
        # Create OHLC DataFrame directly from price data
        sim_prices_df = df.copy()
        
        # Force float type to prevent dtype=object errors
        sim_prices_df['price'] = pd.to_numeric(sim_prices_df['price'], errors='coerce')
        sim_prices_df = ohlc_df.dropna()
    
        # Ensure timestamp index is datetime
        sim_prices_df.index = pd.to_datetime(sim_prices_df.index)
        
        ohlc_df = pd.DataFrame()
        
        # Resample to get OHLC
        ohlc_df['open']     = sim_prices_df['price'].resample(new_window).first()
        ohlc_df['high']     = sim_prices_df['price'].resample(new_window).max()
        ohlc_df['low']      = sim_prices_df['price'].resample(new_window).min()
        ohlc_df['close']    = sim_prices_df['price'].resample(new_window).last()
        ohlc_df             = ohlc_df.dropna()
        
        if ohlc_df.empty:
            print(f"Warning: No data for {self.ticker} after resampling.")
            exit()

        ohlc_df = ohlc_df.dropna()

        return ohlc_df




    
    def __init_producer(self):
        '''
            inits a kafka producer on a given server with a txt serializer
        '''
        
        try:
            self.k_producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: v.encode('utf-8')  # serialize data as string --> fastest
            )
            print("Kafka producer created successfully.")
            
            #download stock history as basis for price simulation
            self.__Download_Stock_Hist()
            
        except Exception as ex:
            print("Failed to create Kafka producer: {ex}")
            self.k_producer = None
        
        
        
        
    def __start_producer(self):
        '''
            Starts a loop that sends logging data to a kafka topic at a specific interval.
        '''
        
        #creates 50 prices in 10sec intervals with random interest rate and votality 
        close_price_window      = 5                             #interval between artificial close prices
        full_price_window       = 5                             #resmaple interval  for all prices in mins
        resample                = string(full_price_window) + 'min'
        #creates 20 price intervals
        n_prices                = 60/close_price_window * full_price_window  * 20   #number of prices to create
        self.last = self.hist["Close"].iloc[-1, 0]               #safe last price in history as next initial price
        log_time_logger         =   datetime.now()
        
        while self.run_producer:
            
            #get next prices, or produce new prices if exhausted
            try:
                current_prices = next(price_iterator)
            except:
                next_close_price_trajectory     = self.__simulate_prices(close_price_window, n_prices, round(random.uniform(0.005, 0.05), 3), round(random.uniform(0.4, 0.05), 3))
                next_prices_trajectory          = self.__resample_and_aggregate(next_close_price_trajectory, resample)
                next_close_price_trajectory.set_index('name', inplace=True)
                price_iterator                  = iter(next_prices_trajectory.iloc())
                self.last                       = next_prices_trajectory["Close"].iloc[-1, 0]   #safe last simulated price as next init price
            
            
            log_data   =  "{},{},{},{},{},{}".format(current_prices['timestamps'], self.ticker, 
                                    current_prices['open'], current_prices['high'], current_prices['low'], current_prices['close'])
            
            
            #wait time if faster than log interval
            log_timediffer  = datetime.now() - log_time_logger
            if  log_timediffer < self.publ_interval:
                time.sleep(self.publ_interval - log_timediffer)
                
            self.k_producer.send(self.kafka_topic, log_data)
            log_time_logger =   datetime.now()
    
    


        
        
    def start_logging(self, log_file_path):
        '''
            start the logging loop
        '''
        self.run_producer = True
        self.__init_producer()
        self.__start_producer(log_file_path)
        
        
        
        
    def stop_logging(self):
        '''
            stop the logging loop
        '''
        self.run_producer = False

