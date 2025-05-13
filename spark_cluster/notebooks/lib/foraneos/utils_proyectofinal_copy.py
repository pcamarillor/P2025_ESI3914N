import findspark
findspark.init()
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, FloatType, BooleanType, ShortType,LongType, MapType, ArrayType, TimestampType ,DateType
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQueryListener
from kafka import KafkaProducer
import time
from datetime import datetime, timezone
import random
import string
import os
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import ta  # Technical Analysis library: pip install ta
import copy
import threading
import logging

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
        
            
        
            
            
            
GLOBAL_LOCK = threading.Lock()

            
class Stock_Producer:
    def __init__(self, ticker: string, kafka_server: string, kafka_topic: string, publ_interval: float = 2.0, 
                 full_price_window: int = 5, close_price_window: int = 5, start_date: string = "2025-01-01", end_date: str = None):
        '''
            This class creates downloads the history of a given stock 
            , calculates based on this data a new price trajectory in time.
            This calculated price is always the close stock price.
            The prices are calculated using a Geometric Brownian Motion (GBM) model.
            based on this close price it calculates 4 prices on a coarser time scale:
            [Open, High, Low, Close]
            The class creates a kafka producer and sends the data to a given topic.
            The data is sent in a csv format with the following columns:
            date time , Stock-ID , Open , High, Low, Close
        
        Args:
            ticker (int, optional):             int or 'random', interval between log messages, default is 2 seconds
            kafka_server (str):                 Kafka server address
            kafka_topic (str):                  Kafka topic to publish to
            publ_interval (float, optional):    Interval between log messages in seconds, default is 2 seconds
            full_price_window (int, optional):  Resample interval for all prices in minutes, default is 5 minutes
            close_price_window (int, optional): Interval between two artificially generated close prices in seconds, default is 5 seconds
            start_date (str, optional):         Start date for stock download in 'YYYY-MM-DD' format, default is "2025-01-01"
            end_date (str, optional):           End date for stock download in 'YYYY-MM-DD' format, default is None (today)
        
        Attributes:
            PUBL_INTERVAL (float):           Interval between log messages in seconds
            TICKER (str):                    Stock ID
            KAFKA_TOPIC (str):               Kafka topic to publish to
            KAFKA_SERVER (str):              Kafka server address
            CLOSE_RPICE_WINDOW (int):        Interval between two artificially generated close prices in seconds
            FULL_PRICE_WINDOW (int):         Resample interval for all prices in minutes
            run_producer (bool):             Flag for active logging
            start_date (str):                Start date for stock download in 'YYYY-MM-DD' format
            end_date (str):                  End date for stock download in 'YYYY-MM-DD' format
            hist (list):                     Downloaded history of stock prices
            price_iterator (iter):           Iterator for calculated prices
            last (float):                    Last close price from previously simulated prices

        Methods:
            __get_historical_data():        Downloads historical close prices for a given ticker using yfinance
            __simulate_gbm_prices(r, sigma, n_periods, period_seconds): Simulates stock prices using the Geometric Brownian Motion (GBM) model
            __Download_Stock_Hist():        Initializes download of stock price history
            __simulate_prices(window_seconds=10, n_periods=50, r=0.01, sigma=0.2): Simulates future stock prices using GBM for each ticker
            __resample_and_aggregate(df, new_window): Processes simulated prices into OHLC format
            __init_producer():              Initializes a Kafka producer on a given server with a text serializer
            __start_producer():             Starts a loop that sends logging data to a Kafka topic at a specific interval
            start():                        Starts the Server, Producer and then the streaming loop
            close():                        Stops the streaming loop
                 
        '''
             
        self.PUBL_INTERVAL      = publ_interval     #how often to publish to kafka
        self.TICKER             = ticker            #id of stock
        self.KAFKA_TOPIC        = kafka_topic       #the topic where should be published
        self.KAFKA_SERVER       = kafka_server      #server where the stream runs
        self.CLOSE_RPICE_WINDOW = close_price_window #interval between two artificially generated close prices
        self.FULL_PRICE_WINDOW  = full_price_window  #resmaple interval for all prices in mins
        self.run_producer       = False             #bool for active logging 
        self.start_date         = start_date        #for stock download
        self.end_date           = end_date          #for stock download
        self.hist               = []                #downloaded history of stock
        self.price_iterator     = iter([])          #iterator for calculated prices
        self.last               = 0.0               #last close price from previously simulated prices
        self.k_producer         = None              #init variable for kafka producer
        self.msg_counter        = 0                 #to count the number of messages sent
        self.msg_counter_sink_path = r"/home/jovyan/notebooks/data/final_project_ParDeForaneos" + self.TICKER + "_msg_counter.txt"     #where to save the message counter




    def __get_historical_data(self):
        """
            Downloads historical close prices for a given ticker using yfinance.

            Returns:
                pd.DataFrame: DataFrame with "Close" prices.
        """
        
        # in case no end-date is given, use today
        if self.end_date is None:
            self.end_date = datetime.today().strftime("%Y-%m-%d")

        # use a global thread lock to prevent multiple threads from downloading data at the same time
        # which causes yfincance to fail and all threads get downloaded data from frist thread
        # this is a workaround for the yfinance bug
        with GLOBAL_LOCK:
            downloaded = yf.download(self.TICKER, start=self.start_date, end=self.end_date, progress=False, threads=True)
            return downloaded




    def __simulate_gbm_prices(self, r, sigma, n_periods, period_seconds):
        """
            Simulates stock prices using the Geometric Brownian Motion (GBM) model.
            The simulation is based on the last known price and generates a series of future prices.
            The function generates a list of simulated stock prices, including the initial price.
            The GBM model is defined by the following formula:
            S(t) = S(0) * exp((r - 0.5 * sigma^2) * t + sigma * sqrt(t) * Z)
            where:
                - S(t) is the stock price at time t
                - S(0) is the initial stock price
                - r is the risk-free interest rate
                - sigma is the volatility of the stock
                - Z is a standard normal random variable
                       
            Args:                          
                r (float):              Risk-free interest rate (annualized).
                sigma (float):          Volatility of the stock (annualized).
                n_periods (int):        Number of time steps to simulate.
                period_seconds (int):   Time window between each simulated price in seconds.

            Returns:
                List[float]:            Simulated stock prices including the initial price.
        """
        
        dt = period_seconds / (365 * 24 * 60 * 60)
        prices = [self.last]        #init start price

        for _ in range(n_periods):
            z = np.random.normal()      
            S_new = prices[-1] * np.exp((r - 0.5 * sigma ** 2) * dt + sigma * np.sqrt(dt) * z)  
            prices.append(S_new)    
    
        return prices




    def __download_stock_hist(self):
        '''
            Initializes download of stock price history.
            This method is called in the constructor to download the historical data for the given ticker.
            The downloaded data is stored in the hist attribute of the class.
        '''

        try:
            self.hist = self.__get_historical_data()
            print(f"Successfully downloaded historical data for {self.TICKER}")
        except Exception as e:
            print(f"Error downloading {self.TICKER}: {e}")
            exit()

        if self.hist.empty:
            print(f"No data for {self.TICKER}. Skipping.")
            exit()




    def __simulate_prices(self, window_seconds=10, n_periods=50, r=0.01, sigma=0.2):
        """
            A wrapper function that simulates future stock prices using the Geometric Brownian Motion (GBM) model.
            The function generates a DataFrame with timestamps as the index and the close prics of a ticker symbol in the column.
            The simulation is based on the last known price and generates a series of future prices.

            Args:
                window_seconds (int):       Time window in seconds between each simulated price.
                n_periods (int):            Number of future prices to simulate.
                r (float):                  Risk-free interest rate (annualized).
                sigma (float):              Volatility (annualized).

            Returns:
                pd.DataFrame:               DataFrame with timestamps as index and ticker columns.
        """
        
        now = datetime.now(timezone.utc)  # Get the current UTC time
        # Generate timestamps for the future prices based on given window and price interval
        times = [now + timedelta(seconds=i * window_seconds) for i in range(n_periods + 1)]
        # Simulate prices using GBM
        prices = self.__simulate_gbm_prices(r=r, sigma=sigma, n_periods=n_periods, period_seconds=window_seconds)
        # Create a DataFrame with the simulated prices and timestamps as index
        df_prices = pd.DataFrame({'price': prices, 'timestamp': pd.to_datetime(times)})
            
        return df_prices



    def __init_producer(self):
        '''
            Initializes a Kafka producer on a given server with a text serializer.
            This method is called in the constructor to set up the Kafka producer.
            The producer is used to send messages to the specified Kafka topic.
        '''
        
        try:
            self.k_producer = KafkaProducer(
                bootstrap_servers=self.KAFKA_SERVER,
                value_serializer=lambda v: v.encode('utf-8')  # serialize data as string --> fastest
            )
            print("Kafka producer created successfully.")
            
        except Exception as ex:
            print("Failed to create Kafka producer: {ex}")
            self.k_producer = None
            exit()
            
      
        #download stock history as basis for price simulation
        self.__download_stock_hist()
     
      
      

    def __start_producer(self):
        '''
            Starts a loop that sends logging data to a Kafka topic at a specific interval.
            This method is called in the start() method to begin the logging process.
            The loop generates new prices based on the historical data and sends them to the Kafka topic.
            It also logs the time taken to send messages and the number of messages sent and saves this information to a file.
        '''
        
        #interval at which simulated prices should be resampled to calculate OHLC prices
        
        #Create so many initial close prices that we can then publish 20 OHLC prices
        n_prices                = int(60/self.CLOSE_RPICE_WINDOW * self.FULL_PRICE_WINDOW  * 20)   #number of prices to create
        self.last               = self.hist["Close"].iloc[-1, 0]               #safe last price of history as next initial price
        starting_log_time       = datetime.now()                               #time reference for msg counter
        log_time_logger         = datetime.now()                               #time reference for publishing time
        price_iterator = iter([])                                              #init an iterator for easy price access
        
        #publish prices until stopped
        while self.run_producer:
            
            #get next prices, or produce new prices if exhausted
            try:
                current_prices = next(price_iterator)
            except:
                #simulat new prices with random interest rate and volatility
                next_close_price_trajectory     = self.__simulate_prices(self.CLOSE_RPICE_WINDOW, n_prices, round(random.uniform(0.005, 0.05), 3), round(random.uniform(0.4, 0.05), 3))
                #create an iterator and save last price
                price_iterator                  = iter(next_close_price_trajectory.iloc())
                self.last                       = next_close_price_trajectory["price"].iloc[-1]   #safe last simulated price as next init price
                current_prices                  = next(price_iterator)                            #get first price from iterator
                with open(self.msg_counter_sink_path, 'w') as f:
                    time_so_far = (datetime.now() - starting_log_time).total_seconds()
                    f.write(f"time:{time_so_far} , counter:{self.msg_counter}")
                    #print(f"\nTime so far: {time_so_far} seconds, Messages sent: {self.msg_counter} for Ticker {self.TICKER}")
            
            #create new message to publish with a new OHLC price
            #in format: date time , Stock-ID , Close
            log_data   =  "{},{},{}".format(current_prices['timestamp'], self.TICKER, 
                                    current_prices['price'])
                        
            #wait time if process was faster than defined log interval
            log_timediffer  = (datetime.now() - log_time_logger).total_seconds()
            if  log_timediffer < self.PUBL_INTERVAL:
                time.sleep(self.PUBL_INTERVAL - log_timediffer)
                
            self.k_producer.send(self.KAFKA_TOPIC, log_data)
            self.msg_counter += 1
            #print(log_data)
            log_time_logger =   datetime.now()


      

    def start(self):
        '''
            Starts the Server, Producer and then the streaming loop.
            This method is called to initiate the logging process.
            It sets the run_producer flag to True, initializes the producer, and starts the logging loop.
            During this process it counts the number of messages sent to Kafka.
        '''
        self.run_producer = True
        self.__init_producer()
        self.__start_producer()
        
        
        
        
    def close(self):
        '''
            Stops the streaming loop and closes the Kafka producer.
            This method is called to terminate the logging process.
            It sets the run_producer flag to False and closes the Kafka producer.
            The latest message counter is saved to a file for later reference.
        '''
        self.run_producer = False
        self.k_producer.close()
        with open(self.msg_counter_sink_path, 'w') as f:
            f.write(str(self.msg_counter))





def resample_and_aggregate(new_window: int=15 ):
    
    
    def start_resampling(df):
        """
        Processes simulated prices into OHLC format, computes indicators, and adds lag features.

        Args:
            df (pd.DataFrame): DataFrame with timestamps as index and ticker symbols as columns.
            new_window (str): Pandas-compatible resampling window (e.g., '5min', '15min').

        Returns:
            List[pd.DataFrame]: List of dataframes (one per ticker) with:
                - OHLC prices
                - 4 technical indicators: RSI, Williams %R, Ultimate Oscillator, EMA
                - 5 lagged close prices
        """
        
        try:
            resample_interval = str(new_window) + 'min'
                        
            #ticker = df["company"].iloc[0]
            # Create OHLC DataFrame directly from price data
            # try:
            #     df = df.drop('company', axis=1)
            # except:
            #     pass

            sim_prices_df = df.copy()
            sim_prices_df['timestamp'] = pd.to_datetime(sim_prices_df['timestamp'])
            sim_prices_df.set_index('timestamp', inplace=True)
            # Force float type to prevent dtype=object errors
            sim_prices_df['price'] = pd.to_numeric(sim_prices_df['close'], errors='coerce')
            sim_prices_df = sim_prices_df.dropna()
            # Ensure timestamp index is datetime
            
            ohlc_df = pd.DataFrame()
            
        
            # Resample to get OHLC
            ohlc_df['company'] = sim_prices_df['company'].resample(resample_interval).first()
            ohlc_df['open']     = sim_prices_df['price'].resample(resample_interval).first()
            ohlc_df['high']     = sim_prices_df['price'].resample(resample_interval).max()
            ohlc_df['low']      = sim_prices_df['price'].resample(resample_interval).min()
            ohlc_df['close']    = sim_prices_df['price'].resample(resample_interval).last()
            ohlc_df             = ohlc_df.dropna()
            #convert inaccesible index columnn into normal date-time column
            #and add new integer-based index column 
            ohlc_df             = ohlc_df.reset_index()
            return ohlc_df
        except Exception as e:
            #fail save for debugging
            return pd.DataFrame()   
        
        
    return start_resampling




def calc_techincal_indicators(df):

    
    try:
        ohlc_df = df.copy()
        # Technical indicators
        print("Calculating technical indicators...")
        ohlc_df['williams_r'] = ta.momentum.WilliamsRIndicator(
            high=ohlc_df['high'], low=ohlc_df['low'], close=ohlc_df['close']
        ).williams_r()

        ohlc_df['rsi'] = ta.momentum.RSIIndicator(close=ohlc_df['close'], window=6).rsi()
        ohlc_df['ultimate_osc'] = ta.momentum.UltimateOscillator(
            high=ohlc_df['high'], low=ohlc_df['low'], close=ohlc_df['close']
        ).ultimate_oscillator()
        ohlc_df['ema'] = ta.trend.EMAIndicator(close=ohlc_df['close']).ema_indicator()

        # Lag features
        for lag in range(1, 6):
            print(f"Creating lag feature for lag {lag}...")
            ohlc_df[f'close_lag_{lag}'] = ohlc_df['close'].shift(lag)


        # for the indicators all have NaN so this line would drop everything
        print(ohlc_df.head())
        print(ohlc_df.iloc[0])
        print("Dropping NaN values...")
        ohlc_df = ohlc_df.dropna()
        
        return ohlc_df
        #
        
    except:        
        #fail save for debugging
        if ohlc_df.empty:
            print("is empty")
            return pd.DataFrame([{
                "timestamp": pd.Timestamp.now(),
                "open": 0.0,
                "high": 0.0,
                "low": 0.0,
                "close": 0.0,
                "rsi": 0.0,
                "williams_r": 0.0,
                "ultimate_osc": 0.0,
                "ema": 0.0,
                "close_lag_1": 0.0,
                "close_lag_2": 0.0,
                "close_lag_3": 0.0,
                "close_lag_4": 0.0,
                "close_lag_5": 0.0,
                }])



