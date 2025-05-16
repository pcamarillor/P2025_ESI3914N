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
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import ta  # Technical Analysis library: pip install ta
import copy
import threading
import sys
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
from sklearn.metrics import f1_score
import optuna

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
            CLOSE_PRICE_INTERVAL (int):        Interval between two artificially generated close prices in seconds
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
        self.CLOSE_PRICE_INTERVAL = close_price_window #interval between two artificially generated close prices
        self.FULL_PRICE_WINDOW  = full_price_window  #resmaple interval for all prices in mins
        self.run_producer       = False             #bool for active logging 
        self.start_date         = start_date        #for stock download
        self.end_date           = end_date          #for stock download
        self.hist               = []                #downloaded history of stock
        self.price_iterator     = iter([])          #iterator for calculated prices
        self.last               = 0.0               #last close price from previously simulated prices
        self.k_producer         = None              #init variable for kafka producer
        self.msg_counter        = 0                 #to count the number of messages sent
        self.msg_counter_sink_path = r"/home/jovyan/notebooks/data/final_project_ParDeForaneos/" + self.TICKER + "_msg_counter.txt"     #where to save the message counter
        self.log_msg_size       = 0.0                 #size of message to be sent
        self.starting_log_time  = datetime.now()    #init time logger for message counter
        self.time_so_far        = 0.0               #time logger for message counter


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
        
        #Create so many initial close prices that we can then keep publishing prices for 40 mins
        n_prices                = int(60/self.PUBL_INTERVAL * 40)                 #number of prices to create
        self.last               = self.hist["Close"].iloc[-1, 0]               #safe last price of history as next initial price
        self.starting_log_time  = datetime.now()                               #time reference for msg counter
        log_time_logger         = datetime.now()                               #time reference for publishing time
        price_iterator = iter([])                                              #init an iterator for easy price access
        
        print(f"Starting producer for {self.TICKER} at time: {self.starting_log_time}")
        #publish prices until stopped
        while self.run_producer:
            
            #get next prices, or produce new prices if exhausted
            try:
                current_prices = next(price_iterator)
            except:
                #simulat new prices with random interest rate and volatility
                next_close_price_trajectory     = self.__simulate_prices(self.CLOSE_PRICE_INTERVAL, n_prices, round(random.uniform(0.005, 0.05), 3), round(random.uniform(0.4, 0.05), 3))
                #create an iterator and save last price
                price_iterator                  = iter(next_close_price_trajectory.iloc())
                self.last                       = next_close_price_trajectory["price"].iloc[-1]   #safe last simulated price as next init price
                current_prices                  = next(price_iterator)                            #get first price from iterator
                with open(self.msg_counter_sink_path, 'w') as f:
                    self.time_so_far = (datetime.now() - self.starting_log_time).total_seconds()
                    f.write(f"time:{self.time_so_far} ,ticker: {self.TICKER}, counter:{self.msg_counter}, size per log: {self.log_msg_size/1024} kb" )
                    #print(f"\nTime so far: {time_so_far} seconds, Messages sent: {self.msg_counter} for Ticker {self.TICKER}")
            
            #create new message to publish with a new OHLC price
            #in format: date time , Stock-ID , Close
            log_data   =  "{},{},{}".format(current_prices['timestamp'], self.TICKER, 
                                    current_prices['price'])
            self.log_msg_size = sys.getsizeof(log_data)  #size of message to be sent
                        
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
            self.time_so_far = (datetime.now() - self.starting_log_time).total_seconds()
            f.write(f"time:{self.time_so_far} ,ticker: {self.TICKER}, counter:{self.msg_counter}, size per log: {self.log_msg_size/1024} kb" )



def resample_and_aggregate(new_window: int=15 ):
    """
    Resamples and aggregates stock prices into OHLC format.
    This function is a wrapper for the start_resampling function to be used in kafka streaming

    Args:
        new_window (int, optional):     interval of resampling in minutes. Defaults to 15.
        
    Returns:
        Callable:                     Function to process the DataFrame and return OHLC prices.
    """
    
    def start_resampling(df):
        """
        Processes simulated prices into OHLC format

        Args:
            df (pd.DataFrame): DataFrame with timestamps as index and ticker symbols as columns.

        Returns:
            pd.DataFrame:   with Timestamp, Company and resampled OHLC prices
        """
        
        try:
            resample_interval = str(new_window) + 'min'
                        
            sim_prices_df = df.copy()
            #make sure timestamp is in datetime format
            sim_prices_df['timestamp'] = pd.to_datetime(sim_prices_df['timestamp'])
            #for resampling we need to set the timestamp as index
            sim_prices_df.set_index('timestamp', inplace=True)
            # Force float type to prevent dtype=object errors
            sim_prices_df['price'] = pd.to_numeric(sim_prices_df['close'], errors='coerce')
            sim_prices_df = sim_prices_df.dropna()
            
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
    '''
        Calculates technical indicators for a given DataFrame containing OHLC stock prices.
        The function computes the following indicators:
        - Williams %R
        - Ultimate Oscillator
        - Relative Strength Index (RSI)
        - Exponential Moving Average (EMA)
        - Lag features for the close price (1 to 5 lags)
        
        Args:
            df (pd.DataFrame): DataFrame with columns: 'timestamp', 'company', 'open', 'high', 'low', 'close'.
            
        Returns:
            pd.DataFrame: DataFrame with the same columns as input, plus the calculated indicators and lag features.
    '''

    
    try:
        ohlc_df = df.copy()
        ohlc_df.set_index('timestamp', inplace=True)
        # Technical indicators
        print("Calculating technical indicators...")

        ohlc_df['williams_r'] = ta.momentum.WilliamsRIndicator(
            high=ohlc_df['high'], low=ohlc_df['low'], close=ohlc_df['close']
        ).williams_r()
        

        ohlc_df['ultimate_osc'] = ta.momentum.UltimateOscillator(
            high=ohlc_df['high'], low=ohlc_df['low'], close=ohlc_df['close'], window1 = 3, window2= 6, window3 = 12
        ).ultimate_oscillator()
        
        ohlc_df['rsi'] = ta.momentum.RSIIndicator(close=ohlc_df['close']).rsi()
        
        ohlc_df['ema'] = ta.trend.EMAIndicator(close=ohlc_df['close']).ema_indicator()

        # Lag features
        print(f"Creating lag features...")
        for lag in range(1, 6):
            ohlc_df[f'close_lag_{lag}'] = ohlc_df['close'].shift(lag)


        # for the indicators all have NaN so this line would drop everything

        print("Dropping NaN values...")
        ohlc_df = ohlc_df.dropna()
        ohlc_df.reset_index(inplace=True)
        if ohlc_df.empty:
            print("ohlc_df is empty")
            return None
        else:
            return ohlc_df
        #
        
    except:        
        #fail save 
        if ohlc_df.empty:
            print("ohlc_df is empty")
            exit()



class ProgressListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        num_input_rows = event.progress.processedRowsPerSecond
        print(f"Query made progress: {num_input_rows} rows processed per second")
        
        # if num_input_rows >= 50:
        #     send_alert(f"High volume of data: {num_input_rows} rows")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")





def load_and_prepare_data(ticker, source='parquet', lookback_period_days=30, interval="5m"):
    """
    Load stock data either from parquet files or historical data and prepare it for modeling.
    
    Args:
        ticker (str): Stock ticker symbol
        source (str): 'parquet' or 'historical'
        lookback_period_days (int): Days of historical data to download if using 'historical' source
        interval (str): Data interval for historical data (e.g., '5m', '1d')
        
    Returns:
        pd.DataFrame: Prepared dataframe with technical indicators
    """
    if source == 'parquet':
        # Load from parquet files
        try:
            df = pd.read_parquet(f"/home/jovyan/notebooks/data/final_project_ParDeForaneos/final_indicator_df_{ticker}.parquet")
            print(f"Successfully loaded parquet data for {ticker}")
            
            # Ensure lowercase column names for consistency
            df.columns = [str(col).lower() for col in df.columns]
            
            # Calculate return for signal generation
            df['return'] = df['close'].pct_change().shift(-1)  # Next period return
            
            # Ensure timestamp is datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
            
            # Drop NaN values
            df = df.dropna()
            
            return df
            
        except Exception as e:
            print(f"Error loading parquet data for {ticker}: {e}")
            print("Falling back to historical data...")
            source = 'historical'
    
    if source == 'historical':
        try:
            # For 5-minute data, we need to be careful about the date range
            # Yahoo Finance limits how far back we can go for intraday data
            end_date = datetime.now()
            
            # Yahoo only provides about 60 days of 5-minute data
            if interval == "5m" and lookback_period_days > 59:
                print(f"Warning: Yahoo only provides ~60 days of 5-minute data. Limiting to 59 days.")
                lookback_period_days = 59
                
            start_date = end_date - timedelta(days=lookback_period_days)
            
            print(f"Downloading {ticker} data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
            # Download data
            data = yf.download(ticker, start=start_date.strftime('%Y-%m-%d'), 
                              end=end_date.strftime('%Y-%m-%d'), 
                              interval=interval,
                              progress=False)
            
            print(f"Downloaded data shape: {data.shape}")
            
            if data.empty:
                print(f"No data returned for {ticker}")
                return None
                
            # Create a clean DataFrame with proper column names
            df = pd.DataFrame()
            
            # Copy each relevant column with appropriate naming
            df['timestamp'] = data.index
            df['open'] = data.loc[:, 'Open'].values
            df['high'] = data.loc[:, 'High'].values
            df['low'] = data.loc[:, 'Low'].values
            df['close'] = data.loc[:, 'Close'].values
            df['volume'] = data.loc[:, 'Volume'].values
            df['company'] = ticker

            # Calculate technical indicators
            # Williams %R
            df['williams_r'] = ta.momentum.WilliamsRIndicator(
                high=df['high'], low=df['low'], close=df['close']
            ).williams_r()
            
            # Ultimate Oscillator
            df['ultimate_osc'] = ta.momentum.UltimateOscillator(
                high=df['high'], low=df['low'], close=df['close']
            ).ultimate_oscillator()
            
            # RSI
            df['rsi'] = ta.momentum.RSIIndicator(close=df['close']).rsi()
            
            # EMA
            df['ema'] = ta.trend.EMAIndicator(close=df['close']).ema_indicator()
            
            # Create lag features
            for lag in range(1, 6):
                df[f'close_lag_{lag}'] = df['close'].shift(lag)
                
            # Calculate return for signal generation
            df['return'] = df['close'].pct_change().shift(-1)  # Next period return
            
            # Drop NaN values
            df = df.dropna()
            
            print(f"Final prepared data shape: {df.shape}")
            return df
            
        except Exception as e:
            print(f"Error downloading historical data for {ticker}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    return None

def generate_signals(df, buy_threshold=0.015, sell_threshold=-0.015):
    """
    Generate BUY/SELL/WAIT signals based on future returns.
    
    Args:
        df (pd.DataFrame): Dataframe with a 'return' column
        buy_threshold (float): Minimum return threshold for BUY signal
        sell_threshold (float): Maximum return threshold for SELL signal
        
    Returns:
        pd.DataFrame: Original dataframe with a new 'Signal' column
    """
    df = df.copy()
    
    # Generate signals based on future returns
    df['Signal'] = 'WAIT'
    df.loc[df['return'] > buy_threshold, 'Signal'] = 'BUY'
    df.loc[df['return'] < sell_threshold, 'Signal'] = 'SELL'
    
    return df

class Model:
    """
    A Support Vector Machine model for trading signal prediction.
    
    Attributes:
        svm: The SVM classifier (SVC by default).
        ypred: Predicted target values.
        f1: F1 score of the model.
    """
    
    def __init__(self, svm=None, kernel="rbf", 
                 gamma="scale", weight="balanced", iter=10_000):
        """
        Initializes the SVM model with specified parameters.
        
        Args:
            svm: SVM classifier instance. Default is SVC().
            kernel: Kernel type. Default is rbf/sigmoid.
            gamma: Kernel coefficient. Default is 'scale'.
            weight: Class weight handling. Default is 'balanced'.
            iter: Maximum number of iterations. Default is 10,000.
        """
        if svm is None:
            self.svm = SVC()
        else:
            self.svm = svm
        self.svm.kernel = kernel
        self.svm.gamma = gamma
        self.svm.class_weight = weight
        self.svm.max_iter = iter
        self.ypred = None
        self.f1 = None
        
    def fit(self, X, y):
        """
        Trains the SVM model on the provided data.
        
        Args:
            X: Design matrix (features).
            y: Target values.
        """
        self.svm.fit(X, y)
        
    def predict(self, X):
        """
        Generates predictions using the trained model.
        
        Args:
            X: Input features for prediction.
        """
        self.ypred = self.svm.predict(X)
        
    def f1_score(self, test, pred, average="macro"):
        """
        Calculates the F1 score of the model's predictions.
        
        Args:
            test: True target values.
            pred: Predicted target values.
            average: F1 score averaging method. Default is 'macro'.
        """
        self.f1 = f1_score(test, pred, average=average)


class Backtest:
    """
    A backtesting engine for evaluating trading strategies.
    
    Attributes:
        capital: Initial investment capital. Default is $1,000,000.
        portfolio_value: List tracking portfolio value over time.
        active_long_pos: Dictionary with current long position details.
        active_short_pos: Dictionary with current short position details.
        n_shares: Number of shares traded per position. Default is 2,000.
        com: Commission rate per trade. Default is 0.125% (0.00125).
        stop_loss: Stop-loss threshold. Default is 10% (0.1).
        take_profit: Take-profit threshold. Default is 10% (0.1).
        calmar_ratio: Calculated Calmar ratio.
        sortino_ratio: Calculated Sortino ratio.
        sharpe_ratio: Calculated Sharpe ratio.
    """
    
    def __init__(self, capital=1_000_000, n_shares=2_000, 
                 com=0.125 / 100, stop_loss=0.1, 
                 take_profit=0.1):
        self.capital = capital
        self.portfolio_value = [capital]
        self.active_long_pos = None
        self.active_short_pos = None
        self.n_shares = n_shares
        self.com = com
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.calmar_ratio = 0
        self.sortino_ratio = 0
        self.sharpe_ratio = 0
        
    def calculate_portfolio(self, dataset, capital=1_000_000):
        """
        Simulates trading based on signals and tracks portfolio value.
        
        Args:
            dataset: DataFrame containing price data and trading signals.
            capital: Initial capital for simulation. Default is $1,000,000.
        """
        self.capital = capital
        self.active_long_pos = None
        self.active_short_pos = None
        self.portfolio_value = [self.capital]
        
        for i, row in dataset.iterrows():
            
            # Close active long position
            if self.active_long_pos:
                if row.close < self.active_long_pos['stop_loss'] or row.close > self.active_long_pos['take_profit']:
                    pnl = row.close * self.n_shares * (1 - self.com)
                    self.capital += pnl
                    self.active_long_pos = None
            
            # Close active short position
            if self.active_short_pos:
                if row.close > self.active_short_pos['stop_loss'] or row.close < self.active_short_pos['take_profit']:
                    cost = row.close * self.n_shares * (1 + self.com)
                    pnl = self.active_short_pos['entry'] * self.n_shares - cost
                    self.capital += pnl
                    self.active_short_pos = None
                
            # Open long position
            if row["Signal"] == 'BUY' and self.active_short_pos is None and self.active_long_pos is None:
                cost = row.close * self.n_shares * (1 + self.com)
                if self.capital > cost:
                    self.capital -= cost
                    self.active_long_pos = {
                        'datetime': row.name,
                        'opened_at': row.close,
                        'take_profit': row.close * (1 + self.take_profit),
                        'stop_loss': row.close * (1 - self.stop_loss)
                    }

            # Open short position
            if row["Signal"] == 'SELL' and self.active_short_pos is None and self.active_long_pos is None:
                cost = row.close * self.n_shares * (self.com)
                self.capital -= cost
                self.active_short_pos = {
                    'datetime': row.name,
                    'entry': row.close,
                    'take_profit': row.close * (1 - self.take_profit),
                    'stop_loss': row.close * (1 + self.stop_loss)
                }

            # Calculate current position value
            position_value = 0
            if self.active_long_pos:
                position_value = row.close * self.n_shares
            elif self.active_short_pos:
                position_value = self.active_short_pos['entry'] * self.n_shares - row.close * self.n_shares

            # Update total portfolio value
            self.portfolio_value.append(self.capital + position_value)

    def calculate_calmar(self, bars_per_year=19_656):
        """
        Calculates the Calmar ratio (CAGR / Max Drawdown).
        
        Args:
            bars_per_year: Number of trading periods in a year. 
                            Default is 19,656 (for 5-minute bars).
        """
        initial_val = self.portfolio_value[0]
        final_val = self.portfolio_value[-1]
        n_bars = len(self.portfolio_value)
        
        # CAGR calculation
        if initial_val <= 0 or final_val <= 0:
            return 0.0
        
        cagr = (final_val / initial_val) ** (1 / (n_bars/bars_per_year)) - 1
        
        # Max Drawdown calculation
        max_so_far = self.portfolio_value[0]
        max_drawdown = 0
        for pv in self.portfolio_value:
            if pv > max_so_far:
                max_so_far = pv
            drawdown = (max_so_far - pv) / max_so_far
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                
        if max_drawdown == 0:
            return cagr if cagr > 0 else 0.0
        
        self.calmar_ratio = cagr / max_drawdown
        
    def calculate_sharpe(self, bars_per_year=19_656, rfr=0.041):
        """
        Calculates the Sharpe ratio (excess return per unit of risk).
        
        Args:
            bars_per_year: Number of trading periods in a year. Default is 19,656 (for 5-minute bars).
            rfr: Risk-free rate. Default is 4.1% (0.041).
        """
        returns = pd.Series(self.portfolio_value).pct_change().dropna()
        excess_returns = returns - (rfr / bars_per_year)
        mean_excess_return = excess_returns.mean()
        std_return = returns.std()

        if std_return == 0:
            self.sharpe_ratio = 0.0
            return
        
        self.sharpe_ratio = (mean_excess_return / std_return) * np.sqrt(bars_per_year)
        
    def calculate_sortino(self, bars_per_year=19_656, rfr=0.041):
        """
        Calculates the Sortino ratio (excess return per unit of downside risk).
        
        Args:
            bars_per_year: Number of trading periods in a year. Default is 19,656 (for 5-minute bars).
            rfr: Risk-free rate. Default is 4.1% (0.041).
        """
        returns = pd.Series(self.portfolio_value).pct_change().dropna()
        excess_returns = returns - (rfr / bars_per_year)
        negative_returns = excess_returns[excess_returns < 0]
        mean_excess_return = excess_returns.mean()
        std_negative = negative_returns.std()

        if std_negative == 0:
            self.sortino_ratio = 0.0
            return    
        
        self.sortino_ratio = (mean_excess_return / std_negative) * np.sqrt(bars_per_year)

class StockModel:
    def __init__(self, ticker, data_source='parquet', lookback_period_days=730):
        self.ticker = ticker
        self.data = load_and_prepare_data(ticker, source=data_source, lookback_period_days=lookback_period_days)
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.model = Model()
        self.scaler = StandardScaler()
        self.backtest = Backtest()
        self.indicators = {
            'rsi': {'window': 14},
            'ultimate': {'window1': 7, 'window2': 14, 'window3': 28},
            'williams': {'lbp': 14}
        }
        
    def prepare_data(self, buy_threshold=0.015, sell_threshold=-0.015, test_size=0.3):
        if self.data is None:
            print(f"No data available for {self.ticker}")
            return False
        
        # Generate signals
        self.data = generate_signals(self.data, buy_threshold, sell_threshold)
        
        # Define features
        feature_columns = [
            'open', 'high', 'low', 'close', 'return', 'rsi', 'ultimate_osc', 'williams_r', 'ema',
            'close_lag_1', 'close_lag_2', 'close_lag_3', 'close_lag_4', 'close_lag_5'
        ]
        
        # Split data chronologically
        train_size = int(len(self.data) * (1 - test_size))
        train_data = self.data.iloc[:train_size]
        test_data = self.data.iloc[train_size:]
        
        # Prepare X and y for train and test sets
        self.X_train = train_data[feature_columns]
        self.y_train = train_data['Signal']
        self.X_test = test_data[feature_columns]
        self.y_test = test_data['Signal']
        
        # Scale features
        self.X_train = pd.DataFrame(
            self.scaler.fit_transform(self.X_train),
            columns=self.X_train.columns,
            index=self.X_train.index
        )
        
        self.X_test = pd.DataFrame(
            self.scaler.transform(self.X_test),
            columns=self.X_test.columns,
            index=self.X_test.index
        )
        
        self.test_data = test_data.copy()
        
        return True

    def optimize_indicators(self, n_trials=20):
        """
        Optimize technical indicator parameters using Optuna.
        """
        def objective(trial):
            # Define hyperparameters to optimize
            rsi_window = trial.suggest_int("rsi_window", 5, 20)
            ultimate_window1 = trial.suggest_int("ultimate_window1", 1, 10)
            ultimate_window2 = trial.suggest_int("ultimate_window2", 10, 20)
            ultimate_window3 = trial.suggest_int("ultimate_window3", 20, 30)
            williams_lbp = trial.suggest_int("williams_lbp", 10, 20)
            svm_c = trial.suggest_float("C", 0.01, 100, log=True)
            
            # Apply parameters to indicators
            temp_data = self.data.copy()
            
            # Recalculate indicators with new parameters
            temp_data['rsi'] = ta.momentum.RSIIndicator(
                close=temp_data['close'], window=rsi_window
            ).rsi()
            
            temp_data['ultimate_osc'] = ta.momentum.UltimateOscillator(
                high=temp_data['high'], low=temp_data['low'], close=temp_data['close'],
                window1=ultimate_window1, window2=ultimate_window2, window3=ultimate_window3
            ).ultimate_oscillator()
            
            temp_data['williams_r'] = ta.momentum.WilliamsRIndicator(
                high=temp_data['high'], low=temp_data['low'], close=temp_data['close'],
                lbp=williams_lbp
            ).williams_r()
            
            # Regenerate signals
            temp_data = generate_signals(temp_data)
            
            # Drop NaN values
            temp_data = temp_data.dropna()
            
            # Split data again (since we might have different NaN values after recalculation)
            train_size = int(len(temp_data) * 0.7)
            train_data = temp_data.iloc[:train_size]
            test_data = temp_data.iloc[train_size:]
            
            # Get features and target
            feature_columns = [
                'open', 'high', 'low', 'close', 'return', 'rsi', 'ultimate_osc', 'williams_r', 'ema',
                'close_lag_1', 'close_lag_2', 'close_lag_3', 'close_lag_4', 'close_lag_5'
            ]
            
            X_train = train_data[feature_columns]
            y_train = train_data['Signal']
            X_test = test_data[feature_columns]
            y_test = test_data['Signal']
            
            # Scale data
            scaler = StandardScaler()
            X_train = pd.DataFrame(
                scaler.fit_transform(X_train),
                columns=X_train.columns,
                index=X_train.index
            )
            
            X_test = pd.DataFrame(
                scaler.transform(X_test),
                columns=X_test.columns,
                index=X_test.index
            )
            
            # Train and evaluate model
            model = Model()
            model.svm.C = svm_c
            model.fit(X_train, y_train)
            model.predict(X_test)
            model.f1_score(y_test, model.ypred)
            
            return model.f1
        
        # Create and run Optuna study
        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=n_trials)
        
        # Save best parameters
        best_params = study.best_params
        self.indicators['rsi']['window'] = best_params['rsi_window']
        self.indicators['ultimate']['window1'] = best_params['ultimate_window1']
        self.indicators['ultimate']['window2'] = best_params['ultimate_window2']
        self.indicators['ultimate']['window3'] = best_params['ultimate_window3']
        self.indicators['williams']['lbp'] = best_params['williams_lbp']
        self.model.svm.C = best_params['C']
        
        print(f"Best parameters: {best_params}")
        print(f"Best F1 score: {study.best_value}")
        
        # Recalculate indicators with optimized parameters
        self.data['rsi'] = ta.momentum.RSIIndicator(
            close=self.data['close'], window=self.indicators['rsi']['window']
        ).rsi()
        
        self.data['ultimate_osc'] = ta.momentum.UltimateOscillator(
            high=self.data['high'], low=self.data['low'], close=self.data['close'],
            window1=self.indicators['ultimate']['window1'],
            window2=self.indicators['ultimate']['window2'],
            window3=self.indicators['ultimate']['window3']
        ).ultimate_oscillator()
        
        self.data['williams_r'] = ta.momentum.WilliamsRIndicator(
            high=self.data['high'], low=self.data['low'], close=self.data['close'],
            lbp=self.indicators['williams']['lbp']
        ).williams_r()
        
        # Regenerate signals and prepare data again
        self.data = generate_signals(self.data)
        self.data = self.data.dropna()
        self.prepare_data()
        
        return best_params

    def train_model(self):
        """Train the SVM model with the prepared data"""
        self.model.fit(self.X_train, self.y_train)
        self.model.predict(self.X_test)
        self.model.f1_score(self.y_test, self.model.ypred)
        
        # Replace test signals with predictions for backtesting
        self.test_data['Signal'] = self.model.ypred
        
        print(f"F1 Score for {self.ticker}: {self.model.f1}")
        
        return self.model.f1

    def optimize_backtest(self, n_trials=20):
        """Optimize backtest parameters using Optuna"""
        def objective(trial):
            # Define hyperparameters to optimize
            stop_loss = trial.suggest_float("stop_loss", 0.01, 0.2)
            take_profit = trial.suggest_float("take_profit", 0.01, 0.3)
            n_shares = trial.suggest_int("n_shares", 100, 5000, step=100)
            
            # Apply parameters to backtest
            backtest = Backtest(stop_loss=stop_loss, take_profit=take_profit, n_shares=n_shares)
            backtest.calculate_portfolio(self.test_data)
            backtest.calculate_calmar()
            
            return backtest.calmar_ratio
        
        # Create and run Optuna study
        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=n_trials)
        
        # Save best parameters
        best_params = study.best_params
        self.backtest.stop_loss = best_params['stop_loss']
        self.backtest.take_profit = best_params['take_profit']
        self.backtest.n_shares = best_params['n_shares']
        
        print(f"Best backtest parameters: {best_params}")
        print(f"Best Calmar ratio: {study.best_value}")
        
        # Apply best parameters to backtest
        self.backtest.calculate_portfolio(self.test_data)
        self.backtest.calculate_calmar()
        self.backtest.calculate_sharpe()
        self.backtest.calculate_sortino()
        
        return best_params

    def run_backtest(self):
        """Run backtest with current parameters"""
        self.backtest.calculate_portfolio(self.test_data)
        self.backtest.calculate_calmar()
        self.backtest.calculate_sharpe()
        self.backtest.calculate_sortino()
        
        print(f"\nBacktest results for {self.ticker}:")
        print(f"Calmar ratio: {self.backtest.calmar_ratio:.4f}")
        print(f"Sharpe ratio: {self.backtest.sharpe_ratio:.4f}")
        print(f"Sortino ratio: {self.backtest.sortino_ratio:.4f}")
        print(f"Initial portfolio value: ${self.backtest.portfolio_value[0]:,.2f}")
        print(f"Final portfolio value: ${self.backtest.portfolio_value[-1]:,.2f}")
        print(f"Return: {((self.backtest.portfolio_value[-1]/self.backtest.portfolio_value[0])-1)*100:.2f}%")
        
        return {
            'ticker': self.ticker,
            'calmar': self.backtest.calmar_ratio,
            'sharpe': self.backtest.sharpe_ratio,
            'sortino': self.backtest.sortino_ratio,
            'initial_value': self.backtest.portfolio_value[0],
            'final_value': self.backtest.portfolio_value[-1],
            'return': ((self.backtest.portfolio_value[-1]/self.backtest.portfolio_value[0])-1)*100,
            'portfolio_value': self.backtest.portfolio_value,
            'close_prices': self.test_data['close'].values
        }