from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, FloatType, BooleanType, ShortType,LongType, MapType, ArrayType, TimestampType ,DateType
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQueryListener
import time
from datetime import datetime
import random
import string
import os



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
                "int": IntegerType(),
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
    


    @staticmethod
    def write_df(config: dict) -> None:
        '''
        Method to write a PySpark DataFrame to a Parquet file, partitioned by specified criteria.
        
        Possible partitioning criteria: Any column(s) present in the DataFrame.

        Args:
            config (dict): A dictionary containing the following keys:
                - dataframe (DataFrame): The PySpark DataFrame to write.
                - path (str): The file path where the Parquet file will be saved.
                - mode (str): The write mode ('overwrite', 'append', 'ignore', 'error').
                - criteria (list): A list of column names to partition the data by.
        
        Returns:
            None
        '''
        
        dataframe: DataFrame = config.get("dataframe")
        path: str = config.get("path")
        mode: str = config.get("mode", "overwrite")
        criteria: list = config.get("criteria", [])
        
        # Starts the write method
        # Replaces existing files
        # Partitions the data based on the specified criteria. 
        # Defines the file path.
        dataframe.write \
            .mode(mode) \
            .partitionBy(criteria) \
            .parquet(path)
            
            
            
            
            
            
            
class Logging:
    
    def __init__(self, log_time = 2, fileentry_number_type = 99999):
        '''
        This class creates random log files from a server where the los message contains
            date time | Message Type | Message | Server ID
        The user can set the interval between two log messages or specify 'random'. Not set is 2 seconds.
        And the user can specify how many log messages per log file should be written. Mot set is 'random'. 
        
        Args:
            log_time (optional):                    int or 'random', interval between log messages, default is 2 seconds
        Returns:
            fileentry_number_type (int, optional):   number of log messages pero log file, default is random 
        '''
        
        
        if log_time != 'random' and not isinstance(log_time, int):
            raise ValueError("log_time should be int or 'random'!")
        elif log_time == 'random': 
            self.log_time = 99999
        else:    
            self.log_time = log_time
            
        if not isinstance(fileentry_number_type, int):
            raise ValueError("fileentry_number_type should be an integer!")
        elif fileentry_number_type != 99999:
            self.fileentry_number_type = fileentry_number_type
        else:
            self.fileentry_number_type = 99999

        
        self.error_types = ['WARNING', 'ERROR', 'INFO', 'DEBUG']
        self.run_logs = False
        self.log_messages = {
        "WARN": [
            "Disk usage 85%", "Memory usage high", "CPU temperature warning", "Network latency detected", "Disk space running low",
            "Slow database response", "High number of connections", "Service restart needed", "Potential security risk detected", "System time drift detected",
            "Configuration file modified", "Unexpected traffic spike", "Software update required", "Possible memory leak", "Process taking too long",
            "Filesystem corruption detected", "Swap usage high", "Database connection unstable", "CPU usage 95%", "High error rate in logs",
            "Unusual login activity", "VPN connection unstable", "Packet loss detected", "Background job running long", "SSL certificate expiring soon",
            "High CPU load average", "Hardware temperature rising", "Excessive API calls detected", "Unusual outbound traffic", "Potential data inconsistency",
            "New device detected on network", "Backup process slow", "Unexpected reboot detected", "Service response delayed", "Email queue backlog growing",
            "Too many open files", "DNS resolution failures increasing", "Insufficient file handles available", "Low available entropy", "Message queue delay growing",
            "High garbage collection time", "Unexpected spike in failed jobs", "Slow response from external service", "Cloud resource limits approaching",
            "New SSH key detected", "Filesystem read-only mode", "Unusual cron job execution", "Increasing error rate in API", "Possible DoS attack detected"
        ],
        "ERROR": [
            "500 Internal Server Error", "Application crashed", "Database connection failed", "Memory allocation failed", "File system full",
            "Service not responding", "Disk I/O error", "Critical process terminated", "Kernel panic detected", "Network unreachable",
            "Data corruption detected", "Unauthorized access attempt", "Service authentication failure", "Critical security vulnerability detected",
            "Application timeout error", "Data inconsistency detected", "Severe performance degradation", "System out of disk space", "Disk read failure",
            "Process out of memory", "Service startup failure", "System clock sync failure", "Hardware failure detected", "Unexpected service termination",
            "Database corruption detected", "Cannot write to log file", "Backup process failed", "Configuration error detected", "API service failure",
            "Cloud resource exhausted", "Container crash detected", "SSL handshake failure", "Unauthorized API request", "Broken pipe error",
            "File permissions issue", "Kernel module load failure", "Network interface down", "Firewall rule misconfiguration", "Unexpected shutdown detected",
            "Critical hardware component failure", "Log file write failure", "Too many authentication failures", "Service deployment failure",
            "Unexpected data format in input", "Job queue failure", "Data loss detected", "Service loop detected", "Major dependency failure",
            "Unrecoverable system error"
        ],
        "INFO": [
            "User login successful", "Service started", "Scheduled job executed", "Configuration file updated", "New user registered",
            "Backup completed successfully", "Cache cleared", "Database query executed", "File uploaded successfully", "Password changed successfully",
            "System update completed", "API request processed", "New device connected", "System rebooted", "Log rotation completed",
            "Session expired", "Cron job completed", "Network interface reset", "System boot completed", "New connection established",
            "Certificate renewed successfully", "Service configuration reloaded", "Authentication token issued", "User session ended", "Scheduled maintenance started",
            "Email sent successfully", "Data export completed", "Cloud instance started", "File download completed", "Scheduled report generated",
            "System diagnostics completed", "Service cache refreshed", "Access granted", "Load balancer updated", "Deployment completed",
            "Autoscaling event triggered", "Resource usage within limits", "Database optimization completed", "API key issued",
            "User preferences updated", "Security scan completed", "Monitoring alert resolved", "System shutdown completed",
            "Configuration backup created", "New firewall rule applied", "Filesystem check completed", "New admin user added",
            "Audit log entry recorded", "Server software version updated", "System performance check completed", "User logged out"
        ],
        "DEBUG": [
            "Executing query: SELECT * FROM users", "Cache miss for key: user:1234", "Request headers: {user-agent: Mozilla}", "Debugging session started",
            "Function process_data() called with args: (10, 20)", "Received API request: GET /status", "Retrying database connection...", "Memory allocation: 1024MB",
            "Executing background job ID: 42", "Processing batch 5 of 10", "Attempting to acquire lock on resource", "User input validation passed",
            "Generated token: abc123", "Connecting to external API: example.com", "Request timeout set to 30s", "Thread pool size adjusted to 8",
            "Reading configuration from /etc/config.yaml", "Temporary file created: /tmp/tmpfile1", "Performance benchmark started", "Heap size: 2048MB",
            "Loading user preferences from database", "Releasing lock on resource", "Generating session ID: xyz789", "Debug mode enabled",
            "Initializing new connection pool", "Response payload: true", "File read operation took 120ms", "Checking for software updates",
            "CPU utilization: 45%", "Attempting connection retry #3", "Clearing temporary cache", "API response status: 200 OK",
            "Setting environment variable: DEBUG=true", "Triggering log rotation", "Reading data chunk: 256KB", "Parsing JSON response",
            "Verifying SSL certificate", "Starting new transaction", "Rolling back transaction due to error", "HTTP request latency: 85ms",
            "Sending email notification", "Execution time: 5.3s", "Revalidating cache entry", "Checking for memory leaks", "Processing image upload",
            "Spawning new worker thread", "Compiling configuration settings", "Exiting debug mode", "Switching to maintenance mode"
        ]
    }
        
    
    
    
    
    def __generate_log_entry(self):
        '''
        this function generates log messages consisting of 
        date time | message type | message | server id
        '''
        category = random.choice(list(self.log_messages.keys()))
        message = random.choice(self.log_messages[category])
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        server_node = "server-node-{}".format( random.randint(1,9))
        return "{} | {} | {} | {}\n".format(timestamp, category, message, server_node)


    def __generate_logfilename(self, length=20):
        '''
        This function creates a random string of e set length. Default is 20 characters
        Args:
            length (int):             length of random file name string
        '''
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


    def __append_log_message(self, filestring):
        '''
        this function appends a new message to the log string
        Args:
            filestring (string):             current log messages
        '''
        return filestring + self.__generate_log_entry()
        
        
    def __run_logging(self,log_file_path):
        '''
        write logging files, the user passes a path where the files will be written
        Args:
            log_file_path (string):             absolut file path
        '''
        
        # start endless logging loop
        while self.run_logs:
            
            fileentry_number = 0
            #create random log file name
            filename = "logentry_"+self.__generate_logfilename()+".txt"
            filepath = os.path.join(log_file_path, filename)
            
            
            logstring_to_file = ''
            #when the user wants a random number of entries per log file
            if self.fileentry_number_type == 99999:
                fileentry_number = random.randint(0, 15)
            
            #when the user passed an integer as number of entries per logfile
            else:
                fileentry_number = self.fileentry_number_type
                
            #start writing to file
            for entry in range(fileentry_number):
                    logstring_to_file = self.__append_log_message(logstring_to_file)
                    
                    #nothing
                    if self.log_time == 2:
                        time.sleep(2)                                       # Wait for 2 seconds
                    #otherwise use a random sleep time within 0.1sec to 5.5sec
                    elif self.log_time == 99999:
                        time.sleep(random.uniform(0.1, 5.5))                
                    # use userspecific time
                    else:
                        time.sleep(self.log_time)
                            
            #create and write to file
            with open(filepath, "a") as file:
                file.write(logstring_to_file)
                            



    def start_logging(self, log_file_path):
        '''
            start the logging loop
        '''
        self.run_logs = True
        self.__run_logging(log_file_path)
        
        
        
    def stop_logging(self):
        '''
            stop the logging loop
        '''
        self.run_logs = False


class TrafficListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        num_input_rows = event.progress.numInputRows
        print(f"Query made progress: {event.progress}")
        
        if num_input_rows >= 50:
            send_alert(f"High volume of data: {num_input_rows} rows")


    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")
