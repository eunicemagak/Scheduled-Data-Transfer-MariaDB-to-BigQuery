import logging

# Configure the logging format and level
logging.basicConfig(
    level=logging.DEBUG,  # Set the desired logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Create a file handler to log messages to a file
file_handler = logging.FileHandler('/var/log/bigquerymigrationlogs/app.log')
file_handler.setLevel(logging.INFO)  # Set the desired logging level for the file

# Create a console handler to log messages to the console (stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Set the desired logging level for the console

# Create a formatter and attach it to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Get the root logger and attach the handlers to it
logger = logging.getLogger()
logger.addHandler(file_handler)
logger.addHandler(console_handler)
