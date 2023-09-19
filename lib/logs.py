# ref: https://github.com/acschaefer/duallog

import inspect
import logging
import copy
import os
import re

# Define the default logging message formats.
FILE_FORMAT = '%(levelname)s:%(name)s:%(asctime)s:%(message)s'

'''
The Custom Handler classes below override logging File and Stream Handlers
to allow log formatting on evert line of a log message, instead of only on
the first line as is the default handling emit method of logging's handlers.

Emit creates a copy of the LogRecord (a logged event), and sets the copy's
message to getMessage() which is the argument-evaluated form of the message,
it then sets the arguments to an empty tuple. the LogRecord's message is then
split on each newline and each line gets emitted to the super class's emit().
As a result, each line is treated as a new LogRecord with no arguments to be
evaluated; so the Formatter formats each new line of logged messages.

References:
https://docs.python.org/3.7/library/logging.html#logging.LogRecord
https://docs.python.org/3.7/library/logging.html#logging.LogRecord.getMessage
'''
class CustomFileHandler(logging.FileHandler):
    def __init__(self, file):
        super(CustomFileHandler, self).__init__(file)

    def emit(self, record):
        fh_repack = copy.copy(record)
        fh_repack.msg = fh_repack.getMessage()
        fh_repack.args = ()
        messages = fh_repack.msg.split('\n')
        for message in messages:
            fh_repack.msg = message
            super(CustomFileHandler, self).emit(fh_repack)

class CustomFormatter(logging.Formatter):
    """ Formatter that applies custom filters to the log outputs 
    """
    @staticmethod
    def _PasswordFilter(s):
        """ Uses regex to identify and redact passwords in the log outputs. 

        This filter identifies and redacts a password that is written to the logs 
        in the S3 to SFTS microservice upon error. Because the structure of this 
        error is known, a password is identified as the text between two string 
        literals, '-password:' and '-quiterror'. If no password is identified by 
        the regex, no redaction is made. There is a small chance that the regex 
        expression identifies text that is not actually part of the password if 
        it appears between the -password:' and '-quiterror' string literals. In 
        this case, all of that text will be redacted and lost in addition to the 
        password. This is a known bug, but the chances of it occurring has been 
        identified as low enough that the code in it's current state is approved 
        for use in production.  
        """
        return re.sub(r"-password:.*?-quiterror", r"-password:********', '-quiterror", s)

    def format(self, record):
        """ Applies the custom filters to the formatter
        """
        original = logging.Formatter.format(self, record)
        filtered = self._PasswordFilter(original)
        return filtered 

def setup(dir='logs', minLevel=logging.INFO):
    """ Set up dual logging to console and to logfile.
    """

    # Create the root logger.
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create the log filename based on caller filename
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    file_name = module.__file__.replace('.py', '.log')

    # Validate the given directory.
    dir = os.path.normpath(dir)

    # Create a folder for the logfiles.
    if not os.path.exists(dir):
        os.makedirs(dir)

    # Construct the name of the logfile.
    file_path = os.path.join(dir, file_name)

    # Set up logging to the logfile.
    file_handler = CustomFileHandler(file_path)
    file_handler.setLevel(minLevel)
    file_formatter = CustomFormatter(FILE_FORMAT)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
