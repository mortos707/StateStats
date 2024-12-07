#
# State State Lib
#
import os, sys
import json
import mysql.connector
from mysql.connector import errorcode

# ---------------------------------------------------------
#
# connectToDatabase
#
# ---------------------------------------------------------
def connectToDatabase(param_file):
    """
    Abstracts making a connection to a database
    """
    
    rDict = genReturnDict("In main")
    RS    = ReturnStatus

    dbh    = ""
    D      = {}
    conn   = ""
    cursor = ""
    
    rDict = getDbParams(param_file)
    data = rDict['data']
    if data['loaded']:
        dbName   = data['db_name']
        dbHost   = data['hostname']
        dbUser   = data['user']
        dbPasswd = data['password']

        try:
            conn = mysql.connector.connect(user=dbUser,
                                           password=dbPasswd,
                                           host=dbHost,
                                           database=dbName)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

        cursor = conn.cursor(buffered=True)
        
    D['conn']   = conn
    D['cursor'] = cursor

    rDict['data'] = D
    
    return rDict
    #
    # End of conectToDatabase
    #

# ---------------------------------------------------------
#
# getDbParams
#
# ---------------------------------------------------------
def getDbParams(param_file):
    """
    Internal function to read the database connection config file
    """

    rDict = genReturnDict("In main")
    RS = ReturnStatus

    data = {}
    data['loaded'] = False

    if os.path.exists(param_file) and os.path.isfile(param_file):
        with open(param_file, 'r') as fp:
            Lines = fp.read()

        jObj = json.loads(Lines)
        
        data['db_name']  = jObj['db_name'] 
        data['hostname'] = jObj['hostname'] 
        data['password'] = jObj['password'] 
        data['user']     = jObj['user'] 

        data['loaded'] = True
    else:
        data['loaded'] = False

    rDict['data'] = data
    
    return rDict
    #
    # End of __getDbParams
    #

# --------------------------------------------------------------------
#
# genReturnDict
#
# --------------------------------------------------------------------
def genReturnDict(msg = "") -> dict:
    """
    This sets up a dictionary that is  instantiated to be returned
    from a function call. The real value here is that this dictonary
    contains information, like the function name and line number,
    about the function. This is handy when debugging a mis-behaving
    function.

    Args:
        msg: A text string containg a simple, short message

    Returns:
        r_dict: a dictonary that is returned from a function call

    """
    RS = ReturnStatus()

    r_dict = {}

    # These values come from the previous stack frame ie. the
    # calling function.
    r_dict['line_number']   = sys._getframe(1).f_lineno
    r_dict['filename']      = sys._getframe(1).f_code.co_filename
    r_dict['function_name'] = sys._getframe(1).f_code.co_name

    r_dict['status']   = RS.OK # See the class ReturnStatus
    r_dict['msg']      = msg   # The passed in string
    r_dict['data']     = ''    # The data/json returned from func call
    r_dict['path']     = ''    # FQPath to file created by func (optional)
    r_dict['resource'] = ''    # What resource is being used (optional)

    return r_dict
    # End of gen_return_dict

# --------------------------------------------------------------------
#
# class ReturnStatus
#
# --------------------------------------------------------------------
class ReturnStatus:
    """
    Since we can't have nice things, like #define, this is
    a stand in.

    These values are intended to be returned from a function
    call. For example

    def bar():
        RS = ReturnStatus()
        r_dict = gen_return_dict('Demo program bar')

        i = 1 + 1

        if i == 2:
            r_dict['status'] = RS.OK
        else:
            r_dict['status'] = RS.NOT_OK
            r_dict['msg'] = 'Basic math is broken'

        return r_dict

    def foo():
        RS = ReturnStatus()

        r_dist = bar()
        if r_dict['status'] = RS.OK:
            print('All is right with the world')
        else:
            print('We're doomed!')
            print(r_dict['msg'])
            sys.exit(RS.NOT_OK)

        return RS.OK

    """
    
    OK         = 0 # It all worked out
    NOT_OK     = 1 # Not so much
    SKIP       = 2 # We are skipping this block/func
    NOT_YET    = 3 # This block/func is not ready
    FAIL       = 4 # It all went to hell in a handbasket
    NOT_FOUND  = 5 # Could not find what we were looking for
    FOUND      = 6 # Found my keys
    YES        = 7 # Cant believe I missed these
    NO         = 8 #
    RESTRICTED = 9 #
    # End of class ReturnStatus
