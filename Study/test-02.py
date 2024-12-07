#!/usr/bin/env python3

import os, sys

script_dir = os.path.dirname( __file__ )
mymodule_dir = os.path.join( script_dir, '..', 'lib' )
sys.path.append( mymodule_dir )

import sslib

def main():
    RS = sslib.ReturnStatus
    
    cwd = os.environ['PWD']
    configFile = f"{cwd}/../etc/redway-lorax-db.json"
    rDict = sslib.connectToDatabase(configFile)

    if rDict['status'] == RS.OK:
        dbDict   = rDict['data']
        dbConn   = dbDict['conn']
        dbCursor = dbDict['cursor']
    
    sys.exit(0)
    #

if __name__ == '__main__':
    main()
