#!/usr/bin/env python3

import os, sys

def main():

    script_dir = os.path.dirname( __file__ )
    mymodule_dir = os.path.join( script_dir, '..', 'Lib' )
    sys.path.append( mymodule_dir )

    import test_one

    myList = test_one.aList
    
    sys.exit(0)
    #

if __name__ == '__main__':
    main()
