#!/usr/bin/env python3

import os, sys

import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import argparse

from datetime import datetime
from MooseRiver import MR_db_util as MRDB
from MySQLdb import MySQLError

def main():
    homeDir  = os.environ['HOME']
    baseDir  = f"{homeDir}/MooseRiver/Data/Lorax/StateCounty"
    dataHome = f"{baseDir}/Json/State3"

    StateCount = {}
    
    logHome  = f"{homeDir}/MooseRiver/Data/Lorax/Log"
    logFile = f"{logHome}/image-creation.log"
    logHandle = open(logFile, 'w')

    parser = argparse.ArgumentParser()

    parser.add_argument('--debug', help='Turn on debugging',
                        action='store_true', default=False)

    parser.add_argument('--doimage', help='Turn on image creation',
                        action='store_true', default=False)

    parser.add_argument('--logit', help='Turn on logging',
                        action='store_true', default=False)

    args = parser.parse_args()

    logIt   = args.logit
    doImage = args.doimage
    debug   = args.debug

    if not os.path.exists(dataHome):
        print(f"Error: {dataHome} NOT found")
        sys.exit(1)
        
    if doImage:
        for (Root, Dirs, Files) in os.walk(dataHome, topdown=True):
            for fileName in Files:
                inFile = f"{Root}/{fileName}"

                if not inFile.endswith('.json'):
                    continue
            
                with open(inFile, 'r') as fh:
                    xLines = fh.read()

                jObj = json.loads(xLines)
    
                fips       = jObj[0]['fips']
                countyName = jObj[0]['county_name']
                stateAbbrv = jObj[0]['state_abbrv']
                workState  = jObj[0]['state_name']
                workCounty = jObj[0]['county_name']
                
                demVotes   = int(jObj[0]['dem_votes'])
                repVotes   = int(jObj[0]['rep_votes'])
                indVotes   = int(jObj[0]['ind_votes'])
                
                titleStr = f"{countyName}, {stateAbbrv}"

                if 'County' in workCounty:
                    workCounty = workCounty.replace('County', '').strip()

                    if ' 'in workCounty:
                        workCounty = workCounty.replace(' ', '-').strip()

                if 'Parish' in workCounty:
                    workCounty = workCounty.replace('Parish', '').strip()
                    if ' 'in workCounty:
                        workCounty = workCounty.replace(' ', '-').strip()

                if ' ' in workState:
                    workState = workState.replace(' ', '-').strip()

                outDir  = f"{baseDir}/Image/State3/{workState}"
                outFile = f"{outDir}/{workCounty}.png"

                if not os.path.exists(outDir):
                    os.makedirs(outDir)
                    print(f"Created {outDir}")
                
                DF = pd.read_json(inFile)
                
                plt.plot(DF['year'], DF['dem_percent'], color = 'blue', label='Dem', marker='d')
                plt.plot(DF['year'], DF['ind_percent'], color = 'green', label='Ind', marker='d')
                plt.plot(DF['year'], DF['rep_percent'], color = 'red', label='Rep', marker='d')
                
                plt.xlabel('Election Year')
                plt.ylabel('% of vote')
                plt.title(titleStr)
    
                plt.grid(axis='both')
                plt.legend()
                
                plt.savefig(outFile)
                logStr = f"Wrote {countyName}, {stateAbbrv}"
                print(logStr)
                logHandle.write(f"{logStr}\n")
            
                plt.cla()
                plt.clf()
                logStr = ''
                BP = 0
            BP = 1
        BP = 2
    BP = 3


    logHandle.close()

    #genStateDict(dataHome)
    
    sys.exit(0)
    # End of main

def genStateDict(dataHome):
    BP = 0
    StateCount = {}
    debug = True
    
    for (Root, Dirs, Files) in os.walk(dataHome, topdown=True):
        for fileName in Files:
            inFile = f"{Root}/{fileName}"

            if not inFile.endswith('.json'):
                continue
            
            with open(inFile, 'r') as fh:
                xLines = fh.read()

            jObj = json.loads(xLines)

            for entry in jObj:
                BP = 0
                
                fips       = entry['fips']
                year       = entry['year']
                countyName = entry['county_name']
                stateAbbrv = entry['state_abbrv']
                stateName  = entry['state_name']

                if not stateName in StateCount:
                    StateCount[stateName] = {}

                if debug:
                    print(f"Year: {year} - State: {stateName} - County: {countyName}")
                    
                StateCount[stateName][year] = {}

                demVotes   = int(entry['dem_votes'])
                repVotes   = int(entry['rep_votes'])
                indVotes   = int(entry['ind_votes'])

                # DEM
                if not 'demVotes' in StateCount[stateName][year]:
                    StateCount[stateName][year]['demVotes'] = demVotes
                else:
                    dVotes = StateCount[stateName][year]['demVotes']
                    dVotes += demVotes
                    StateCount[stateName][year]['demVotes'] = dVotes
                    
                # REP
                if not 'repVotes' in StateCount[stateName][year]:
                    StateCount[stateName][year]['repVotes'] = repVotes
                else:
                    rVotes = StateCount[stateName][year]['repVotes']
                    rVotes += repVotes
                    StateCount[stateName][year]['repVotes'] = rVotes
                    
                # IND
                if not 'indVotes' in StateCount[stateName][year]:
                    StateCount[stateName][year]['indVotes'] = indVotes
                else:
                    iVotes = StateCount[stateName][year]['indVotes']
                    iVotes += indVotes
                    StateCount[stateName][year]['indVotes'] = iVotes
                    
                BP = 0
            BP = 1
        BP = 2
    BP = 3
    
    return

if __name__ == '__main__':
    
    main()
    
    
