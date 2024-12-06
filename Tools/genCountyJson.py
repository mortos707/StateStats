#!/usr/bin/env python3

import os, sys
import json
from MooseRiver import MR_db_util as MRDB
from MooseRiver import MR_state_abbrev as MRSA
from MySQLdb import MySQLError

def main():
    homeDir = os.environ['HOME']
    dataDir = f"{homeDir}/MooseRiver/Data/Lorax/StateCounty/Json/State3"

    dbParamFile = '/usr/local/site/etc/berkeley-lorax-db.json'

    DF = {}
    pDict = {}
    debug = False

    D = MRDB.connectToDatabaseJson(dbParamFile)
    pDict['dbConn'] = D['data']['conn']
    pDict['dbCur']  = D['data']['cursor']
    dbConn = pDict['dbConn']
    dbCur = pDict['dbCur']

    query = "select distinct state_name, state_abbrv from election_live; "
    dbCur.execute(query)
    Result1 = dbCur.fetchall()

    for E1 in Result1:
        stateName  = E1[0]
        stateAbbrv = E1[1]
        stateNameTrimed = stateName.replace(' ', '-')

        print(f"Doing {stateName}")
        
        query2 = ("select distinct county_name, fips "
                  " from election_live where state_abbrv = \'{}\';"
                  .format(stateAbbrv))

        dbCur.execute(query2)
        Result2 = dbCur.fetchall()

        for E2 in Result2:
            countyName = E2[0]
            if 'County' in countyName:
                countyNameTrimed = countyName.replace('County', '').strip()
                countyNameTrimed = countyNameTrimed.replace(' ', '-')

            outDir = f"{dataDir}/{stateNameTrimed}"
            if not os.path.exists(outDir):
                os.makedirs(outDir)
                
            
            fips = E2[1]
            stateFips = fips[:2]
            countyFips = fips[2:]
            outFile = f"{outDir}/{fips}.json"

            query3 = (" select * from election_live where "
                      " fips = \'{}\' and year >= 1920 "
                      " order by year; "
                      .format(fips))

            if debug:
                print(query3)

            dbCur.execute(query3)
            Result3 = dbCur.fetchall()

            cList = []
            for E3 in Result3:
                rDict = genElectionDict(E3)
                cList.append(rDict)
                BP = 0

            jStr = json.dumps(cList, indent=4)
            with open(outFile, 'w') as fh:
                fh.write(jStr)
                
            BP = 1
        BP = 2
    BP = 3

    sys.exit(0)
    # End of main

def genElectionDict(E):
    D = {}

    D['state_name']  = E[1]
    D['state_abbrv'] = E[2]
    D['state_fips']  = E[3]
    D['county_name'] = E[4]
    D['county_fips'] = E[5]
    D['fips']        = E[6]
    D['year']        = E[7]
    D['dem_votes']   = E[8]
    D['dem_percent'] = E[9]
    D['rep_votes']   = E[10]
    D['rep_percent'] = E[11]
    D['ind_votes']   = E[12]
    D['ind_percent'] = E[13]
    D['total_votes'] = E[14]
    
    
    return D
    # End of genElectionDict

if __name__ == '__main__':
    main()
