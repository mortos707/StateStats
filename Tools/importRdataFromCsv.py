#!/usr/bin/env python3

import os, sys
import csv
import pprint
import json

from MooseRiver import MR_db_util as MRDB
from MooseRiver import MR_state_abbrev as MRSA
from MySQLdb import MySQLError

def main():
    baseDir = '/usr3/home/jgrosch/MooseRiver/Data/Lorax/StateCounty/Json'
    inFile = f"{baseDir}/elections.csv"
    missingFile = f"{baseDir}/missing.json"
    dbParamFile = '/usr/local/site/etc/berkeley-lorax-db.json'

    DF = {}
    pDict = {}
    missing = []
    debug = False
    pDict['tableName'] = 'copy_county_rdata'
        
    D = MRDB.connectToDatabaseJson(dbParamFile)
    pDict['dbConn'] = D['data']['conn']
    pDict['dbCur']  = D['data']['cursor']
    dbConn = pDict['dbConn']
    dbCur = pDict['dbCur']

    Fields = ["election_year",
              "fips",
              "county_name",
              "state",
              "sfips",
              "office",
              "election_type",
              "seat_status",
              "democratic_raw_votes",
              "dem_nominee",
              "republican_raw_votes",
              "rep_nominee",
              "pres_raw_county_vote_totals_two_party",
              "raw_county_vote_totals",
              "county_first_date",
              "county_end_date",
              "state_admission_date",
              "complete_county_cases",
              "original_county_name",
              "original_name_end_date"
              ]

    BP = 0
    
    with open(inFile) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        lineCount = 0
        for row in csv_reader:
            if lineCount == 0:
                if debug:
                    print(f'Column names are {", ".join(row)}')
                lineCount += 1
                continue
            else:
                lineCount += 1
                DF = {}
                
                for index, key in enumerate(Fields):
                    value = row[index]
                    if value.endswith(".0"):
                        value = value.replace('.0', '')

                    if 'nan' in value:
                        value = ''

                    if value is None:
                        value = ''

                    if 'None' in value:
                        value = ''

                    DF[key] = value
                    BP = 0
                BP = 1
                # End of for loop
            BP = 2
            # End of if / else
            # data dict filled
            fips = DF['fips']
            if len(fips) == 0:
                print(f"Fips missing")
                pprint.pprint(DF)
                missing.append(DF)
                continue
                
            pDict['fips'] = fips
            pDict['DF'] = DF
            query = genInsertQuery(pDict)
            if len(query) == 0:
                continue
            try:
                dbCur.execute(query)
            
                if debug:
                    print(query)
                
                if (lineCount % 100) == 0:
                    print(f"{lineCount} records proceesed.")
                    dbConn.commit()
            except MySQLError as ex:
                print(ex)
                sys.exit(1)

        BP = 3
        # End of for loop row / csv_reader
    BP = 4
    dbConn.commit()
    dbCur.close()
    dbConn.close()

    fh = open(missingFile, 'w')

    jStr = json.dumps(missing, indent=4)
    fh.write(jStr)
    fh.close()
    
    sys.exit(0)
    # End of main
    
# --------------------------------------------------------------------
#
# fetchCountyDate
#
# --------------------------------------------------------------------
def fetchCountyData(pDict):
    DF    = pDict['DF']
    dbCur = pDict['dbCur']
    fips  = DF['fips']
    fips = pDict['fips']

    rDict = {}

    if len(fips) == 0:
        BP = 0
        rDict['status'] = '404'
    else:
        query = f"select distinct * from county where fips = {fips};"
        dbCur.execute(query)
        results = dbCur.fetchall()

        if len(results) == 0:
            BP = 0
            rDict['status'] = '404'
        else:
            BP = 1
            rDict['status']     = '200'
            rDict['stateAbbrv'] = results[0][3]
            rDict['stateName']  = results[0][2]
            rDict['countyName'] = results[0][1]
            rDict['fips']       = fips
            rDict['stateFips']  = fips[:2]
            rDict['countyFips'] = fips[2:]
        
    return rDict
    # End of fetchCountyData

# --------------------------------------------------------------------
#
# genInsertQuery
#
# --------------------------------------------------------------------
def genInsertQuery(pDict):
    DF = pDict['DF']
    debug = False
    query = ''

    tableName = pDict['tableName']
    
    electionYear        = str(int(DF['election_year']))
    fips                = DF['fips']
    state               = DF['state']
    sfips               = DF['sfips']
    office              = DF['office']
    electionType        = DF['election_type']
    seatStatus          = DF['seat_status']
    repNominee          = DF['rep_nominee']
    countyFirstDate     = DF['county_first_date']
    stateAdmissionDate  = DF['state_admission_date']
    completeCountyCases = DF['complete_county_cases']
    originalCountyName  = DF['original_county_name']
    demNominee          = DF['dem_nominee']

    #
    if "'" in DF['original_county_name']:
        originalCountyName  = DF['original_county_name'].replace("'", "\\'")
    else:
        originalCountyName  = DF['original_county_name']

    #
    if "'" in DF['county_name']:
        countyName = DF['county_name'].replace("'", "\\'")
    else:
        countyName = DF['county_name']

    #
    if DF['democratic_raw_votes'].endswith('.0'):
        demRawVotes = DF['democratic_raw_votes'].replace('.0', '')
    elif len(DF['democratic_raw_votes']) == 0:
        demRawVotes = 0
    else:
        demRawVotes = DF['democratic_raw_votes']
        
    #
    if DF['republican_raw_votes'].endswith('.0'):
        repRawVotes = DF['republican_raw_votes'].replace('.0', '')
    elif len(DF['republican_raw_votes']) == 0:
        repRawVotes = 0
    else:
        repRawVotes = DF['republican_raw_votes']
        
    #
    if DF['pres_raw_county_vote_totals_two_party'].endswith('.0'):
        rawCountyVoteTotalsTwoParty = DF['pres_raw_county_vote_totals_two_party'].replace('.0', '')
    elif len(DF['pres_raw_county_vote_totals_two_party']) == 0:
        rawCountyVoteTotalsTwoParty = 0
    else:
        rawCountyVoteTotalsTwoParty = DF['pres_raw_county_vote_totals_two_party']

    #
    if DF['raw_county_vote_totals'].endswith('.0'):
        rawCountyVoteTotals = DF['raw_county_vote_totals'].replace('.0', '')
    elif len(DF['raw_county_vote_totals']) == 0:
        rawCountyVoteTotals = 0
    else:
        rawCountyVoteTotals = DF['raw_county_vote_totals']

    #
    indRawVotes = int(rawCountyVoteTotals) - int(rawCountyVoteTotalsTwoParty)

    countyEndDate = DF['county_end_date']
    if countyEndDate is None:
        countyEndDate = ''

    originalNameEndDate = DF['original_name_end_date']
    if originalNameEndDate is None:
        originalNameEndDate = ''

    cfips = fips[2:]
    rDict = fetchCountyData(pDict)
    returnStatus = rDict['status']
    if '404' in returnStatus:
        query = ''
    else:
        if 'countyName' in rDict and len(rDict['countyName']) > 0:
            DF['county_name'] = rDict['countyName']
            if "'" in DF['county_name']:
                countyName = DF['county_name'].replace("'", "\\'")
            else:
                countyName = DF['county_name']
    
    if '200' in rDict['status']:
        query = ("insert into {} ( "
                 " election_year, fips, county_name, state, "
                 " sfips, office, election_type, seat_status, "
                 " dem_raw_votes, dem_nominee, rep_raw_votes, "
                 " rep_nominee, raw_votes_two_parties, "
                 " raw_votes_totals, county_first_date, county_end_data, "
                 " state_admission_date, complete_county_cases, "
                 " original_county_name, original_name_end_date, "
                 " state_abbrv, state_fips, county_fips ) "
                 " values ( "
                 " \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', "
                 " \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', "
                 " \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', "
                 " \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', "
                 " \'{}\', \'{}\', \'{}\' ); "
                 .format(tableName, electionYear, fips, countyName, state, 
                         sfips, office, electionType, seatStatus, demRawVotes, 
                         demNominee, repRawVotes, repNominee, rawCountyVoteTotalsTwoParty, 
                         rawCountyVoteTotals, countyFirstDate, countyEndDate, 
                         stateAdmissionDate, completeCountyCases, originalCountyName, 
                         originalNameEndDate, rDict['stateAbbrv'], rDict['stateFips'], 
                         rDict['countyFips']))
        
        if debug:
            print(query)
    else:
        query = ''
        
    return query
    # End of genInsertQuery

if __name__ == '__main__':
    main()

"""
"""
