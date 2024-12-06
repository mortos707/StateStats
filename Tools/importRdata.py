#!/usr/bin/env python

import os, sys
import pyreadr
from MooseRiver import MR_db_util as MRDB
from MooseRiver import MR_state_abbrev as MRSA
from MySQLdb import MySQLError

# --------------------------------------------------------------------
#
# Main
#
# --------------------------------------------------------------------
def main():

    homeDir     = os.environ['HOME']
    dataDir     = os.environ['DATA_DIR']
    baseDir     = f"{dataDir}/Lorax/StateCounty/Json"
    dbParamFile = '/usr/local/site/etc/redway-lorax-db.json'

    #'/usr3/home/jgrosch/Downloads/StuffMaster/Stuff39/election_Rdata/County_Level_US_Elections_Data/TEMP'
    
    base    = f"{dataDir}/Election/election_Rdata/County_Level_US_Elections_Data"
    inFile  = f"{base}/election-county.Rdata"
    outFile = f"{baseDir}/elections.csv"

    if not os.path.exists(inFile):
        print(f"Error: {inFile} NOT found.")
        sys.exit(1)
        
    pDict       = {}
    fh          = ''
    insertCount = 0
    
    debug     = False
    toCsv     = True
    toDB      = False
    firstTime = True
    
    if toDB:
        D = MRDB.connectToDatabaseJson(dbParamFile)
        pDict['dbConn'] = D['data']['conn']
        pDict['dbCur']  = D['data']['cursor']
        dbConn = pDict['dbConn']
        dbCur = pDict['dbCur']

    result = pyreadr.read_r(inFile)

    df = result['pres_elections_release']
    dfLen = len(df)

    index = range(dfLen)
    for index in range(dfLen):
        dDict = df.loc[index]

        pDict['DF'] = dDict
        if toDB:
            query = genInsertQuery(pDict)
            if len(query) == 0:
                dumpRecord(pDict)
                print("record missing")
                continue
        
            if debug:
                print(query)

            try:
                dbCur.execute(query)
                insertCount += 1
                if (insertCount % 100) == 0:
                    print(f"Records inserted: {insertCount}")
                    dbConn.commit()
            except MySQLError as ex:
                print(ex)
                sys.exit(1)

        if toCsv:
            BP = 0
            if firstTime:
                firstTime = False
                fh = open(outFile, 'w')
                writeHeader(fh)
            csvStr = genCsvStr(pDict)
            fh.write(csvStr)
            insertCount += 1
            if (insertCount % 1000) == 0:
                print(f"Records inserted: {insertCount}")
            #
            BP = 1
        #
        BP = 0

    if toDB:
        dbConn.commit()
        dbCur.close()
        dbConn.close()

    if toCsv:
        fh.close()
        
    sys.exit(0)
    # End of main

# --------------------------------------------------------------------
#
# genInsertQuery
#
# --------------------------------------------------------------------
def genInsertQuery(pDict):
    DF = pDict['DF']
    debug = False
    query = ''
    
    electionYear                = str(int(DF['election_year']))
    fips                        = DF['fips']
    countyName                  = DF['county_name']
    state                       = DF['state']
    sfips                       = DF['sfips']
    office                      = DF['office']
    electionType                = DF['election_type']
    seatStatus                  = DF['seat_status']
    demRawVotes                 = str(int(DF['democratic_raw_votes']))
    demNominee                  = DF['dem_nominee']
    repRawVotes                 = str(int(DF['republican_raw_votes']))
    repNominee                  = DF['rep_nominee']
    rawCountyVoteTotalsTwoParty = str(int(DF['pres_raw_county_vote_totals_two_party']))
    rawCountyVoteTotals         = str(int(DF['raw_county_vote_totals']))
    indRawVotes                 = str(int(rawCountyVoteTotals) - int(rawCountyVoteTotalsTwoParty))
    countyFirstDate             = DF['county_first_date']
    stateAdmissionDate          = DF['state_admission_date']
    completeCountyCases         = DF['complete_county_cases']
    originalCountyName          = DF['original_county_name']

    countyEndDate = DF['county_end_date']
    if countyEndDate is None:
        countyEndDate = ''

    originalNameEndDate = DF['original_name_end_date']
    if originalNameEndDate is None:
        originalNameEndDate = ''

    cfips = fips[2:]
    rDict = fetchCountyData(pDict)
    if '200' in rDict['status']:
        query = ("insert into county_rdata ( "
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
                 .format(electionYear, fips, countyName, state, 
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

# --------------------------------------------------------------------
#
# fetchCountyData
#
# --------------------------------------------------------------------
def fetchCountyData(pDict):
    DF    = pDict['DF']
    dbCur = pDict['dbCur']
    fips  = DF['fips']

    rDict = {}

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
# dumpRecord
#
# --------------------------------------------------------------------
def dumpRecord(pDict):
    
    return
    # End of dumpRecord

# --------------------------------------------------------------------
#
# genCsvStr
#
# --------------------------------------------------------------------
def genCsvStr(pDict):
    outStr = ''
    DF = pDict['DF']
    Fields = ['election_year,', 'fips,', 'county_name,',
              'state,', 'sfips,', 'office,', 'election_type,',
              'seat_status,', 'democratic_raw_votes,',
              'dem_nominee,,', 'republican_raw_votes,,',
              'rep_nominee,', 'pres_raw_county_vote_totals_two_party,',
              'raw_county_vote_totals,', 'county_first_date,',
              'county_end_date,','state_admission_date,',
              'complete_county_cases,','original_county_name,',
              'original_name_end_date']

    outStr = ("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},"
              "{10},{11},{12},{13},{14},{15},{16},{17},{18},{19}\n"
              .format(DF['election_year'], DF['fips'],
                      DF['county_name'], DF['state'], DF['sfips'],
                      DF['office'], DF['election_type'],
                      DF['seat_status'], DF['democratic_raw_votes'],
                      DF['dem_nominee'], DF['republican_raw_votes'],
                      DF['rep_nominee'], DF['pres_raw_county_vote_totals_two_party'],
                      DF['raw_county_vote_totals'], DF['county_first_date'],
                      DF['county_end_date'], DF['state_admission_date'],
                      DF['complete_county_cases'],
                      DF['original_county_name'],
                      DF['original_name_end_date']))

    
    return outStr
    # End of genCsvStr

# --------------------------------------------------------------------
#
# writeHeader
#
# --------------------------------------------------------------------
def writeHeader(fh):
    Header = ['election_year,', 'fips,', 'county_name,',
              'state,', 'sfips,', 'office,', 'election_type,',
              'seat_status,', 'democratic_raw_votes,',
              'dem_nominee,,', 'republican_raw_votes,,',
              'rep_nominee,', 'pres_raw_county_vote_totals_two_party,',
              'raw_county_vote_totals,', 'county_first_date,',
              'county_end_date,','state_admission_date,',
              'complete_county_cases,','original_county_name,',
              'original_name_end_date']
    outStr = ''.join(Header)
    outStr = f"{outStr}\n"
    
    fh.write(outStr)
    
# --------------------------------------------------------------------
#
# Entry point
#
# --------------------------------------------------------------------
if __name__ == '__main__':
    main()
    
