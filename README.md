# StateStats

Here are the steps to recreate this work.

* cd $HOME
* mkdir Git
* export GIT_HOME="${HOME}/Git"
* mkdir DataDir
* export DATA_DIR="${HOME}/DataDir"
* cd $DATA_DIR

Go to the following URL
```
   https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/DGUMFI
```
   download dataverse_files.zip from the pulldown labeled "Access Dataset" to $DATA_DIR. 

* cp etc/HOSTNAME-DBNAME-db.json to an appropayly name file. Edit the file with the correct values

* importRdata.py
* importRdataFromCsv.py
* genCountyJson.py
* genVotingGraphs3.py
