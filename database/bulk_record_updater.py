#!/bin/python
#-------------------------------------------------------------------------------
# Purpose : To do bulk update on Unix Server Table.
#
# Author:      1427693
#
# Created:     Wed 10/28/2015
# Copyright:   (c) Sampath 2015
#
# Input : update csv-file in same location as this script.
#-------------------------------------------------------------------------------

"""
======== tutorial ==========

sam@bash$: python update_server.py -h
Usage: update_server.py [options]

Options:
  -h, --help           show this help message and exit
  --cleanup            clear temp files if any present
  --template           creates generic server-update template
  --app_template       creates application-template
  --rename_template    creates a server rename-template
  --query              generates sql query and stop
  --run                generates queries & direcly run to server
  --trail_run          does a trail run, generates sql queries & errors
  --run_sql            runs input sql query file
  --filename=FILENAME  to give file input


How to update server details ?
step 1: Generate a bulk update template.

we have three standard template which can be used for form filling. 

Generice Template : A generic template which covers of columns seen from unix inventor page.
    sam@bash$: python update_server.py --template

     template create as 'test.csv'

    sam@bash$: head test.csv
    HOSTNAME,STATUS,FIREWALLED,IP_ADDR,NOTES,VIRTUALIZATION_TYPE,BUSINESS_CRITICALITY,MANAGEMENT_TEAM,OS
    _VERSION,HOSTING_PROVIDER,ASSET_TYPE,SERVER_MODEL_NAME,LOCATION,LIFECYCLE

        ( or )

Application Template : Template which is specifically for updating Application details in unix inventor page.

    sam@bash$: python update_server.py --app_template

     application template create as 'test.csv'

    sam@bash$: head test.csv
    HOSTNAME,APPLICATION
    

        ( or )

Re-Name Template : Template which is specifically for updating/re-name hostname.

    sam@bash$: python update_server.py --rename_template

     host-rename template create as 'test.csv'

    sam@bash$: head test.csv
    HOSTNAME,NEW_HOSTNAME
    sam@bash$: 
    
step 2: Fill the details in the CSV templates generated above. Only in case of Generic template, we remove columns we dont wish to update but a minmum of two columns are required and HOSTNAME column is always a must.

maintain caution while updating template. this is update script, only update details will happen here. In case if APPLICATION NAME or MANAGEMENT_TEAM or ASSET_HEAD, ... IF NOT FOUND, THEY WILL BE UPDATED AS NULL !!


step 3:

step 3.1 :
    If you are confident to update details will be successfully, then run below command
    
    sam@bash$: python update_server.py --run
    
    In above case, application will take available 'test.csv' by default. In case if you have different file to update, use below format
    
    sam@bash$: python update_server.py --run -f <new_file.csv>
    
    once you finish, above command update will be finished. You can check 'queries.sql' and 'errors.sql' files to get more details.
    
step 3.2
    If you are un-sure of updates and wish to do a trail run before you do commit changes follow this method instead of 3.1 (above)
    
    sam@bash$: python update_server.py --trail_run
    
    Above command will do a trail and generates 'queries.sql' and 'errors.sql' files to get more details.
    
    once you are done with amends, you go back to step 3.1 and finish you updates.


"""

import os
import csv
from pprint import pprint
from optparse import OptionParser


# resetting global params
SAVE_SQL = True
SAVE_ERROR_SQL = True

STANDARD_CSV_HEADER = 'HOSTNAME,STATUS,FIREWALLED,\
IP_ADDR,NOTES,VIRTUALIZATION_TYPE,\
BUSINESS_CRITICALITY,MANAGEMENT_TEAM,\
OS_VERSION,HOSTING_PROVIDER,\
ASSET_TYPE,SERVER_MODEL_NAME,\
LOCATION,LIFECYCLE'

STANDARD_CSV_SPECIAL_HEADER_01 = 'HOSTNAME,APPLICATION'

STANDARD_CSV_SPECIAL_HEADER_02 = 'HOSTNAME,NEW_HOSTNAME'

STANDARD_COLS_SQL = { 'IP_ADDR' : " IP_ADDR = lower('$@m'),",
 'BUSINESS_CRITICALITY' : " BUSINESS_CRITICALITY = lower('$@m'),",
 'FIREWALLED' : " FIREWALLED =  lower('$@m'),",
 'NOTES' : " NOTES =  NOTES || lower('$@m'),",
 'SERVER_MODEL_NAME' : " SERVER_MODEL_NAME = (select id from SERVER_MODEL where lower(SERVER_MODEL_NAME) =  lower('$@m')),",
 'STATUS' : " STATUS_ID = (select id from Status_option where lower(name) = lower('$@m') ),",
 'LIFECYCLE' : " LIFECYCLE_ID = (select id from lifecycle_option where lower(name) = lower('$@m') ),",
 'LOCATION' : " LOCATION_ID = (select id from location where datacenter_id = (\
            select id from datacenter where datacenter_name is null and country_id = (\
            select id from country where LOWER(ISO_CODE) = lower('$@m')))),",
 'OS_VERSION' : " OS_VERSION_ID = (select id from OS_VERSION where lower(OS_VERSION_NAME) = lower('$@m') ),",
 'ASSET_TYPE' : " ASSET_type_ID = (select id from ASSET_TYPE WHERE lower(ASSET_TYPE_NAME) = lower('$@m')  ),",
 'MANAGEMENT_TEAM' : " MANAGEMENT_TEAM_ID = (select id from management_team where lower(management_team_name) = lower('$@m')  ),",
 'HOSTING_PROVIDER' : " HOSTING_PROVIDER_ID = (select id from hosting_provider_option where lower(name) = lower('$@m') ),",
 'VIRTUALIZATION_TYPE' : " VIRTUALIZATION_TYPE_ID = (select id from VIRTUALIZATION_TYPE where LOWER(VIRTUALIZATION_TYPE_NAME) =  lower('$@m')  ),",
 }

SERVER_APPLICATION_SQL = "insert into SERVERS_APPLICATIONS values (null, \
 (select id from server where lower(hostname) = lower('$@m1')), \
 (select min(id) from application where lower(application_name) = lower( '$@m2' )), \
 1427693, \
 sysdate)"

SERVER_RENAME_SQL = "Update server set hostname = '$@m1',\
 last_udpated = sysdate, last_updated_by =1427693\
 where lower(hostname) = lower('$@m2');"



APP_INPUT_TEMLATE_FILE = 'test.csv'

APP_SQL_FILE = 'queries.sql'

APP_SQL_ERR_FILE = 'errors.sql'


APPLICATION_FILES = [ APP_INPUT_TEMLATE_FILE, APP_SQL_FILE, APP_SQL_ERR_FILE ]

def clean_up():
    """
    housekeeps files
    """
    for filename in APPLICATION_FILES:
        try:
            os.remove(filename)
        except OSError:
            pass

def create_sample_file(filename, header):
    """
    Generic/Standard update template
    """
    fp = open(filename, 'w')
    fp.write(header)
    fp.close()

def create_sample_file1(filename=APP_INPUT_TEMLATE_FILE, header=STANDARD_CSV_HEADER):
    """
    Generic/Standard update template
    """
    create_sample_file(filename, header)

def create_sample_file2(filename=APP_INPUT_TEMLATE_FILE, header=STANDARD_CSV_SPECIAL_HEADER_01):
    """
    Server-Application mapping template
    """
    create_sample_file(filename, header)

def create_sample_file3(filename=APP_INPUT_TEMLATE_FILE, header=STANDARD_CSV_SPECIAL_HEADER_02):
    """
    Server-renaming template
    """
    create_sample_file(filename, header)

def read_update_data(filename=APP_INPUT_TEMLATE_FILE):
    data  = csv.DictReader(open(filename))
    sam = []
    for each in data:
        sam.append(each)
    return sam

def update_query(row_dict={'HOSTNAME' : 'uklpipinf01a', 'IP_ADDR' : '123123' }):
    """
    """
    core_keys = [ 'HOSTNAME']
    for each in core_keys :
        if each not in row_dict.keys():
            print 'key columns for update are missing !'
            return # nothing
    
    query_pool = ["UPDATE server set"]
    # add updates if any
    for key in row_dict.keys():
        row_dict[key] = row_dict[key].strip()
        if ( key in STANDARD_COLS_SQL.keys() ) and ( row_dict[key] ) :
            half_query = STANDARD_COLS_SQL[key].replace('$@m', row_dict[key])
            query_pool.append( half_query )
    query_pool.append("LAST_UPDATED = sysdate, LAST_UPDATED_BY = 1427693")
    query_pool.append( "where lower(hostname) = lower('$@m');".replace('$@m', row_dict['HOSTNAME']) )
    query = '\n'.join(query_pool)
    
    if len(query_pool) > 3 :
        return query
    else:
        print query
        return ''# nothing

def update_hostname_query(hostname, new_hostname):
    hostname = hostname.strip()
    new_hostname = new_hostname.lower().strip()
    if not ( hostname and new_hostname ):
        print 'hostname or new_hostname is improper or not provided !'
        return ''# nothing
    query = str(SERVER_RENAME_SQL).replace('$@m1', new_hostname)
    query = query.replace('$@m2', hostname)
    return query

def update_application_query(hostname, application_name):
    hostname = hostname.strip()
    application_name = application_name.strip()
    if not (hostname and application_name):
        print 'hostname or application_name is improper or not provided !'
        return ''# nothing
    query = str(SERVER_APPLICATION_SQL).replace('$@m1', hostname)
    query = query.replace('$@m2', application_name)
    return query
  
def generate_sql_queries(filename='test.csv'):
    """
    update server's application or rename a hostname
    """
    all_queries = []
    with open('test.csv') as fp:
        hostname, special_column = fp.readline().lower().strip().split(',')[:2]
        if special_column == 'application':
            for line in fp.readlines():
                hostname, value =  line.lower().strip().split(',')[:2]
                query = update_application_query(hostname, value)
                all_queries.append(query)
        elif special_column == 'new_hostname':
            for line in fp.readlines():
                hostname, value =  line.lower().strip().split(',')[:2]
                query = update_hostname_query(hostname, value)
                all_queries.append(query)
        else:
            # generic state where
            for row_dict in read_update_data(filename):
                query = update_query(row_dict)
                if query:
                    all_queries.append(query)
    if not all_queries:
        print '\n no data found or expected columns not found !'
    return all_queries
        
def save_sql_queries(all_queries, filename='queries.sql'):
    fp = open('queries.sql', 'w')
    store = []
    for sql in all_queries:
        sql = sql.replace('\n', '\t').strip()
        if not sql:
            continue
        if (not sql.endswith(';')):
            sql += ';'
        sql += '\n'
        store.append(sql)
    if store:
        fp.writelines(store)
    fp.close()

def print_sql_queries(all_queries):
    for each in all_queries:
        print each

def run(all_queries, commit=False):
    from db_config import cursor
    failed = []
    for each in all_queries:
        try:
            cursor.execute(each)
        except:
            failed.append(each)
    save_sql_queries(failed, 'errors.sql')
    if commit:
        cursor.execute('commit')
    cursor.close()

def runfile(filename="queries.sql"):
    all_queries = []
    fp = open(filename)
    data = fp.readlines()
    data = ''.join(data).split(';')
    all_queries = [ x.strip() for x in data]
    run(all_queries)

def main(filename='test.csv', save_queries=True, print_queries=True, run_queries=True, run_commit=True ):
    all_queries = generate_sql_queries(filename)
    if save_queries:
        save_sql_queries(all_queries, 'queries.sql')
    if print_queries:
        print_sql_queries(all_queries)
    if run_queries:
        run(all_queries, run_commit)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("--cleanup", action="store_true", default=False,  help= "clear temp files if any present\n")
    #
    parser.add_option("--template", action="store_true", default=False, help= "creates generic server-update template")
    parser.add_option("--app_template", action="store_true", default=False, help= "creates application-template")
    parser.add_option("--rename_template", action="store_true", default=False, help= "creates a server rename-template")
    #
    parser.add_option("--query", action="store_true", default=False,  help= "generates sql query and stop\n")
    parser.add_option("--run", action="store_true", default=False,  help= "generates queries & direcly run to server \n")
    parser.add_option("--trail_run", action="store_true", default=False,  help= "does a trail run, generates sql queries & errors\n")
    parser.add_option("--run_sql", action="store_true", default=False,  help= "runs input sql query file \n") 
    #
    parser.add_option("--filename", action="store", dest="filename", help= "to give file input \n")
    

    (options, args)  =  parser.parse_args()
    # print ''
    # print(options)
    # print args
    #
    if options.filename:
        filename = str(options.filename)
        # print filename
    else:
        filename = 'test.csv'
        # print filename
    
    if options.cleanup :
        clean_up()
        print ("\n clean up completed")
    elif options.template :
        create_sample_file1(filename)            # templates
        print ("\n template create as %r" % filename)
    elif options.app_template :
        create_sample_file2(filename)            # application templates
        print ("\n application template create as %r" % filename)
    elif options.rename_template :
        create_sample_file3(filename)            # host-rename templates
        print ("\n host-rename template create as %r" % filename)
    elif options.query:
        # generates query and stops
        main(filename, save_queries=True, print_queries=False, run_queries=False, run_commit=True )
        print ("\n stored sql queries in %r" % 'queries.sql')
    elif options.trail_run:
        # generates query and generates possible error reprot
        main(filename, save_queries=True, print_queries=False, run_queries=False, run_commit=False ) # trail run
        print ("\n stored sql queries in %r" % 'queries.sql')
        print (" stored sql errors in %r" % 'queries.sql')
    elif options.run:
        # generates query and runs
        main(filename, save_queries=True, print_queries=False, run_queries=True, run_commit=True )
        print ("\n host-rename template create as %r" % filename)
    elif options.run_sql:
        # generates query and runs
        runfile(filename)
        print (" stored sql errors in %r" % 'queries.sql')




