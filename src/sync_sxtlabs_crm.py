from datetime import datetime
from dateutil import parser
from pysteve import pySteve 
from spaceandtime import SpaceAndTime, SxTExceptions, SXTTable

envfile = 'private.env' # change to your .env file

# setup logging
logger = pySteve.logger_setup('Notion-2-SXT')

# load notion key and tableid
vars = pySteve.envfile_load(envfile, docstring_marker_override='EOM')
notion_api_key = vars['NOTION_API_KEY']
sxt_schema = vars['SXTLABS_SCHEMA']
sxt_biscuit = vars['SXTLABS_BISCUIT']
sxt = SpaceAndTime(envfile_filepath=envfile, 
                   user_id=vars['USER_ID'], # TODO: should be picked up from envfile
                   application_name=logger.name, 
                   logger=logger)

# sorted_dict = dict(sorted(my_dict.items()))
prework_sqls =  dict(sorted({n:v for n,v in vars.items() if str(n).startswith('PRE')  and '_SQL_' in n}.items()))
midwork_sqls =  dict(sorted({n:v for n,v in vars.items() if str(n).startswith('MID')  and '_SQL_' in n}.items()))
postwork_sqls = dict(sorted({n:v for n,v in vars.items() if str(n).startswith('POST') and '_SQL_' in n}.items()))
all_biscuits =  [v for n,v in vars.items() if str(n).endswith('BISCUIT')]

# get list of all table names from SXT 
sxt.authenticate()
success, sxt_tables = sxt.discovery_get_tables(sxt_schema, search_pattern='CRM_%')
notion_tables = {n:v for n,v in vars.items() if n[:4]=='CRM_'}

if not success or len(sxt_tables) == 0:
    raise SxTExceptions.SxTQueryError('Error while connecting to the SXT network, or not CRM tables were found.')

# save all notion datasets before insert, so we can perform row ID/Title swap
final_kvdata = []
final_rowdatasets = {}
final_rowidtitles = []


# process all PRE-work queries:
for name, sql in prework_sqls.items():
    sxt.logger.info(f'Running Postwork Query: {name}')
    success, response = sxt.execute_query(sql, biscuits=all_biscuits)


### iterate thru each table def
for table_name, notion_id in notion_tables.items():
    
    sxt.logger.info('\n' + '-'*35 + f'\nProcessing Data for: {table_name}\n' + '-'*35)

    if table_name not in [t['table'] for t in sxt_tables.values()]: 
        sxt.logger.warning(f'SXT missing table: {table_name}... skipping')
        continue

    # get SXT metadata
    sxt.authenticate()
    success, sxt_columns = sxt.discovery_get_table_columns(sxt_schema, table_name)
    sxtcollist = [r['column'].lower() for r in sxt_columns]

    # connect to the SXT Table object and pull data 
    sxttable = SXTTable(f'{sxt_schema}.{table_name}', SpaceAndTime_parent=sxt)
    sxttable.biscuits.append(sxt_biscuit)
    success, sxt_data = sxttable.select(row_limit=10000) # TODO: extend SELECT to allow pagination
    delete_sxt_table = len(sxt_data) > 0

    # get Notion data + metadata
    notion_name = notion_data = notion_kvdata = notion_columns = None
    notion_name,  notion_data,  notion_kvdata,  notion_columns = pySteve.notionapi_get_dataset(notion_api_key, notion_id, row_limit=2000)
    for column in notion_columns: column['db_name'] = column['db_name'].lower()

    # if CRM_PEOPLE, add Notion_Users to the dataset 
    if table_name == 'CRM_PEOPLE':

        # get all internal users of notion, and add to CRM_People
        notion_users = pySteve.notionapi_get_users(notion_api_key)
        notion_users = [v for n,v in notion_users.items()] # from dict to list
        notion_users = [{'id':r['id'], 
                         'Person Name':r['name'], 
                         'Email':r['email'],
                         '🚀 Account Name':'15ae53de-c425-483d-a94b-7211485a8e87', 
                         'last_edited_time':'2020-01-01T00:00:00.000+00:00',
                         'Tags':'internal_notion_user',
                         '__notion_row_title__':r['name']} for r in notion_users]
        notion_data.extend(notion_users)

        # add Notion_Users data to Notion_KVData (manually)
        for user in notion_users: 
            for nm,val in user.items():
                if nm in ('id', '__notion_row_title__'): continue
                notion_kvdata.append({'Notion_DBName':'People', 
                                      'ColumnName':nm, 
                                      'ColumnType':'text', 
                                      'RowID':user['id'],
                                      'CellValue':val,
                                      'CellCount':1})
        
        # delete all users from SXT, as occasion needs 
        if False: sxt.execute_query(f"""DELETE FROM SXTLABS.CRM_PEOPLE WHERE ID in ( '{"', '".join([r['id'] for r in notion_users])}' )""", biscuits=all_biscuits)


    # keep a final list of all keyvalue data for ID/title find & replace below
    final_kvdata.extend(notion_kvdata) # hold for the end
    final_rowidtitles.extend([{'id':r['id'], 'title':r['__notion_row_title__']} for r in notion_data])

    # in the notion_data, replace the notion_names with the db_names,
    # and remove any that don't exist in the table
    #   TODO: extend pySteve to remove elements where value == ''
    #   TODO: add filter API by date (much more efficient)
    notion_newdata = [] 
    for row in notion_data:
        newrow = {'id':row['id']}
        for colname in notion_columns:
            if colname['notion_name'] in['parent','id','object'] or \
               colname['notion_name'] not in row.keys()          or \
               row[colname['notion_name']] in ('', None): continue
            if str(colname['db_name']).lower() in sxtcollist: # only add if col is in sxt_table
                newrow[colname['db_name']] = row[colname['notion_name']]

        # only append row if it's (a) missing from SXT data, or (b) last modify time is greater in notion
        sxtlastedited = [s['LAST_EDITED_TIME'] for s in sxt_data if s['ID'] == newrow['id']] 
        if len(sxtlastedited) <= 0: # <-- New Notion Row, add to DB
            notion_newdata.append(newrow) 
        else:  # <-- check for updates
            if not sxtlastedited[0]: continue # no last_edited_time in SXT, skip insert
            notion_record_time = parser.parse(newrow['last_edited_time'])
            sxtdb_record_time =  parser.parse(sxtlastedited[0])
            if notion_record_time == sxtdb_record_time: # No change, do nothing
                pass
            elif notion_record_time > sxtdb_record_time: # Notion record newer, so update DB
                notion_newdata.append(newrow) 
            elif sxtdb_record_time > notion_record_time: # TODO:  DB is newer, update Notion
                pass 
 
    sxt.logger.info(f'---> Rows to Update: {len(notion_newdata)}')

    # hold notion_newdata in collection of all tables, so we can replace id/title before insert
    final_rowdatasets[notion_name] = {'notion_newdata':notion_newdata, 'notion_alldata':notion_data,
                                      'sxttable':sxttable, 'sxttable_data':sxt_data}


# Loop thru any records we're going to insert, and find/replace ID for Title
# (must be done after all tables are pulled, to ensure we have all replace ids)
# this is going to be gross, sorry:
sxt.logger.info('Performing find/replace for any linked IDs with the actual name')
for notion_name, notion_obj in final_rowdatasets.items():
    notion_newdata = notion_obj['notion_newdata']
    sxt.logger.info(f'...{notion_name}')
    for rownum, rowdata in enumerate(notion_newdata):
        for colname, colvalue in rowdata.items():
            if not colvalue or len(colvalue)==0: continue
            if colname in ['id','parent_id']: continue
            newcolvalue = colvalue
            for idtitle in final_rowidtitles:
                if idtitle['id'] in colvalue:
                    newcolvalue = newcolvalue.replace(idtitle['id'],idtitle['title'])
            final_rowdatasets[notion_name]['notion_newdata'][rownum][colname] = newcolvalue


# iterate thru all datasets curated above and perform INSERT
sxt.logger.info('Performing SXT Inserts')
for notion_name, notion_obj in final_rowdatasets.items():
    notion_newdata = notion_obj['notion_newdata']
    sxttable = notion_obj['sxttable']
    sxt.logger.info(f'...{notion_name}')

    if len(notion_newdata) > 0:
        # delete any changed records
        if delete_sxt_table:
            sxttable.delete(where = f"""ID in ({', '.join([f"'{r['id']}'" for r in notion_newdata])})""")

        # insert new and changed records                
        sxttable.insert.with_list_of_dicts(list_of_dicts = notion_newdata)

# process all post-work queries:
for name, sql in postwork_sqls.items():
    sxt.logger.info(f'Running Postwork Query: {name}')
    success, response = sxt.execute_query(sql, biscuits=all_biscuits)


# at this point, SXT DB should have all new and changed records from notion

# TODO: write logic in the DB to perform UPDATES and select out those records:
#   if 1a qualified lead, and have had a meeting, change to 2a discovery initial meeting
#   if more than N-weeks pass since last meeting or last action update, move to fallout subphase (phase 5 or below)
# then update changes back to notion UI


# TODO: perform final KVData Insert
pass