import pymarc
from pymarc import JSONReader, Field, JSONWriter, XMLWriter
import psycopg2
import psycopg2.extras
import time
import logging
import json

#WRITTEN W/PYTHON 3.7.3


print("...starting export");

# constructing file and log name
timestr = time.strftime("%Y%m%d-%H%M%S")
logging.basicConfig(filename=timestr + "-export.log")

#PRODUCTION DB
DATABASE_HOST = "redacted"
DATABASE_USERNAME = "redacted"
DATABASE_PASSWORD = "redacted"
DATABASE_PORT = 5432
DATABASE_NAME = "redacted"
TENANT = "redacted"

#PRE PRODUCTION DB
#DATABASE_HOST = "redacted"
#DATABASE_USERNAME = "redacted"
#DATABASE_PASSWORD = "redacted"
#DATABASE_PORT = 5432
#DATABASE_NAME = "redacted"
#TENANT = "redacted"

count = 0
folio_db = psycopg2.connect(
	user=DATABASE_USERNAME,
	password=DATABASE_PASSWORD,
	host=DATABASE_HOST,
	port=DATABASE_PORT,
	database=DATABASE_NAME
)

#init a list of material types
materialTypeLookup = {}
matCursor = folio_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
select_all_mat = '''
select id, jsonb->>'name' as name from {}_mod_inventory_storage.material_type'''.format(TENANT)
matCursor.execute(select_all_mat)
materialTypes = matCursor.fetchall()
for m in materialTypes:
    materialTypeLookup[m['id']] = m['name']

#init a list of locations 
locLookup = {}
locCursor = folio_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
select_all_loc = '''
select id, jsonb->>'name' as name from {}_mod_inventory_storage.location'''.format(TENANT)

locCursor.execute(select_all_loc)
locations = locCursor.fetchall()
for l in locations:
    locLookup[l['id']] = l['name']

#init a list of call number types
callNoTypeLookup = {}
callNoTypeCursor = folio_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
select_all_call_no_types = '''
select id, jsonb->>'name' as name from {}_mod_inventory_storage.call_number_type'''.format(TENANT)
callNoTypeCursor.execute(select_all_call_no_types)
callNoTypes = callNoTypeCursor.fetchall()
for c in callNoTypes:
    callNoTypeLookup[c['id']] = c['name']

cursor = folio_db.cursor(name='folio',cursor_factory=psycopg2.extras.DictCursor)
#THIS COULD BE MODIFIED TO RETREIVE X NUMBER OF RECORDS PER FILE
cursor.itersize=300000
#from {}_mod_marc_storage.marc_record'''.format(TENANT)

#select_ids_sql = '''
#select
#id, 
#instance_id 
#from {}_mod_source_record_storage.records_lb where state = {} and (suppress_discovery = False or suppress_discovery is null)'''.format(TENANT,"'ACTUAL'")

select_ids_sql = '''
select
id,
suppress_discovery, 
instance_id 
from {}_mod_source_record_storage.records_lb where state = {}'''.format(TENANT,"'ACTUAL'")

#select_ids_sql = '''
#select
#id,
#suppress_discovery, 
#instance_id 
#from {}_mod_source_record_storage.records_lb where instance_id = '5101ae83-6a0b-3ede-a170-52a8705a805f' AND state = {}'''.format(TENANT,"'ACTUAL'")

print("executing query")
cursor.execute(select_ids_sql)

while True:
	print("in the while true - fetching...")
	rows = cursor.fetchmany(cursor.itersize)
	print("fetch is done")
	marcRecordCursor = folio_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
	if rows:
		save_file = timestr + "." + str(count) + ".json"
		writer = open(save_file,'wt')
		print("created the file: " + save_file)
		count += 1
		for row in rows:
			try: 
				rowId = row['id'];
				rowInstanceId = row['instance_id'];
				if rowInstanceId == None:
						logging.error("BAD RECORD: INSTANCE ID WAS NULL" + str(row))
						continue
				select_record_sql = '''
				select id, 
				content as marc
				from {}_mod_source_record_storage.marc_records_lb where 
				id = '{}' limit 1'''.format(TENANT, rowId)
				#print(select_record_sql)
				marcRecordCursor.execute(select_record_sql)
				marcRow = marcRecordCursor.fetchone()
				marcJsonAsString = json.dumps(marcRow['marc'])
				marcString = marcJsonAsString.encode('utf-8').strip()
				#print(marcJsonAsString);
                
				for record in JSONReader(marcJsonAsString):
					discoverySuppressItem = "0"
					discoverySuppressHold = "0"
					discoverySuppress = "1"                
					#write MARC JSON to output file
					#ADD A 998 FOR EACH HOLDING RECORD
					if record['6xx'] is not None:
						logging.error("BAD RECORD: 6xx" + str(row))
						continue
					if record['4xx'] is not None:
						logging.error("BAD RECORD: 4xx" + str(row))
						continue
#					select_holding_sql = '''
#					select id, creation_date, callnumbertypeid, 
#					jsonb->>'permanentLocationId' as permanentlocationid, 
#					jsonb->'holdingsStatements' as holdingstatements,
#					jsonb->>'callNumber' as callNumber from 
#					{}_mod_inventory_storage.holdings_record 
#					where instanceid = '{}' and (jsonb->>'discoverySuppress'='false' or jsonb->>'discoverySuppress' is null)'''.format(TENANT,rowInstanceId)
                    
					select_holding_sql = '''
					select id, creation_date, callnumbertypeid, 
					jsonb->>'permanentLocationId' as permanentlocationid, 
					jsonb->'holdingsStatements' as holdingstatements,
					jsonb->>'callNumber' as callNumber, 
                    jsonb->>'discoverySuppress' as discoverySuppress from 
					{}_mod_inventory_storage.holdings_record 
					where instanceid = '{}' '''.format(TENANT,rowInstanceId)
                    
#					print(select_holding_sql)
					marcRecordCursor.execute(select_holding_sql)
					holdingRows = marcRecordCursor.fetchall()
                    
					for holding in holdingRows:
					
#						print("Begin Holding Record:  Holding Suppressed 1=YES, 0=NO: " + holding[6])            
                    
						discoverySuppressHold = "0"
                        
						if not (holding[6] is None):
							tmp_hold_supress = holding[6]
							if (tmp_hold_supress == "true"):
#								print("I should be supressed Set Hold DS = 1")
								discoverySuppressHold = "1"


						if (discoverySuppressHold != "1"):
							discoverySuppress = "0"
							holdingsStatements = holding['holdingstatements']
							rowHoldingsId = holding['id']
							newField = Field(tag = '998',
									  indicators = [' ',' '],
									  subfields = ['a',holding.get('callnumber',''),
												'l',locLookup.get(holding.get('permanentlocationid',''),'')])
                                            
							for statement in holdingsStatements:
								if statement is not None:
									newField.add_subfield('s',statement.get('statement','').replace('Extent of ownership:',''));
                                
                                
							record.add_field(newField)
							#ADD AN 952 FOR EACH ITEM
							select_item_sql = '''
							select id, materialtypeid, 
							jsonb->>'effectiveLocationId' as effectivelocationid, 
							jsonb->>'barcode' as barcode, 
							jsonb->'effectiveCallNumberComponents'->>'prefix' as prefix,
							jsonb->'effectiveCallNumberComponents'->>'typeId' as callnotype,
							jsonb->'effectiveCallNumberComponents'->>'callNumber' as callnumber,
							jsonb->'effectiveCallNumberComponents'->>'discoverySuppress' as discoverySuppress
							from {}_mod_inventory_storage.item where 
							holdingsrecordid = '{}' '''.format(TENANT,rowHoldingsId)
#							print(select_item_sql)
							marcRecordCursor.execute(select_item_sql)
							itemRows = marcRecordCursor.fetchall()
							for item in itemRows:

								discoverySuppressItem = "0"
#								if not (item[7] is None):
#									print("Item Suppressed 1=YES 0=NO:" +item[7]) 
                            
								if not (item[7] is None):
									tmp_item_supress = item[7]
									if (tmp_item_supress == "true"):
#										print("I should be supressed Set Item DS = 1")                                
										discoverySuppressItem = "1"
                        
								callNoToUse = item.get('callnumber','na')
								#print(callNoToUse)
								prefix = item.get('prefix',None)

									
                            
								if (prefix is not None):
									callNoToUse = prefix + " " + callNoToUse

#								print('discoverySuppressItem: ' + discoverySuppressItem)
#								print('discoverySuppressHold: ' + discoverySuppressHold)
#								print('discoverySuppress: ' + discoverySuppress)

#								print('Add 952 Field')
                            
								record.add_field(
									Field(tag = '952',
										indicators = [' ',' '],
										subfields = ['m',item.get('barcode',''),
										'j',callNoTypeLookup.get(item.get('callnotype',''),''),
										'd',locLookup.get(item.get('effectivelocationid'),''),
										'i',materialTypeLookup.get(item.get('materialtypeid'),''),
										'e',callNoToUse]))
								if (len(record.leader) < 24):
									logging.error("BAD LEADER" + record.leader + " " + str(row))
									record.leader = "{:<24}".format(record.leader)

#								print('End Add 952 Field')

#						print("End Holding Record")            
					
                    #ADD AN 999t FOR EACH ITEM to show it is not suppressed
					record['999'].add_subfield('t',str(discoverySuppress))
					# DCH 

					writer.write(record.as_json())
					writer.write('\n')
                    
			except Exception as e:
					print("ERROR PROCESSING ROW:" + str(row))
					print(e)
					if rowInstanceId == None:
						rowInstanceId = "None" #FOR LOGGING
					logging.error("UNABLE TO WRITE TO FILE: " + rowInstanceId)
					logging.error(e)
					continue
		writer.close()
	else:
		print("in the else --> finishing")
		break

if (folio_db):
	cursor.close()
	marcRecordCursor.close()
	folio_db.close()
	print("complete")
