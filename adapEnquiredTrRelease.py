#
# adapEnquiredTrRelease.py
# v.1.1 (16N9)
#
import ast
import csv
import json
from pymongo import MongoClient
from pprint import pprint
from bson import ObjectId
from bson.decimal128 import Decimal128
import dateutil
from datetime import datetime, timedelta
import sys
import time

old_stdout = sys.stdout
log_file = open("adapEnquiredTrRelease.log","a")
sys.stdout = log_file

print(f"\n[{time.strftime('%Y-%m-%d %H:%M')}] ADaP – Servizio di Sblocco Traffico v1.0 – inizio:")

uri="localhost:27017"

if not uri:
	print("\nERRORE: Dichiarare la stringa di connessione dentro una variabile uri all'interno del file\n")
	exit()
try:
	now=time.strftime("%Y-%m-%d %H:%M")
	print(f"[{now}] Connecting to", uri)
	client = MongoClient(uri)
except pymongo.errors.OperationFailure as err:
    print('ERRORE DI CONNESSIONE AL DB')
    print(err)
    exit()

db = client.adap

def get_log_documents():
	log_results5=db.LinesEnquiredLog.find({ "$or": [ { "data.blkStatus": "3" }, { "data.blkStatus": "4" } ] })
	log_results3=db.LinesEnquiredLog.find({ "$or": [ { "data.blkStatus": "3" }, { "data.blkStatus": "4" } ] })
	list_logs=list(log_results3)

	# COUNT DOCUMENTS.
	# counter=0
	# for index, item in enumerate(list_logs):
	# 	blkStatus=item['data']['blkStatus']
	# 	if blkStatus == 3 or blkStatus == "4":
	# 		counter+=1


	if len(list_logs)>0:

		for cursor in log_results5:
			cursor_id=cursor["_id"]

			if cursor["data"]["blkStatus"]=="3":
				if "rlsDate" in cursor["data"]:
					rls_date=cursor["data"]["rlsDate"]
				else:
					now=time.strftime("%Y-%m-%d %H:%M")
					print(f'[{now}] Errore - manca il campo rlsDate')
					continue

				# se la data è più vecchia di 60 giorni
				if datetime.now() > rls_date + timedelta(days=60):
					#print('\n* Trovata Utenza da sbloccare', rls_date)
					
					a={'_id': cursor_id, 'utenza': cursor['data']['lineNumber'], 'data_dal': cursor['data']['blkEnquiredTrStart'], 'data_al': cursor['data']["blkEnquiredTrEnd"] }
					now=time.strftime("%Y-%m-%d %H:%M")
					print(f"[{now}] LinesEnquiredLog: trovata richiesta di sblocco valida _id: {a['_id']} (utenza: {a['utenza']}, data dal: {a['data_dal']}, data al: {a['data_al']})")
					#exit()
					
					 
					query_filter_delete={ "data.blkLockId": cursor_id }
					results_to_delete=db.LinesEnquired.find(query_filter_delete)


					res_array=list(results_to_delete)
					
					if len(res_array)>0:
						# caso in cui ci sono multipli Documents.
						for result in enumerate(res_array):
							#print('\nTrovato Documento, id del doc da cancellare: ',result["_id"],'id del log doc ', result["data"]["blkLockId"])
							deleted_result=db.LinesEnquired.delete_one(query_filter_delete)
							
							if deleted_result.deleted_count>0:
								#print('\n* Deleted con successo', deleted_result.deleted_count,'L.ENQ documenti')
								update_data={ "data.blkStatus": "4", "data.rlsDelDate": datetime.now()}
								update_result=db.LinesEnquiredLog.update_one({"_id": cursor_id }, {"$set": update_data}, upsert=False)
								if update_result.matched_count>0:
									now=time.strftime("%Y-%m-%d %H:%M")
									print(f'[{now}] sblocco traffico eseguito con successo')
									#print('\n*Updated', update_result.modified_count, 'L.ENQ.L Documents:')
							else:
								now=time.strftime("%Y-%m-%d %H:%M")
								print(f'[{now}] Delete query ha generato errore')
								#print('* Delete andato in errore')
								update_data={ "data.blkStatus": "5", "data.rlsDelDate": datetime.now(), "data.rlsErrCode": -512, "data.rlsErrDes": "PyMongo Error" }
								update_result=db.LinesEnquiredLog.update_one({"_id": cursor_id }, {"$set": update_data}, upsert=False)
								if update_result.matched_count>0:
									print(f'[{now}] Updated con errore.')
									#print('\n* Updated', update_result.modified_count, 'L.ENQ.L Documents con blkStatus: 5 (error)')
					else:
						now=time.strftime("%Y-%m-%d %H:%M")
						print(f"[{now}] Richiesta valida di sblocco con _id: {a['_id']} (utenza: {a['utenza']}, data dal: {a['data_dal']}, data al: {a['data_al']}) non ha trovato Documenti corrispondenti in LinesEnquired")


				else:
					continue
					#print('La richiesta con blkStatus: 3 NON è più vecchia di 60 giorni')
			
			elif cursor["data"]["blkStatus"]=="4":

				
				if datetime.now() - timedelta(days=365) > cursor["data"]["rlsDate"]:
					
					#print('Trovato Documento più vecchio di un anno con data',cursor["data"]["rlsDate"])
					a={'_id': cursor_id, 'utenza': cursor['data']['lineNumber'], 'data_dal': cursor['data']['blkEnquiredTrStart'], 'data_al': cursor['data']["blkEnquiredTrEnd"] }
					now=time.strftime("%Y-%m-%d %H:%M")
					print(f"[{now}] LinesEnquiredLog: trovata richiesta di blocco sblocco DA CANCELLARE _id: {a['_id']} (utenza: {a['utenza']}, data dal: {a['data_dal']}, data al: {a['data_al']})")		
					delete_document_result=db.LinesEnquiredLog.delete_one({ "_id" : cursor_id })

					if delete_document_result.deleted_count>0:
						print(f'[{now}] cancellazione richiesta eseguita con successo')
						#print('Deleted',delete_document_result.deleted_count,'Documents in LinesEnquiredLog')
					else:
						print('Cancellazione richiesta genera errore', delete_document_result)

				else:
					#print(f"[{time.strftime('%Y-%m-%d %H:%M')}] Delta inferiore ad un anno")
					continue
					#print('Trovato Doc con blkStatus:4, MA NON più vecchio di un anno')

			else:
				print(f'[{now}] Il campo blkStatus non corrisponde a 3 o 4')
				continue 
				#print('Trovato Document che non ha blkStatus 3 o 4')

	else:
		now=time.strftime('%Y-%m-%d %H:%M')
		print(f'[{now}] LinesEnquiredLog: non trovata alcuna richiesta di sblocco')
		exit()
	
	now=time.strftime("%Y-%m-%d %H:%M")
	print(f'[{now}] ADaP – Servizio di Sblocco Traffico – fine')
	

get_log_documents()

sys.stdout = old_stdout
log_file.close()

exit()