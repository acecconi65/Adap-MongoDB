#
# adapEnquiredTrBlock.py
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
import time
import sys

old_stdout = sys.stdout
log_file = open("adapEnquiredTrBlock.log","a") # was w
sys.stdout = log_file

print(f"\n[{time.strftime('%Y-%m-%d %H:%M')}] ADaP – Servizio di Blocco Traffico v1.0 – inizio:")

uri='localhost:27017'

if not uri:
	print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] ERRORE: Dichiarare la stringa di connessione dentro una variabile uri all'interno del file")
	#print("\nERRORE: Dichiarare la stringa di connessione dentro una variabile uri all'interno del file\n")
	exit()
try:
	print(f'\n[{time.strftime("%Y-%m-%d %H:%M:%S")}] Connecting to {uri}')
	#print('Connecting to..', uri)
	client = MongoClient(uri)
except pymongo.errors.OperationFailure as err:
	print(f'\n[{time.strftime("%Y-%m-%d %H:%M:%S")}] Errore di connessione al DB {err}')
    #print('ERRORE DI CONNESSIONE AL DB')
    #print(err)
    #exit()

db = client.adap


def gestione_errore(error_spec, _id):	
	update_error_item={}
	update_error_item['data']={}
	update_error_item['data']['blkStatus'] = "5"

	error_string=error_spec["tipo"]
	error_string_sub=error_spec["sub_tipo"]

	if error_string=='A':
		campo_mancante=error_spec["campo_mancante"]

	if error_string=='A':
		update_error_item['data']['blkErrCod'] = -502

		# CASE 1
		if error_string_sub=="valori":
			update_error_item['data']['blkErrDes'] = f"Formato input errato {campo_mancante} non valorizzato"
			print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] LinesEnquiredLog: trovata richiesta di blocco NON valida _id {_id} ERRORE: {update_error_item['data']['blkErrDes']}")

		# CASE 2
		if error_string_sub=="lunghezza":
			update_error_item['data']['blkErrDes'] = "Formato input errato campo lungo più di lunghezza massima)"
			print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] LinesEnquiredLog: trovata richiesta di blocco NON valida _id {_id} ERRORE: {update_error_item['data']['blkErrDes']}")

		# CASE 3
		if error_string_sub=="data":
			update_error_item['data']['blkErrDes'] = f"Formato input errato: non è una data {campo_mancante}"
			print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] LinesEnquiredLog: trovata richiesta di blocco NON valida _id {_id} ERRORE: {update_error_item['data']['blkErrDes']}")

	if error_string=='B':

		#print('ERRORE B')
		update_error_item['data']['blkErrCod'] = -503
		update_error_item['data']['blkErrDes'] ="Dati input errati: DataInizio maggiore di DataFine"
		print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] LinesEnquiredLog: trovata richiesta di blocco NON valida _id {_id} ERRORE: {update_error_item['data']['blkErrDes']}")

	if error_string=='C':

		#print('ERRORE C')
		update_error_item['data']['blkErrCod'] = -504
		update_error_item['data']['blkErrDes'] = "Dati input errati: Intervallo di ricerca fuori range!"
		print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] LinesEnquiredLog: trovata richiesta di blocco NON valida _id {_id} ERRORE: {update_error_item['data']['blkErrDes']}")

	if error_string=='D':

		#print('ERRORE D')
		update_error_item['data']['blkErrCod'] = -505
		update_error_item['data']['blkErrDes'] = "Dati input errati: DataInizio maggiore della Data Odierna"
		print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] LinesEnquiredLog: trovata richiesta di blocco NON valida _id {_id} ERRORE: {update_error_item['data']['blkErrDes']}")

	# UPDATE
	update_result=db.LinesEnquiredLog.update_one({"_id": _id }, {"$set": update_error_item}, upsert=False)	
	if update_result:
		print('')
		#print('* Errore nel Log Document: aggiornato Document con errore *', update_result)


def validate_date(date_text):
    date_text=date_text.strftime('%Y-%m-%d')
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
        #return True
    except ValueError:
    	raise ValueError("Incorrect data format, should be YYYY-MM-DD")
    else:
    	return True

def check_campi_obbligatori(cursor):
	campi_obbligatori=['blkSubSys', 'blkCodOpr', 'blkCodEse', 'lineNumber', 'blkEnquiredTrStart', 'blkEnquiredTrEnd']
	for campo in campi_obbligatori:
		# try .get(['key'])
		try:
			if not cursor['data'][campo]:
				#print('MISSING A FIELD')
				return True
		except KeyError:
			#pass
			return True

# QUERY (get) LOG documents (ADD gestione errore se ricevi zero risultati)
log_results=db.LinesEnquiredLog.find({ "data.blkStatus": "1" })
queries=[]

# CHECK ERRORI nei dati che ricevi dalla collection LinesEnquiredLog.
for cursor in log_results:
	# serve per le update call(s)
	cursor_id=cursor["_id"]

	# CONTROLLO CHE CI SONO TUTTI I CAMPI:
	if check_campi_obbligatori(cursor):

		#print('\nManca un campo, update() con errore\n')
		error_spec={ "tipo": "A", "sub_tipo": "valori", "campo_mancante": "blkSubSys"}
		gestione_errore(error_spec, cursor_id)
		continue# move to the next cursor (test this with multiple cursors in db)

	# CASO A
	if len(cursor['data']['blkSubSys'])>10:
		
		error_spec={ "tipo": "A", "sub_tipo": "lunghezza", "campo_mancante": "blkSubSys"}
		gestione_errore(error_spec, cursor_id)
		continue# move to the next cursor (test this with multiple cursors in db)

	if len(cursor['data']['blkCodOpr'])>12:
		error_spec={ "tipo": "A", "sub_tipo": "lunghezza", "campo_mancante": "blkSubSys"}
		gestione_errore(error_spec, cursor_id)
		continue# move to the next cursor (test this with multiple cursors in db)

	if len(cursor['data']['blkCodEse'])>15:
		error_spec={ "tipo": "A", "sub_tipo": "lunghezza", "campo_mancante": "blkSubSys"}
		gestione_errore(error_spec, cursor_id)
		continue# move to the next cursor (test this with multiple cursors in db)

	if len(cursor['data']['lineNumber'])>20:
		error_spec={ "tipo": "A", "sub_tipo": "lunghezza", "campo_mancante": "blkSubSys"}
		gestione_errore(error_spec, cursor_id)
		continue# move to the next cursor (test this with multiple cursors in db)

	if cursor['data']['blkEnquiredTrStart']:

		date=cursor['data']['blkEnquiredTrStart']
		# è una data? valida -> ritorna True/False?

		if isinstance(date, datetime):
			if not validate_date(date):
				error_spec={ "tipo": "A", "sub_tipo": "data"}
				gestione_errore(error_spec, cursor_id)
				continue# move to the next cursor (test this with multiple cursors in db)
			else:
				print('')
		else:
			#print('DATA non è neanche formato data, è qualcosaltro.')
			error_spec={ "tipo": "A", "sub_tipo": "data", "campo_mancante": "-data non in formato data-"}
			gestione_errore(error_spec, cursor_id)
			continue# move to the next cursor (test this with multiple cursors in db)
			

	if cursor['data']['blkEnquiredTrEnd']:

		date=cursor['data']['blkEnquiredTrEnd']
		# è una data? valida -> ritorna True/False?

		if isinstance(date, datetime):
			if not validate_date(date):
				#print('ERRORE di formato data, unvalidated, update() w error')
				error_spec={ "tipo": "A", "sub_tipo": "data"}
				gestione_errore(error_spec, cursor_id)
				continue# move to the next cursor (test this with multiple cursors in db)
			else:
				print('')
		else:
			#print('trEnd non è neanche formato data, è qualcosaltro.')
			error_spec={ "tipo": "A", "sub_tipo": "data", "campo_mancante": "trEnd non in formato data"}
			gestione_errore(error_spec, cursor_id)
			continue# move to the next cursor (test this with multiple cursors in db)
		
	# CASO B:
	if cursor['data']['blkEnquiredTrStart'] > cursor['data']['blkEnquiredTrEnd']:
		error_spec={ "tipo": "B", "sub_tipo": "", "campo_mancante": "blkEnquiredTrStart è dopo blkEnquiredTrEnd"}
		gestione_errore(error_spec, cursor_id)
		continue# move to the next cursor (test this with multiple cursors in db)
	
	# CASO C: (confronta con data di sistema):
	data_6_mesi_fa=datetime.now() - timedelta(days=180)
	if data_6_mesi_fa > cursor['data']['blkEnquiredTrStart']:	
		error_spec={ "tipo": "C", "sub_tipo": ""}
		gestione_errore(error_spec, cursor_id)
		continue# move to the next cursor (test this with multiple cursors in db)

	# CASO D:
	if cursor['data']['blkEnquiredTrStart'] > datetime.now():
		error_spec={ "tipo": "D", "sub_tipo": ""}
		gestione_errore(error_spec, cursor_id)
		continue# move to the next cursor (test this with multiple cursors in db)

	# SE È PRIVO DI ERRORI:
	else:
		item={
		"log_result": cursor,
		"filter": {
		"data.lineNumber": cursor["data"]["lineNumber"]
#		"data.eventDate": cursor["data"]["eventDate"]
		} 
		}
		queries.append(item)


if len(queries)>0:
	# Per ogni risultato (log _id) con un certo lineNumber, cerca tutti i docs in L. Enh.,
	for item in queries:
		print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] LinesEnquiredLog: trovata richiesta di blocco valida _id: {item['log_result']['_id']} (utenza: {item['filter']['data.lineNumber']}, data dal: {item['log_result']['data']['blkEnquiredTrStart']}, data al: {item['log_result']['data']['blkEnquiredTrEnd']})")

		# troverà documenti multipli per questo numero. 
		numero_di_telefono=item['filter']["data.lineNumber"]
		date_start=item['log_result']['data']['blkEnquiredTrStart'] 
		date_end=item['log_result']['data']['blkEnquiredTrEnd']
		
		query_numero_di_telefono={ "data.lineNumber": numero_di_telefono, 'data.eventDate': { '$gte': date_start, '$lte': date_end } }
		results_query_utenza=db.LinesEnhanced.find(query_numero_di_telefono)
		items=list(results_query_utenza)
		
		#print("\n* Trovati", len(items), 'Documenti per utenza e DATE *', numero_di_telefono)
		
		# Se trova Documenti in LinesEnhanced:
		if len(items) > 0:
			log_id=item['log_result']['data']['blkIdLock']}
			copy_date_start=datetime.now()
			number_of_docs=0
			total_cartellini=0
			
			# Per ogni documento trovato in LinesEnh., inserisci in LinesEnquired.
			for cursor in items:
				cursor.pop("_id")
				cursor["data"]["blkLockId"]=log_id
				number_of_cartellini=len(cursor["data"]["trEventList"])
				copy_date_end=datetime.now()
				result_=db.LinesEnquired.insert_one(cursor)

				if result_:
					number_of_docs+=1
					total_cartellini+=number_of_cartellini

					# UPDATE LOG DOC.
					update_data={
					"data.blkStatus": "2",
					"data.blkCopyDateStart": copy_date_start,
					"data.blkCopyDateEnd": copy_date_end,
					"data.blkCopiedDocs":  number_of_docs,
					"data.blkCopiedCdrs":  number_of_cartellini
					}
					update_result=db.LinesEnquiredLog.update_one({"_id": log_id }, {"$set": update_data}, upsert=False)
					
					if update_result:
						print(f'\n[{time.strftime("%Y-%m-%d %H:%M:%S")}] blocco traffico eseguito con successo')
						#print('\n* LinesEnquiredLog Document Updated: * ',update_result)
					
				else:
					result_='PyMongo Error: insert() has returned an error.'
					print('ERROR: insert() failed.', result_)
					update_error_item={
					'data.blkStatus': "5",
					'data.blkErrCode': -511,
					'data.blkErrDes': result_, 
					'data.blkCopyDate': copy_date_end,
					'data.blkCopiedDocs': number_of_docs,
					'data.blkCopiedCdrs': total_cartellini
					}
					update_result=db.LinesEnquiredLog.update_one({"_id": item['log_result']['_id'] }, {"$set": update_error_item}, upsert=False)
					if update_result:
						print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] LinesEnquired: Copia Fallita per log item con _id', {item['log_result']['_id']}")
						#print('* Copia Fallita per log item con _id', item['log_result']['_id'], '\nupdate result:', update_result)

		# SE NON HA TROVATO Utenza
		else:
			update_error_item={
			'data.blkStatus':"5",
			'data.blkErrCod': 510,
			'data.blkErrDes': "Utenza da bloccare non trovata"
			}
			update_result=db.LinesEnquiredLog.update_one({"_id": item['log_result']['_id'] }, {"$set": update_error_item}, upsert=False)
			if update_result:
				print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] LinesEnquiredLog: non trovata alcuna utenza per il Document con _id {item['log_result']['_id']}")
				#print('* Utenza non trovata per log item con _id', item['log_result']['_id'], '\nupdate result:', update_result)
				continue
# NULLA DA COPIARE
else:
	print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] LinesEnquiredLog: non trovata alcuna richiesta di blocco")

print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] ADaP – Servizio di Blocco Traffico – fine")

sys.stdout = old_stdout
log_file.close()

exit()