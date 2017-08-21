# /usr/bin/env python3
import sqlite3 as lite
import csv
import argparse
import collections
import glob
import os
import pandas
import re
import string
import sys

con = lite.connect('cs1656.sqlite')

with con:
	cur = con.cursor()

	########################################################################
	### CREATE TABLES ######################################################
	########################################################################
	cur.execute('DROP TABLE IF EXISTS AsianAmericans')
	cur.execute("CREATE TABLE AsianAmericans(id TEXT, id2 INT, zip INT, Geography TEXT, TotalGroupsTallied INT, ErrorTotalGroupsTallied INT, AsianIndian INT, ErrorAsianIndian INT, Bangladeshi INT, ErrorBangladeshi INT, Bhutanese INT, ErrorBhutanese, Burmese INT, ErrorBurmese, Cambodian INT, ErrorCamodian, Chinese INT,ErrorChinese INT, Filipino INT, ErrorFilipino INT, Hmong INT, ErrorHmong INT, Indoesian INT, ErrorIndoesian INT, Japanese INT, ErrorJapanese INT, Korean INT, ErrorKorean INT, Laotian INT, ErrorLaotian INT, Malaysian INT, ErrorMalaysian, Mongolian INT, ErrorMongolian INT, Nepalese INT, ErrorNepalese INT, Okinawan INT, ErrorOkinawan INT, Pakistani INT, ErrorPakistani INT, SriLankan INT, ErrorSriLankan INT, Taiwanese INT, ErrorTaiwanese INT, Thai INT, ErrorThai INT, Vietnamese INT, ErrorVietnamese INT, OtherSpecified INT, ErrorOtherSpecified INT, OtherNotSpecified INT, ErrorOtherNotSpecified INT, PRIMARY KEY(id2))")

	########################################################################
	### READ DATA FROM FILES ###############################################
	########################################################################
	with open('Asian Categories by Zip Code.csv', 'r') as fileActors:
		dr = csv.reader(fileActors)
		for row in dr:
			insert = list(row)
			cur.execute("INSERT INTO AsianAmericans VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", insert)
	con.commit()

	########################################################################
	### QUERY SECTION ######################################################
	########################################################################
	queries = {}

	########################################################################
	### INSERT YOUR QUERIES HERE ###########################################
	########################################################################

	queries['clean'] = '''
		DELETE
		FROM AsianAmericans
		WHERE zip < 15001 OR zip > 16199
	'''

	#Verion of trim for non-sqlite
	#ALTER TABLE AsianAmericans DROP COLUMN id, Geography, TotalGroupsTallied, ErrorTotalGroupsTallied, Cambodian, ErrorCamodian, Chinese, ErrorChinese, Filipino, ErrorFilipino, Hmong, ErrorHmong, Indoesian, ErrorIndoesian, Japanese, ErrorJapanese, Korean, ErrorKorean, Laotian, ErrorLaotian, Malaysian, ErrorMalaysian, Mongolian, ErrorMalaysian, Okinawan, ErrorOkinawan, Taiwanese, ErrorTaiwanese, Thai, ErrorThai, Vietnamese, ErrorVietnamese, OtherSpecified, ErrorOtherSpecified, OtherNotSpecified, ErrorOtherNotSpecified
	queries['trim'] = '''
			ALTER TABLE AsianAmericans RENAME TO temp
			CREATE TABLE AsianAmericans(id2 INT, zip INT, AsianIndian INT, Bangladeshi INT, Bhutanese INT, Nepalese INT, Pakistani INT, SriLankan INT, PRIMARY KEY(id2))
			INSERT INTO AsianAmericans
			SELECT id2, zip, AsianIndian, Bangladeshi, Bhutanese, Nepalese, Pakistani, SriLankan
			FROM temp
			DROP TABLE temp

		'''

	########################################################################
	### SAVE RESULTS TO FILES ##############################################
	########################################################################
	# DO NOT MODIFY - START
	for (qkey, qstring) in sorted(queries.items()):
		try:
			cur.execute(qstring)
			all_rows = cur.fetchall()

			print ("=========== ",qkey," QUERY ======================")
			print (qstring)
			print ("=========== ",qkey," RESULTS ====================")
			for row in all_rows:
				print (row)
			print (" ")

			with open(qkey+'.csv', 'w') as f:
				writer = csv.writer(f)
				writer.writerows(all_rows)
				f.close()

		except lite.Error as e:
			print ("An error occurred:", e.args[0])
	# DO NOT MODIFY - END
