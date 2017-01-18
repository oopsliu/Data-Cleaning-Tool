## -*- coding: utf-8 -*-
##
##Data cleaning for big data tools(GeoAnalytics tools)
##Author：LIU Zheng
##email: liuz@esrichina.com.cn
##Date：2016-Nov-16

import pandas
import uuid
import datetime
import os
import glob
import gzip
import sys

# gz Path
# gzipPath = "E:\\Huawei\\"
gzipPath = sys.argv[1]

# Export result path
# resultPath = "E:\\result\\"
resultPath = sys.argv[2]
if not os.path.exists(resultPath):
	os.makedirs(resultPath)

# If data has header row
# hasHeader = "T"
hasHeader = sys.argv[3]


# Print messages
def msg(message):
	print (datetime.datetime.now(), ":  ",message)
	
# Unzip temp path
tempUnzip = os.path.join(resultPath,"temp")
if not os.path.exists(tempUnzip):
	os.makedirs(tempUnzip)
tempUnzipFile = os.path.join(tempUnzip,"temp.csv")
	
# Unzip *.gz
def inputDirectory( path ):
	for fn in glob.glob( path + os.sep + '*.gz' ):
		if os.path.isdir( fn ):
			inputDirectory( fn )
		else:
			Unzip(fn)
			
def Unzip(path):
	msg("Unzipping " + path)
	global startTime
	startTime = datetime.datetime.now()
	f = gzip.open(path,"rb")
	file_content = f.read()

	ftemp = open(tempUnzipFile,"wb")
	ftemp.write(file_content)

	tempName = path
	head, tail = os.path.split(tempName)
	fileName = (tail.split(".gz")[0])

	global outPath
	outPath = os.path.join(resultPath,fileName)

	ReadCSV(tempUnzipFile)
		
# Read-in csv, add header row if not exists
def ReadCSV(inputCSV):
	df = pandas.read_csv(inputCSV)
	if hasHeader == "T":
		ProcessData(df)
	elif hasHeader == "F":
		msg("Adding header row")
		df = pandas.read_csv(inputCSV, header=None)
		df.columns = ['MmeUeS1apId', 'Latitude','Longitude','TimeStamp','LteScRSRP','LteScRSRQ','LteScTadv']
		ProcessData(df)
	else:
		msg ("invalid parameter 'hasHeaderRow'(T/F)")

# Process data
def ProcessData(dataFrame):
	df = dataFrame
 
	# Select columns
	msg("Selecting columns")
	selectCols = df[['MmeUeS1apId', 'Latitude','Longitude','TimeStamp']]

	# Delete rows by query condition
	msg("Deleting rows with query")
	removeRows = selectCols[(selectCols.Latitude != 0) & (selectCols.Longitude != 0)]
	removeRows.is_copy = False

	# Timestamp to datetime
	msg("Converting timestamp")
	removeRows['TimeStamp'] = pandas.to_datetime(removeRows['TimeStamp'], unit = 'ms')

	# Add UUID
	msg("Adding UUID")
	for i, row in removeRows.iterrows():
		removeRows.set_value(i, 'UUID',uuid.uuid4())
		
	msg("Wrting to " + outPath)
	removeRows.to_csv(outPath, sep=',', index=False)

	global endTime
	endTime = datetime.datetime.now()
	global timeSpan
	timeSpan = endTime - startTime
	
	msg("---- Finish： " + outPath + " | This took:" + str(timeSpan.total_seconds()) + "s ----")


if __name__ == '__main__':
	inputDirectory(gzipPath)

