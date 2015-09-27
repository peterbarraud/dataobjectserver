#!/usr/bin/python

import MySQLdb
import sys
import os


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
def remove_values_from_list(the_list, val):
   return [value for value in the_list if value != val]
   
def createobjectfiles(className) :
	classfilename = className + '.php'
	#only create file if it exists
	if (os.path.isfile(classfilename)) :
		print bcolors.WARNING + classfilename + ' file already exists. We are not overwriting' + bcolors.ENDC
	else :
		objectfile = open (classfilename,'w')	
		objectfile.write('<?php' + "\n")
		objectfile.write("require_once dirname(__FILE__) . '/common/objectbase.php';" + "\n")
		objectfile.write('class ' + className + ' extends objectbase {}' + "\n")
		objectfile.write('?>' + "\n") 
		objectfile.close()
	classcollectionfilename = className + 'collection.php'
	if (os.path.isfile(classcollectionfilename)) :
		print bcolors.WARNING + classcollectionfilename + ' file already exists. We are not overwriting' + bcolors.ENDC
	else :
		objectfile = open (classcollectionfilename,'w')	
		objectfile.write('<?php' + "\n")
		objectfile.write("require_once dirname(__FILE__) . '/common/collectionbase.php';" + "\n")
		objectfile.write('class ' + className + 'collection extends collectionbase {}' + "\n")
		objectfile.write('?>' + "\n")
		objectfile.close()
	print '==============**==============='
	

try :
	dbserver = raw_input("Enter the db server (Press Enter if it's localhost): ")
	dbuser = raw_input("Enter the db user (Press Enter if it's root): ")
	dbpwd = raw_input("Enter the db password: ")
	dbname = raw_input("Enter the db name: ")
	dbserver = 'localhost' if dbserver == '' else dbserver
	dbuser = 'root' if dbuser == '' else dbuser
	
	db = MySQLdb.connect(dbserver,dbuser,dbpwd,dbname);
	cursor = db.cursor()
	
	#this freaky query get all the tables in the given schema that do NOT have foreign keys defined in them 
	select_sql = "select basetable.table_name from information_schema.tables basetable where basetable.table_schema = '" + dbname + "' and basetable.table_name not in (select table_name FROM information_schema.KEY_COLUMN_USAGE ke WHERE   ke.referenced_table_name IS NOT NULL and ke.table_schema = '" + dbname + "');"
	print select_sql
	cursor.execute(select_sql)
	for row in cursor:
		print 'Creating object files for: ' + row[0]
		createobjectfiles(row[0])
		
		
	db.close()
	#and then finally create the db meta data file
	#this file is used by the php server for db access based on the db data provided in the user input in this script
	dbmetafile = open ('common/dbmetadata.json','w')
	dbmetafile.write('{')
	dbmetafile.write('"dbserver":"' + dbserver + ' ",')
	dbmetafile.write('"dbuser":"' + dbuser + '",')
	dbmetafile.write('"dbpwd":"' + dbpwd + '",')
	dbmetafile.write('"dbname":"' + dbname + '"')
	dbmetafile.write('}')
	dbmetafile.close()
except :
	print "Unexpected error:", sys.exc_info()

