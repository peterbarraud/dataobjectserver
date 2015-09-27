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

try :
	print bcolors.HEADER + 'enter your mysql login credentials' + bcolors.ENDC
	dbserver = raw_input("Enter the db server (Press Enter if it's localhost): ")
	dbrootuser = raw_input("Enter the mysql root user (Press Enter if it's root): ")
	dbrootpwd = raw_input("Enter the mysql root password: ")
	print bcolors.HEADER + 'now enter the new db and credentials you want to create' + bcolors.ENDC
	dbname = raw_input("Enter the name of new db you want to create: ")
	dbuser = raw_input("Enter the new db user you want to create: ")
	dbpwd = raw_input("Enter the new db user password: ")
	dbserver = 'localhost' if dbserver == '' else dbserver
	dbrootuser = 'root' if dbrootuser == '' else dbrootuser
	
	print dbserver
	print dbrootuser
	print dbrootpwd
	
	print '==============================='
	
	db = MySQLdb.connect(dbserver,dbrootuser,dbrootpwd);
	cursor = db.cursor()
	#create the new database
	cursor.execute('create database ' + dbname + ';')
	#create the new user
	cursor.execute("create user '" + dbuser + "'@'" + dbserver + "' identified by '" + dbpwd + "';")
	#grant ALL permissions on the new database to the new user
	cursor.execute("grant all on " + dbname + ".* to '" + dbuser + "'@'" + dbserver + "';")
	#select the new database
	cursor.execute('use ' + dbname + ';')
	#create the contact us table - is this really required - no it is not
	cursor.execute('CREATE TABLE IF NOT EXISTS `contactus` (`id` smallint unsigned NOT NULL AUTO_INCREMENT,`name` varchar(256) NOT NULL,`address` varchar(1000) NULL,`email` varchar(256) NULL,`phonenumber` varchar(50) NOT NULL,`message` text NULL,`createdate` timestamp default now() on update now(),PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;')
	
	db.close()
	print bcolors.OKGREEN + 'ALL DONE!!!' + bcolors.ENDC
except :
	print "Unexpected error:", sys.exc_info()
