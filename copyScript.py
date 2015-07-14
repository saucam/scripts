__author__ = 'yash.datta'

import os, sys, time, subprocess
import syslog
import datetime

from optparse import OptionParser

syslog.openlog(sys.argv[0], syslog.LOG_PID, syslog.LOG_USER)

now = datetime.datetime.now()
date = now.strftime("%Y%m%d%H%M%S")
schema = '/tmp/schema_temp.sql'
cpimportOpt = '-s \'\\t\''
tableDoesNotExistEC = 'IDB-2006'


def execCommand(command, ignoreError = "###", ignoreAll = False, display = True, ex = True):
    if(display):
        print "Executing Command: " + command
    if(ex):
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, close_fds=True)
        output, err = process.communicate()
        errcode = process.returncode
        if errcode:
            if not ignoreAll:
                if (err.find(ignoreError) == -1):
                    raise Exception("COMMAND \"" + str(command) + "\" ERRORED WITH : " + str(err))
        return output

if __name__=="__main__":
    parser = OptionParser(usage="usage: %prog [options] ",
                          version="%prog 1.0")

    parser.add_option("-d", "--dbname",
                      action="store",
                      dest="dbName",
                      help="database name")
    parser.add_option("-p", "--prefix",
                      action="store",
                      dest="pathPrefix",
                      help="path prefix from where data is to be copied")
    parser.add_option("-t", "--tablesList",
                      action="store",
                      dest="tables",
                      help="list of tables to be copied in form : 'T1*T2*T3*T4*T5'.")
    parser.add_option("-s", "--starttime",
                      action="store",
                      dest="startTime",
                      help="start time",
                      type="int")
    parser.add_option("-e", "--endtime",
                      action="store",
                      dest="endTime",
                      help="end time (input 0 for using starttime only)",
                      type="int")
    parser.add_option("-i", "--interval",
                      action="store",
                      dest="interval",
                      help="bin interval for copying data",
                      type="int",
                      default='900')
    parser.add_option("-o", "--outputPath",
                      action="store",
                      dest="outPath",
                      help="output path where data will be copied",
                      default='/data/')

    try:
        (options, args) = parser.parse_args()

        dimensionSetList=[]
        if( options.dbName == None):
                raise Exception("DataBase Name is not provided")
        if( options.startTime == None):
                raise Exception("startTime is not provided")
        if( options.endTime == None):
                raise Exception("endTime is not provided")
        if( options.pathPrefix == None):
                raise Exception("prefix is not provided")
        if( options.endTime > 0 and (options.endTime < options.startTime)):
                raise Exception("endTime less than startTime, provide a valid time range")

        #Convert binClass and DatabaseName to lowercase
        
        startTime = options.startTime
        endTime = options.endTime
        
        #Get the list of tables
        cmd = 'hdfs dfs -ls ' + str(options.pathPrefix) + '/' + options.dbName + " | grep -v \'Found\' | awk \'{print $NF}\'"
        result = execCommand(cmd, ex=True)
        print result

        allTables = []
        if(result!=""):
            result = result.splitlines()
            result = result[1:]
            for line in result:
                print line
                allTables.append(line)
        
        #if(options.tables!=""):
        #    tables = options.tables.split("*")
            
        #TODO: Search for tables in allTables and create the list of tables to be copied   
        
        print options.dbName
        for table in allTables[1:]:
            tableName = table.split(str(options.dbName))[1].split("/")[1]
            finalPath = options.outPath + "/" + options.dbName + "/" + str(tableName)
            cmd = "mkdir -p " + finalPath
            start = startTime
            execCommand(cmd)
            while start<=endTime:
                try:
                    cmd = "hdfs dfs -copyToLocal " + table + "/exporttimestamp=" + str(start) + " " + finalPath
                    execCommand(cmd)
                except Exception, e:
                    print "Error in copying for table " + table + " and exporttimestamp=" + str(start) + " error:" + str(e)
                finally:    
                    start+=options.interval

        '''if( options.dS == None):
                syslog.syslog(syslog.LOG_NOTICE, "dimensionSet list is not provided so all Dimension Tables will be purged.")
                cmd = '%s %s -e "show tables like \'d_%%%s%%\';"' %(idbmysql, options.dbName, options.binClass)
                result = execCommand(cmd)
                if (result==""):
                    raise Exception("No Dimension Tables in database: " + options.dbName)
                result = result.splitlines()
                result = result[1:]
                syslog.syslog(syslog.LOG_NOTICE, str(result))
                for line in result:
                    line = line.replace('d_','',1)
                    line = line.replace(options.binClass + "_",'',1)
                    dimensionSetList.append(line);
        else:
                options.dS = options.dS.lower()
                dlist=options.dS.split('*')
                for d in dlist:
                        dimensionSetList.append(d)'''

        sys.exit()
    except Exception, e:
        print "Error in copying, exiting: " + str(e)
        sys.exit(-1)
