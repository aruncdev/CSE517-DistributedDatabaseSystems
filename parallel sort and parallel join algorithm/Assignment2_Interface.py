#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    threadsList = [0] * 5
    
    maxMinQuery = "SELECT MIN(" + SortingColumnName + "), MAX(" + SortingColumnName + ")  from " + InputTable + ";"
    connection = openconnection.cursor()
    connection.execute(maxMinQuery)
    minVal, maxVal = connection.fetchone()
    
    partitionRange = float(maxVal - minVal) / 5
    
    for i in range(5):
        minimum = minVal + i * partitionRange
        maximum = minimum + partitionRange
        
        threadsList[i] = threading.Thread(target = executions, args=(InputTable, SortingColumnName, minimum, maximum, "partition" + str(i), openconnection, i))
        threadsList[i].start()
    
    dropQuery = "DROP TABLE IF EXISTS " + OutputTable + ";"
    connection.execute(dropQuery)
    
    createQuery = "CREATE TABLE " + OutputTable + " AS SELECT * FROM " + InputTable + " WHERE 0 = 1;"
    connection.execute(createQuery)
    
    for i in range(5):
        threadsList[i].join()
        tableName = "partition" + str(i)
        insertQuery = "INSERT INTO " + OutputTable + " SELECT * FROM " + tableName + ";"
        connection.execute(insertQuery)
    
    openconnection.commit()

    
def executions(InputTable, SortingColumnName, minimum, maximum, partitionTableName, openconnection, index):
    
    connection = openconnection.cursor()
    
    dropQuery = "DROP TABLE IF EXISTS " + partitionTableName + ";"
    createQuery = ""
    
    connection.execute(dropQuery)
                                
    if index == 0:
        createQuery = "CREATE TABLE " + str(partitionTableName) + " AS SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " >= " + str(minimum) + " AND " + SortingColumnName + " <= " + str(maximum) + " ORDER BY " + SortingColumnName + " ASC;"
    else:
        createQuery = "CREATE TABLE " + str(partitionTableName) + " AS SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " > " + str(minimum) + " AND " + SortingColumnName + " <= " + str(maximum) + " ORDER BY " + SortingColumnName + " ASC;"
    
    connection.execute(createQuery)
    
    openconnection.commit()



def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    threadsList = [0] * 5
    
    connection = openconnection.cursor()
    
    maxMinQuery = "SELECT MIN(" + Table1JoinColumn + "), MAX(" + Table1JoinColumn + ")  from " + InputTable1 + ";"
    connection.execute(maxMinQuery)
    minVal1, maxVal1 = connection.fetchone()
    
    maxMinQuery = "SELECT MIN(" + Table2JoinColumn + "), MAX(" + Table2JoinColumn + ")  from " + InputTable2 + ";"
    connection.execute(maxMinQuery)
    minVal2, maxVal2 = connection.fetchone()
    
    minVal = min(minVal1, minVal2)
    maxVal = max(maxVal1, maxVal2)
    
    partitionRange = float(maxVal - minVal) / 5
    
    for i in range(5):
        minimum = minVal + i * partitionRange
        maximum = minimum + partitionRange
        
        threadsList[i] = threading.Thread(target = joinExecutions, args=(InputTable1, InputTable2, "table1Partition" + str(i), "table2Partition" + str(i), Table1JoinColumn, Table2JoinColumn, minimum, maximum, OutputTable, i, openconnection))
        threadsList[i].start()
    
    dropQuery = "DROP TABLE IF EXISTS " + OutputTable + ";"
    connection.execute(dropQuery)
    
    createQuery = "CREATE TABLE " + OutputTable + " AS SELECT "+ InputTable1 + ".* , " + InputTable2 + ".* FROM " + InputTable1 + " , " + InputTable2 + " WHERE 0 = 1;"
    connection.execute(createQuery)
    
    for i in range(5):
        threadsList[i].join()
        tableName1 = "table1Partition" + str(i)
        tableName2 = "table2Partition" + str(i)
        joinQuery = "INSERT INTO " + OutputTable + " SELECT * FROM " + tableName1 + " INNER JOIN " + tableName2 + " ON " + tableName1 + "." + Table1JoinColumn + " = " + tableName2 + "." + Table2JoinColumn + ";"
        
        connection.execute(joinQuery)
    
    openconnection.commit()
    


def joinExecutions(InputTable1, InputTable2, table1Partition, table2Partition, Table1JoinColumn, Table2JoinColumn, minimum, maximum, OutputTable, index, openconnection):
    
    connection = openconnection.cursor()
    
    dropQuery = "DROP TABLE IF EXISTS " + table1Partition + ";"
    connection.execute(dropQuery)
    dropQuery = "DROP TABLE IF EXISTS " + table2Partition + ";"
    connection.execute(dropQuery)
    
    createQuery1 = ""
    createQuery2 = ""
    
    if index == 0:
        createQuery1 = "CREATE TABLE " + str(table1Partition) + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " >= " + str(minimum) + " AND " + Table1JoinColumn + " <= " + str(maximum) + ";"
        createQuery2 = "CREATE TABLE " + str(table2Partition) + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(minimum) + " AND " + Table2JoinColumn + " <= " + str(maximum) + ";"
    else:
        createQuery1 = "CREATE TABLE " + str(table1Partition) + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(minimum) + " AND " + Table1JoinColumn + " <= " + str(maximum) + ";"
        createQuery2 = "CREATE TABLE " + str(table2Partition) + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(minimum) + " AND " + Table2JoinColumn + " <= " + str(maximum) + ";"
    
    connection.execute(createQuery1)
    connection.execute(createQuery2)
    openconnection.commit()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


