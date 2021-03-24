import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    
    createTableQuery = "CREATE TABLE " +  ratingstablename + """ (
                            userid int,
                            temp1 CHAR,
                            movieid int,
                            temp2 CHAR,
                            rating float,
                            temp3 CHAR,
                            timestamp BIGINT);"""
    
    dropColumnQuery = "ALTER TABLE " + ratingstablename + """
                            DROP COLUMN temp1,
                            DROP COLUMN temp2,
                            DROP COLUMN temp3,
                            DROP COLUMN timestamp;"""
    
    connection = openconnection.cursor()
    connection.execute(createTableQuery)
    openconnection.commit()
    connection.copy_from(open(ratingsfilepath,'r'),ratingstablename,sep=':')
    connection.execute(dropColumnQuery)
    openconnection.commit()
    
    connection.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):    
    
    partitionSize = 5.0 / numberofpartitions
    start = 0.0
    end = 0.0
    
    connection = openconnection.cursor()
    
    for partitionIndex in range(0, numberofpartitions):
        end = start + partitionSize
        partitionTableName =  "range_ratings_part" + str(partitionIndex)
        
        createTableQuery = "CREATE TABLE " +  partitionTableName + """ (
                            userid int,
                            movieid int,
                            rating float);"""
        
        if partitionIndex == 0:
            insertQuery = "INSERT INTO " + partitionTableName +"(userid, movieid, rating)" + " SELECT userid, movieid,rating from " + ratingstablename + """
                                                            WHERE rating >= """ + str(start) + " AND rating <= " + str(end)
        else:
            insertQuery = "INSERT INTO " + partitionTableName +"(userid, movieid, rating)" + " SELECT userid, movieid,rating from " + ratingstablename + """
                                                            WHERE rating > """ + str(start) + " AND " + " rating <= " + str(end)
                                                            
        start = end
        
        connection.execute(createTableQuery)
        openconnection.commit()
        connection.execute(insertQuery)
        openconnection.commit()
        
    connection.close()
        


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    
    connection = openconnection.cursor()
    
    for partitionIndex in range(0, numberofpartitions):
        partitionTableName =  "round_robin_ratings_part" + str(partitionIndex)
        
        createTableQuery = "CREATE TABLE " +  partitionTableName + """ (
                            userid int,
                            movieid int,
                            rating float);"""
        
        insertQuery = "INSERT INTO " + partitionTableName + "(userid, movieid, rating) SELECT userid, movieid, rating FROM ( SELECT userid, movieid, rating, ROW_NUMBER () OVER () AS rownbr FROM " +  ratingstablename + """) as temp_table 
                    WHERE (rownbr - 1) % """ + str(numberofpartitions) + " = " + str(partitionIndex) + ";"
                    
        connection.execute(createTableQuery)
        openconnection.commit()
        connection.execute(insertQuery)
        openconnection.commit()
        
    connection.close()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    
    connection = openconnection.cursor()
    
    insertQuery = "INSERT INTO " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
    connection.execute(insertQuery)
    openconnection.commit()
    
    rowCountQuery = "SELECT COUNT(*) from " + ratingstablename + ";"
    connection.execute(rowCountQuery)
    rowCount = (connection.fetchall())[0][0]
    
    numberOfPartitionsQuery = """SELECT COUNT(*) from information_schema.tables WHERE table_name LIKE 'round_robin_ratings_part%';"""
    connection.execute(numberOfPartitionsQuery)
    numberOfPartitions = (connection.fetchall())[0][0]
    
    partitionTableName = "round_robin_ratings_part" + str((rowCount - 1) % numberOfPartitions)
    
    insertQuery = "INSERT INTO " + partitionTableName + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
    
    connection.execute(insertQuery);
    openconnection.commit()
    
    connection.close()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    
    connection = openconnection.cursor()
    
    insertQuery = "INSERT INTO " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
    connection.execute(insertQuery)
    openconnection.commit()
    
    numberOfPartitionsQuery = """SELECT COUNT(*) from information_schema.tables WHERE table_name LIKE 'range_ratings_part%';"""
    connection.execute(numberOfPartitionsQuery)
    numberOfPartitions = (connection.fetchall())[0][0]
    
    eachPartitionRange = 5 / numberOfPartitions
    insertPart = int(rating / eachPartitionRange)
    
    if rating % eachPartitionRange == 0 and insertPart != 0:
        insertPart -= 1
        
    partitionTableName = "range_ratings_part" + str(insertPart)
    
    insertQuery = "INSERT INTO " + partitionTableName + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
    
    connection.execute(insertQuery)
    openconnection.commit()
    
    connection.close()
    

def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):    
    connection = openconnection.cursor()
    
    numberOfPartitionsQuery = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'round_robin_ratings_part%';"
    connection.execute(numberOfPartitionsQuery)
    numberOfPartitions = (connection.fetchall())[0][0]
    
    for i in range(0, numberOfPartitions):
        partitionTableName = "round_robin_ratings_part" + str(i)
        
        selectQuery = "SELECT * FROM " + partitionTableName + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue)
        connection.execute(selectQuery)
        with open(outputPath, 'a') as file:
                    for record in connection.fetchall():
                        file.write(partitionTableName + "," + str(record[0]) + "," + str(record[1]) + "," + str(record[2]) + "\n")
                        
                        
    numberOfPartitionsQuery = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'range_ratings_part%';"
    connection.execute(numberOfPartitionsQuery)
    numberOfPartitions = (connection.fetchall())[0][0]
    
    for i in range(0, numberOfPartitions):
        partitionTableName = "range_ratings_part" + str(i)
        
        selectQuery = "SELECT * FROM " + partitionTableName + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue)
        connection.execute(selectQuery)
        with open(outputPath, 'a') as file:
                    for record in connection.fetchall():
                        file.write(partitionTableName + "," + str(record[0]) + "," + str(record[1]) + "," + str(record[2]) + "\n")
                        
    
    connection.close()


def pointQuery(ratingValue, openconnection, outputPath):
    
    connection = openconnection.cursor()
    
    numberOfPartitionsQuery = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'round_robin_ratings_part%';"
    connection.execute(numberOfPartitionsQuery)
    numberOfPartitions = (connection.fetchall())[0][0]
    
    for i in range(0, numberOfPartitions):
        partitionTableName = "round_robin_ratings_part" + str(i)
        
        selectQuery = "SELECT * FROM " + partitionTableName + " WHERE rating = " + str(ratingValue)
        connection.execute(selectQuery)
        with open(outputPath, 'a') as file:
                    for record in connection.fetchall():
                        file.write(partitionTableName + "," + str(record[0]) + "," + str(record[1]) + "," + str(record[2]) + "\n")
                        
    
    
    #range - partions part
    numberOfPartitionsQuery = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'range_ratings_part%';"
    connection.execute(numberOfPartitionsQuery)
    numberOfPartitions = (connection.fetchall())[0][0]
    
    partitionSize = 5 / numberOfPartitions
    start = 0
    end = 0
    partitionIndex = 0
    
    for partition in range(0, numberOfPartitions):
        end = start + partitionSize
        if ratingValue <= end and ratingValue > start :
            partitionIndex = str(partition)
            break
        start = end
    
    partitionTableName = "range_ratings_part" + partitionIndex 
    selectQuery = "SELECT * FROM " + partitionTableName + " WHERE rating = " + str(ratingValue)
    connection.execute(selectQuery)
    with open(outputPath, 'a') as file:
                    for record in connection.fetchall():
                        file.write(partitionTableName + "," + str(record[0]) + "," + str(record[1]) + "," + str(record[2]) + "\n")
        
    connection.close()


def createDB(dbname='dds_assignment1'):
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
    con.close()

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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
