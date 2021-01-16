import psycopg2
import os
import sys

RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    #pass # Remove this once you are done with implementation
    conn = openconnection
    cur = conn.cursor()
    cur.execute("CREATE TABLE " + ratingstablename + "(userid INTEGER, colon_1 CHAR, movieid INTEGER, colon_2 CHAR, "
                                                     "rating FLOAT, colon_3 CHAR,  Timestamp BIGINT);")
    cur.copy_from(open(ratingsfilepath), ratingstablename, sep=':')
    cur.execute("alter table " + ratingstablename + " DROP COLUMN colon_1, DROP COLUMN colon_2, DROP COLUMN colon_3, DROP COLUMN Timestamp;")
    cur.close()
    conn.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    #pass # Remove this once you are done with implementation
    conn = openconnection
    cur = conn.cursor()
    partition_size = 5.0/numberofpartitions
    upper_range = 0
    i = 0
    while i < numberofpartitions:
        lower_range = upper_range
        upper_range = lower_range + partition_size
        table_name = RANGE_TABLE_PREFIX + str(i)
        #cur.execute("DROP TABLE IF EXISTS " + table_name)
        cur.execute("CREATE TABLE " + table_name + " (userid INTEGER, movieid INTEGER, rating FLOAT);")
        if i == 0:
            #cur.execute("DROP TABLE IF EXISTS " + table_name)
            cur.execute("INSERT INTO " + table_name + " (userid, movieid, rating) SELECT userid, movieid, rating FROM "
                        + ratingstablename + " Where rating >= " + str(lower_range) +
                        "AND rating <= " + str(upper_range) + ";")
        else:
            #cur.execute("DROP TABLE IF EXISTS " + table_name)
            cur.execute("INSERT INTO " + table_name + " (userid, movieid, rating) SELECT userid, movieid, rating FROM "
                        + ratingstablename + " Where rating > " + str(lower_range) +
                        "AND rating <= " + str(upper_range) + ";")
        i = i+1

    cur.close()
    conn.commit()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cur = conn.cursor()
    table_number = 0
    while table_number < numberofpartitions:
        table_name_1 = RROBIN_TABLE_PREFIX + str(table_number)
        cur.execute("CREATE TABLE " + table_name_1 + " (userid INTEGER, movieid INTEGER, rating FLOAT);")
        cur.execute("INSERT INTO " + table_name_1 + " (userid, movieid, rating) SELECT userid, movieid, rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER() AS no FROM " + ratingstablename + ") AS rate WHERE MOD(rate.no-1, 5) = " + str(
                table_number) + ";")
        table_number = table_number + 1
        #if table_number >= numberofpartitions:
        #    table_number = 0
    cur.close()
    conn.commit()

    #pass # Remove this once you are done with implementation
    #conn = openconnection
    #cur = conn.cursor()
    #cur.execute("SELECT COUNT(*) FROM " + ratingstablename)
    #record_count = cur.fetchone()[0]
    #print(record_count)
    #insertion_counter = 0
    #partition_counter = 1
    #while insertion_counter < record_count:
    #    if partition_counter <= numberofpartitions:
    #        table_number = record_count % partition_counter
    #        table_name = RROBIN_TABLE_PREFIX + str(table_number)
    #        cur.execute("INSERT INTO " + table_name + " (userid, movieid, rating) SELECT userid, movieid, rating FROM "
    #                  + ratingstablename + ";")
    #    else:
    #        partition_counter = 1
    #    insertion_counter = insertion_counter + 1
    #    partition_counter = partition_counter + 1

    #cur.close()
    #conn.commit()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    #pass # Remove this once you are done with implementation
    conn = openconnection
    cur = conn.cursor()
    cur.execute("INSERT INTO " + ratingstablename + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.execute("SELECT COUNT(*) FROM " + ratingstablename + ";")
    number_of_records = (cur.fetchall())[0][0]
    number_of_partitions = number_of_records % partitionCount(RROBIN_TABLE_PREFIX, openconnection)
    table_index = (number_of_records-1)%number_of_partitions
    table = RROBIN_TABLE_PREFIX + str(table_index)
    cur.execute("INSERT INTO " + table + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(
        itemid) + "," + str(rating) + ");")
    cur.close()
    conn.commit()


def partitionCount(tablename, openconnection):
    connect = openconnection
    cur = connect.cursor()
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE RELNAME LIKE " + "'" + tablename + "%';")
    count = cur.fetchone()[0]
    cur.close()
    connect.commit()
    return count

def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    #pass # Remove this once you are done with implementation
    conn = openconnection
    cur = conn.cursor()
    number_of_partitions = partitionCount(RANGE_TABLE_PREFIX, openconnection)
    division_range = 5.0/number_of_partitions
    table_index = int(rating/division_range)
    reminder = rating % division_range
    if reminder == 0 and table_index!=0:
        table_index = table_index-1
    table = RANGE_TABLE_PREFIX + str(table_index)
    cur.execute("INSERT INTO " + table + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(
        itemid) + "," + str(rating) + ");")
    cur.close()
    conn.commit()


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #pass #Remove this once you are done with implementation
    conn = openconnection
    cur = conn.cursor()
    cur.execute('select current_database()')
    database_tables = cur.fetchall()[0][0]
    # Query to select RangeRatingsPart tables
    cur.execute(
        'select table_name from information_schema.tables where table_name like \'%ratings_part%\' and table_catalog=\'' + database_tables + '\'')
    partitions = cur.fetchall()
    #print(partitioned_tables + "here i am")
    tuples = []
    #print(ratings_partitions)
    for table in partitions:
        name = table[0]
        cur.execute('select userid, movieid, rating from ' + str(name) + ' where rating >= ' + str(
            ratingMinValue) + ' and rating <= ' + str(ratingMaxValue))
        table_data = cur.fetchall()
        #print(table_data)
        for record in table_data:
            if "range" in name:
                partition_name = "RangeRatingsPart" + name[-1]
            else:
                partition_name = "RoundRobinRatingsPart" + name[-1]
            row = [str(partition_name)]
            row.extend(record)
            tuples.append(row)
    cur.close()
    writeToFile(outputPath, tuples)


def pointQuery(ratingValue, openconnection, outputPath):
    #pass # Remove this once you are done with implementation
    conn = openconnection
    cur = conn.cursor()
    cur.execute('select current_database()')
    database_tables = cur.fetchall()[0][0]
    # Query to select RangeRatingsPart tables
    cur.execute(
        'select table_name from information_schema.tables where table_name like \'%ratings_part%\' and table_catalog=\'' + database_tables + '\'')
    partitions = cur.fetchall()
    tuples = []
    for table in partitions:
        name = table[0]
        cur.execute(
            'select userid, movieid, rating from ' + str(name) + ' where rating = ' + str(ratingValue))
        table_data = cur.fetchall()
        for record in table_data:
            if "range" in name:
                partition_name = "range_ratings_part" + name[-1]
            else:
                partition_name = "round_robin_ratings_part" + name[-1]
            row = [str(partition_name)]
            row.extend(record)
            tuples.append(row)
    cur.close()
    writeToFile(outputPath, tuples)

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