#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    con = openconnection
    cursor = con.cursor()
    cursor.execute('select max(' + SortingColumnName + ') from ' + InputTable)
    max_value = cursor.fetchone()[0]
    cursor.execute('select min(' + SortingColumnName + ') from ' + InputTable)
    min_value = cursor.fetchone()[0]
    noofThreads = 5
    range_tables = float(max_value - min_value) / noofThreads
    start_value = min_value
    end_value = start_value + range_tables
    parallelthreads = [0, 0, 0, 0, 0]
    for i in range(0, noofThreads):
        parallelthreads[i] = threading.Thread(target=sortIndividualTables, args=(InputTable, SortingColumnName, i, start_value, end_value, openconnection))
        start_value = start_value + range_tables
        end_value = end_value + range_tables
        parallelthreads[i].start()
    cursor.execute("Drop table if exists " + OutputTable)
    cursor.execute("Create table " + OutputTable + "( like " + InputTable + " including all)")
    for i in range(0, noofThreads):
        # print("Thread" + str(i))
        parallelthreads[i].join()
        table_name = "temp_table_sort_" + str(i)
        cursor.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + table_name + ";")
        #cursor.execute("insert into " + OutputTable + " select * from range_part" + str(i))
    # Below 3 lines commented are just for verification
    cursor.execute(
        "copy " + OutputTable + " to '/Users/vvvenka1/Documents/DPS/Assignments/ParallelSortandJoin/sort_output.txt';")
    cursor.execute(
        "SELECT * from " + "(SELECT * FROM " + InputTable + ") as st where st.movieid not in (select movieid from " + OutputTable + ");")
    #print(cursor.fetchall())
    cursor.close()
    con.commit()


def sortIndividualTables(InputTable, SortingColumnName, i, start_value, end_value, openconnection):
    cursor = openconnection.cursor()
    table_name = "temp_table_sort_" + str(i)
    cursor.execute('Drop table if exists ' + table_name)
    cursor.execute("CREATE TABLE " + table_name + " ( LIKE " + InputTable + " INCLUDING ALL);")
    if i == 0:
        cursor.execute(
            "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " >= " + str(
                start_value) + " AND " + SortingColumnName + " <= " + str(
                end_value) + " ORDER BY " + SortingColumnName + " ASC;")
    else:
        cursor.execute(
            "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " > " + str(
                start_value) + " AND " + SortingColumnName + " <= " + str(
                end_value) + " ORDER BY " + SortingColumnName + " ASC;")

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    #pass # Remove this once you are done with implementation
    conn = openconnection
    cursor = conn.cursor()
    cursor.execute("select max(" + Table1JoinColumn + ") from " + InputTable1)
    max_table1 = cursor.fetchone()[0]
    cursor.execute("select min(" + Table1JoinColumn + ") from " + InputTable1)
    min_table1 = cursor.fetchone()[0]
    cursor.execute("select max(" + Table2JoinColumn + ") from " + InputTable2)
    max_table2 = cursor.fetchone()[0]
    cursor.execute("select min(" + Table2JoinColumn + ") from " + InputTable2)
    min_table2 = cursor.fetchone()[0]
    min_t = min(min_table1, min_table2)
    max_t = max(max_table1, max_table2)
    noofThreads = 5
    range_table = float(max_t - min_t) / noofThreads
    parallel_threads = [0, 0, 0, 0, 0]
    start_value = min_t
    end_value = start_value + range_table
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + InputTable1 + "';")
    temp_table_1 = cursor.fetchall()
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + InputTable2 + "';")
    temp_table_2 = cursor.fetchall()
    for i in range(0, noofThreads):
        parallel_threads[i] = threading.Thread(target=joinTables, args=(InputTable1, InputTable2, Table1JoinColumn,
                                                                        Table2JoinColumn, temp_table_2, i,
                                                                        start_value, end_value, openconnection))
        start_value = start_value + range_table
        end_value = end_value + range_table
        parallel_threads[i].start()
    cursor.execute("DROP table if exists " + OutputTable)
    cursor.execute("CREATE TABLE " + OutputTable + " (like " + InputTable1 + " including ALL)")
    query_str = "ALTER TABLE " + OutputTable + " "
    for i in range(len(temp_table_2)):
        if i == len(temp_table_2) - 1:
            query_str += "ADD COLUMN " + temp_table_2[i][0] + " " + temp_table_2[i][1] + ";"
        else:
            query_str += "ADD COLUMN " + temp_table_2[i][0] + " " + temp_table_2[i][1] + ","
    cursor.execute(query_str)
    for i in range(0, noofThreads):
        parallel_threads[i].join()
        #cursor.execute("insert into " + OutputTable + " select * from output_table" + str(i))
        tempOutputTable = "temp_outputTable_join_" + str(i)
        cursor.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + tempOutputTable + ";")

    # cursor.execute("copy " + OutputTable + " to '/Users/vvvenka1/Documents/DPS/Assignments/ParallelSortandJoin/join_output.txt';")
    # cursor.execute(
    #    "SELECT * from " + "(SELECT * FROM " + InputTable1 + " INNER JOIN " + InputTable2 + " ON " + InputTable1 + "." + Table1JoinColumn + " = " + InputTable2 + "." + Table2JoinColumn + ") as jt where jt.movieid not in (select movieid from " + OutputTable + ");")
    # print(cursor.fetchall())
    cursor.close()
    conn.commit()


def joinTables(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, temp_table_2,
               index, start_value, end_value, openconnection):
    tempTable1 = "temp_table_1_" + str(index)
    tempTable2 = "temp_table_2_" + str(index)
    tempOutputTable = "temp_outputTable_join_" + str(index)
    conn = openconnection
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + tempTable1 + ";")
    cursor.execute("CREATE TABLE " + tempTable1 + " ( LIKE " + InputTable1 + " INCLUDING ALL);")
    cursor.execute("DROP TABLE IF EXISTS " + tempTable2 + ";")
    cursor.execute("CREATE TABLE " + tempTable2 + " ( LIKE " + InputTable2 + " INCLUDING ALL);")
    cursor.execute("DROP TABLE IF EXISTS " + tempOutputTable + ";")
    cursor.execute("CREATE TABLE " + tempOutputTable + " ( LIKE " + InputTable1 + " INCLUDING ALL);")
    query_str = "ALTER TABLE " + tempOutputTable + " "
    for k in range(len(temp_table_2)):
        if k == len(temp_table_2) - 1:
            query_str = query_str + "ADD COLUMN " + temp_table_2[k][0] + " " + temp_table_2[k][1] + ";"
        else:
            query_str = query_str + "ADD COLUMN " + temp_table_2[k][0] + " " + temp_table_2[k][1] + ","
    cursor.execute(query_str)
    if index == 0:
        cursor.execute(
            "INSERT INTO " + tempTable1 + " SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " >= " + str(
                start_value) + " AND " + Table1JoinColumn + " <= " + str(end_value) + ";")
        cursor.execute(
            "INSERT INTO " + tempTable2 + " SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(
                start_value) + " AND " + Table2JoinColumn + " <= " + str(end_value) + ";")
    else:
        cursor.execute(
            "INSERT INTO " + tempTable1 + " SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(
                start_value) + " AND " + Table1JoinColumn + " <= " + str(end_value) + ";")
        cursor.execute(
            "INSERT INTO " + tempTable2 + " SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " > " + str(
                start_value) + " AND " + Table2JoinColumn + " <= " + str(end_value) + ";")

    cursor.execute("INSERT INTO " + tempOutputTable + " SELECT * FROM " + tempTable1 + " INNER JOIN " + tempTable2 + " ON " + tempTable1\
                + "." + Table1JoinColumn + " = " + tempTable2 + "." + Table2JoinColumn + ";")

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


