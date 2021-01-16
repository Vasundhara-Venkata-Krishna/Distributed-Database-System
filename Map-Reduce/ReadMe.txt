Assignment 3 (Mini Project)

Map-Reduce program using Java and Hadoop Framework:
Input:HDFS location of the input file.
Output: HDFS location of to store the output file.

APPROACH
Mapper:
The input is read line by line from the input file. 
It has two tables, tabmleName1 and tableName2.The tables are joined via foreign key, Table1JoinColumn and Table2JoinColumn. 
In the map function, each file in the input file has to be converted into key-value pairs. The Table1JoinColumn is key and the row is value.
This key-value pair is sent to reducer.

Reducer:
The key-value pairs are sorted with respect to keys. In reducer, the lines from the input file that have same Table1JoinColumn and 
Table2JoinColumn values are assigned to a single Key. The value are then grouped together for each key and values from two different tables are appended.

Driver:
In Main class, conf is created using Configuration. Then Job variable is created using conf variable. 
The names of mapperclass and reducerclass have also been set. The output datatypes of Keys and Values also mentioned in the Driver class. 
The job is executed and is set to wait till the execution completes. The output is checked in the Output path.

IMPLEMENTATION:
Install Java and Hadoop and an IDE. Add hadoop jar files(hadoop-mapreduce-client-core-2.7.3.jar, hadoop-common-2.7.3.jar, commons-cli-1.2.jar)
as external libraries once a java project is created. Write the driver code by creating a class. Once the program runs without error, export a jar file.
Give the path for input and output files in the terminal and run the program.