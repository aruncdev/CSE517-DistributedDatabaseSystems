CSE 512 Assignment 3

To run the assignmnet please use command as below and give 
hadoop jar equijoin.jar equijoin /input.txt /output

Firstly, in map phase I am reading the input file from HDFS and adding them as key-value pairs. Each line of the input file represents a key-value pair in a relation.
The join column will be the key.

Reduce phase, After receiving the inputs in the form of key-value pairs, the first step is to seperated the given values into 2 seperate lists based on the relation name.
If the key matches in both the relations then those particular pair of tuples are considered as a result. This will be repeated till all the tuples are exhausted from table1.