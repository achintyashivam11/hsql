# hsql
<h1>An SQL engine on top of Hadoop</h1>

<h2>There are 4 files</h2>

1. lexer.py : this does the job of lexing
2. parser.py : this does the parsing of the SQL queries
3. main.py : this is the which runs the interpreter (like python !) and you can run SQL Queries
4. implemenation.py : this file contains the implemenations of SQL queries and some functions **(Must be completed)**

<h2>The SQL Queries Currently Supported :</h2>

1. select
2. use
3. drop
4. load
5. create database
6. schema (not there in standard SQL. Added to view schema of databases and tables)
7. current database (again not there in standard SQL. Added to know the currently selected database)
8. exit() or quit() (to quit the interpreter)

<h2>What are requirements ?</h2>

hadoop
python3
ply

<h2>How to install ply ?</h2>

`pip3 install ply` 


<h2>How to run the interpreter ?</h2>

`python3 main.py`


<h2>What's Currently Working ?</h2>

1. use
2. create database
3. load (partially)
4. drop
5. schema
6. current database


<h2>What's must be done ?</h2>

1. Make it work on hadoop (create and delete files/folders in hadoop. currently made to work on file system and not hadoop. May have to change remove() in implementation.py. can do in end I guess)
2. Implement load completely (currently only writing meta data (schema info) into database_name.schema. Must split the csv file into columns and store each column as separate file. All addresses are passed into load function. Must compelete it )
3. Implement select
4. Implement aggregate functions MAX, COUNT, SUM


**Note : May have to write mapper/reducer in separate files and call them via system call in the wrapper functions select, load, MAX,COUNT and SUM via hadoop streaming API**

<h2>The Directory Organization</h2>

DATABASE_ROOT/<br/>
&nbsp;&nbsp;&nbsp;&nbsp;database_name.schema (there can be many schema files (unique))<br/>
&nbsp;&nbsp;&nbsp;&nbsp;dblist.db (only one file which contains the list of all the databases)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;database_name/ (there is one directory for each database)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;table_name/ (there is one directory for a table in a database)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;column_name (there can be many column files (unique))<br/>
    

**Have commented as many important lines as possible. If you have any doubts, call me.**
