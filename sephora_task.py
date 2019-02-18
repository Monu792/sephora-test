import os
import glob
import pyodbc
import time
from threading import Thread, Lock

# Connect to your db server
con = pyodbc.connect(Trusted_Connection='yes',
                     driver='{SQL Server}', server=r'SINPC0K6GHT\SQLDB', database='raw')
cursor = con.cursor()

# The below function takes a folder path as an argument and reads all .sql files
# in the given path and identifies the list of tables used in .sql file
# Stores the .sql filename as 'key' and the tables used in .sql as 'value' (tables_dict)
# For Eg.tmp.variant_images is the key and ['raw.variants', 'raw.pictures'] is a value in a list
# And also this function returns another dictionar called SQL_List
# SQL_List contains .sql filename as 'key' and the entire sql statement as 'value'


def find_dependency(folder_path):
    # Find dependancies
    tables_dict = dict()
    SQL_List = dict()
    for filename in glob.glob(os.path.join(folder_path, '*.sql')):
        with open(filename, 'r') as f:
            text = f.read()
            SQL = " ".join(text.splitlines())

            # print (SQL)
            database = folder_path.split('/')[-1]
            # print(database)
            fname = filename.split('\\')[1]
            name = fname.split('.')[0]
            # print (fname)
            db_table = '.'.join([database, name])
            SQL_List[db_table] = SQL
            list_of_words = SQL.split()
            main_table_search = 'FROM'
            main_table = list_of_words[list_of_words.index(
                main_table_search) + 1]
            parent = [main_table.strip("`")]
            if('JOIN' in list_of_words):
                myIter = iter(list_of_words)
                child_table = []
                for i in range(0, len(list_of_words)):
                    next_item = next(myIter)
                    if (next_item == 'JOIN'):
                        child_table.append(list_of_words[i+1])
                child = []
                for elem in child_table:
                    child.extend(elem.strip("`").split(' '))
                dep_tb = []
                dep_tb = parent+child
                tables_dict[db_table] = dep_tb

            else:
                tables_dict[db_table] = parent

    return tables_dict, SQL_List


# Give the folder path of 'tmp' folder
tables_dict, SQL_List = (find_dependency(
    'C:/Users/t_duramo/Downloads/sephora-test-master/sephora-test-master/tmp'))
# Give the folder path of 'final' folder
final_tables, SQL_lists = (find_dependency(
    'C:/Users/t_duramo/Downloads/sephora-test-master/sephora-test-master/final'))

# Merge two dictionaries returned from 2 function calls above
tables_dict.update(final_tables)
SQL_List.update(SQL_lists)

#print (tables_dict)
#print (SQL_List)

# The below function shows all the combination of key and value (in lists) as edges


def generate_edges(tables_dict):
    edges = []
    for node in tables_dict:
        for neighbour in tables_dict[node]:
            edges.append((node, neighbour))

    return edges


#print ('Edges are')
#print(generate_edges(tables_dict))

# The below function shows all the keys as vertices from the dictionary


def vertices(tables_dict):
    return list(tables_dict.keys())


#print('Vertices are')
#print(vertices(tables_dict))


# This function shows a simple representation of keys and it's value
# this doesn't draw a graph or a tree
def print_dependency(dictionary):
    for key, value in dictionary.items():
        if len(value) > 2:
            print ('\t\t\t'+key)
            print('\n')
            print (value)
            print('\n')
            print('\n')
        else:
            print ('\t'+key)
            print('\n')
            print (value)
            print('\n')
            print('\n')


#print_dependency(tables_dict)

# The below function takes the dictionary which contains the SQL statements as
# argument and identifies if the 'tmp' is not present in the SQL query which means
# those SQL's are not dependant on any other 'tmp' tables
# (assuming the raw tables are already present) and the statements which is dependant
# on another 'tmp' table waits for 5 mins until those are created
def executeScripts(dictionary):
    print("Creating Tables")
    for key, value in dictionary.items():
        if ('tmp' in key and 'tmp' not in value):
            SQL = value.replace('`', '')
            print("Executing query")
            print(key)
            #cursor.execute(SQL)
    
    
    print("Waiting for the dependant tables to be created")
    time.sleep(60)  # Wait for 1 mins untill the above
    for key, value in dictionary.items():
        if ('tmp' in key and 'tmp' in value):
            SQL = value.replace('`', '')
            print("Executing query")
            print(key)
            #cursor.execute(SQL)

    
    print("Waiting for the dependant tables to be created")
    time.sleep(70)  # Wait for 2 mins untill the above
    for key, value in dictionary.items():
        if ('final' in key):
            SQL = value.replace('`', '')
            print("Executing query")
            print(key)
            #cursor.execute(SQL)

executeScripts(SQL_List)