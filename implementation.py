import os
import sys
import shutil
hdfs_path="."
def remove(path):
    """ param <path> could either be relative or absolute. """
    if os.path.isfile(path):
        os.remove(path)  # remove the file
    elif os.path.isdir(path):
        shutil.rmtree(path)  # remove dir and all contains
    else:
        print("COULD NOT DELETE !")

def mkdir(path):
    try:
        os.mkdir(path)
    except:
        print("COULD NOT CREATE !")

class exp:
    def __init__(self,type,*argv):
        self.type = type
        self.left=argv[0]
        self.right=argv[1]
        self.value=argv[2]
        def expParserHelper(root):
            if root!=None:
                if type(root)==str:
                    yield root
                else:
                    yield from expParserHelper(root.left)
                    if type(root.value) ==exp:
                        yield from expParserHelper(root.value)
                    else:
                        yield root.value
                    yield from expParserHelper(root.right)
        self.exp_iter=expParserHelper
    def __str__(self):
        s=""
        for i in self.exp_iter(self):
            s+=i
        return s

class column:
    def __init__(self,name,agg,alias):
        self.name=name
        self.agg=agg
        self.alias=alias

def db_info(db):
    """returns a dict of structure {'table_name':{'file':'csv_file_path','cols':[all the columns],'dtype':[corresponding data types for the cols]}}"""
    global hdfs_path
    f=open(hdfs_path+"/"+db+".schema")
    flag=0
    info={}
    table=""
    for i in f.readlines():
        flag=1
        temp=i.strip().split(":")
        if temp[0]=='table':
            table=temp[1]
            info[temp[1]]={}
            info[temp[1]]["cols"]=[]
            info[temp[1]]["dtype"]=[]
        elif len(temp)==1:
            if ".csv" in temp[0]:
                info[table]['file']=temp[0]
        elif temp[0]=='column':
            continue
        else:
            info[table]["cols"].append(temp[0])
            info[table]["dtype"].append(temp[1])
    f.close()
    return info
        
def load(original_file_path,hdfs_file_path):
    # original_file_path : path (including name) for the loaded csv file
    # hdfs_file_path : hdfs path to store the csv file
    pass
def drop(what,path):
    # what : "table" or "database"
    # path : path to table or database
    pass

def select(columns,db,table,expression):
    #columns : list of objects of type column (defined above)
    #db : name of database 
    #table : name of table 
    #expression: where expression for filtering (it is a string)
    pass

def MAX():
    # max aggregation. called by select. calling parameters have to be decided
    pass

def COUNT():
    # count aggregation. called by select. calling parameters have to be decided
    pass

def SUM():
    # sum aggregation. called by select. calling parameters have to be decided
    pass