import os
import shutil
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
def load(original_file_path,column_file_paths):
    # original_file_path : path (including name) for the loaded csv file
    # column_file_paths : paths (including the column names) for storing the column files.
    pass


def select(columns,db,table,expression):
    #columns : list of objects of type column (defined above)
    #db : name of database (will need it for knowing the folder to search for table folders)
    #table : name of table (will be needing it for knowing the table folder in db folder to search for column files)
    #expression: where expression for filtering
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