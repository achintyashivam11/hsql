import ply.yacc as yacc
from lexer import *
from implementation import *
hdfs_path="." # currently given path as current dir. later have to set the path to a folder in hdfs

current_db=None

def p_all(p):
    """all : create
           | use
           | load
           | drop
           | select
           | cdb
           | schema
           | dblist
           | exit"""

def p_create(p): 
    '''create : CREATE DATABASE ID SEMI'''
    global current_db
    global hdfs_path
    fd=open(hdfs_path+"/"+"dblist.db",'a')
    fd.write(p[3]+"\n")
    fd.close()
    current_db=p[3]
    mkdir(hdfs_path+"/"+p[3])
    f=open(hdfs_path+"/"+p[3]+".schema","w")  # Schema must be stored in HDFS in a separate folder. This folder will contain all the csv files, schema of databases, tables,logs,etc
    f.close()
    print("CREATED DATABASE",p[3],"!")

def p_dblist(p):
    """dblist : LIST DATABASE SEMI"""
    global hdfs_path
    fd=open(hdfs_path+"/"+"dblist.db")
    for i in fd.readlines():
        t=i.strip()
        if t!='':
            print(i)


def p_cdb(p):
    """cdb : CURRENT DATABASE SEMI"""
    global current_db
    if current_db==None:
        print("NO DATABASE SELECTED !")
    else:
        print("CURRENT DATABASE SELECTED IS",current_db)

def p_schema(p):
    """schema : SCHEMA DATABASE ID SEMI
              | SCHEMA CURRENT DATABASE SEMI
              | SCHEMA TABLE ID SEMI"""
    global hdfs_path
    if p[2]=='database':
        try:
            f=open(hdfs_path+"/"+p[3]+".schema")
            flag=0
            for i in f.readlines():
                flag=1
                temp=i.strip().split(":")
                if temp[0]=='table':
                    print(temp[0],temp[1],":")
                elif len(temp)==1:
                    continue
                else:
                    print("\t",temp[0].split("/")[-1],":",temp[1])
            if flag==0:
                print("NO TABLE IN DATABASE !")
            f.close()
        except:
            print("NO DATABASE NAMED",p[3])
    elif p[2]=='current':
        if current_db==None:
            print("NO DATABASE SELECTED !")
        else:
            flag=0
            f=open(hdfs_path+"/"+current_db+".schema")
            for i in f.readlines():
                flag=1
                temp=i.strip().split(":")
                if temp[0]=='table':
                    print(temp[0],temp[1],":")
                elif len(temp)==1:
                    continue
                else:
                    print("\t",temp[0].split("/")[-1],":",temp[1])
            if flag==0:
                print("NO TABLE IN DATABASE !")
            f.close()
    else:
        if current_db==None:
            print("NO DATABASE SELECTED !")
        else:
            f=open(hdfs_path+"/"+current_db+".schema")
            lines=f.readlines()
            lines=list(map(lambda x: x.strip(),lines))
            f.close() 
            pos=-1
            l=len(lines)
            for i in range(l):
                temp=lines[i].strip().split(":")
                if temp[0]=='table' and temp[1]==p[3]:
                    pos=i
                    break
            if pos==-1:
                print("NO TABLE NAMED",p[3],"FOUND !")
            else:
                pos1=l
                for i in range(pos+1,l):
                    temp=lines[i].strip().split(":")
                    if temp[0]=='table':
                        pos1=i
                        break
                for i in range(pos,pos1):
                    temp=lines[i].strip().split(":")
                    if temp[0]=='table':
                        print(temp[0],":",temp[1],"\n")
                    elif len(temp)==1:
                        continue
                    else:
                        print("\t",temp[0],":",temp[1])
                  

def p_use(p):
    '''use : USE ID SEMI'''
    global current_db
    global hdfs_path
    fd=open(hdfs_path+"/"+"dblist.db")
    flag=0
    dblist=fd.readlines()
    dblist=list(map(lambda x: x.strip(),dblist))
    fd.close()
    for i in dblist:
        if i==p[2]:
            f=open(hdfs_path+"/"+p[2]+".schema")
            current_db=p[2]
            flag=1
            break
    fd.close()
    if flag==0:
        print("NO DATABASE NAMED",p[2],"FOUND !")
    else:
        print("USING DATABASE ",p[2])

def p_load(p):
    '''load : LOAD ID AS ID LPAREN column_dtypes RPAREN SEMI'''
    global current_db
    global hdfs_path
    try:
        if current_db==None:
            print("NO DATABASE SELECTED !")
        else:
            f=open(hdfs_path+"/"+current_db+".schema")
            for i in f.readlines():
                temp=i.strip().split()
                if temp[0]=="table" and temp[1]==p[4]:
                    print("TABLE NAME ALREADY EXISTS !")
                    return
            f.close()
            f=open(hdfs_path+"/"+current_db+".schema","a")
            f.write("table:"+p[4]+"\n")
            f.write(hdfs_path+"/"+current_db+"/"+p[4]+"/"+p[2])
            f.write("column:dtype"+"\n")
            columns=[]
            #mkdir(hdfs_path+"/"+current_db+"/"+p[4])
            for i in p[6]:
                f.write(i[0]+":"+i[1]+"\n")
                columns.append((i[0],i[1]))
            f.write("\n")
            f.close()
            load(p[2],hdfs_path+"/"+current_db+"/"+p[4])
            print("LOADED TABLE",p[4],"INTO DATABASE ",current_db)
    except:
        print("NO FILE NAMED ",p[2],"FOUND !")


def p_column_dtypes(p):
    '''column_dtypes : column_dtypes COMMA column_dtype
                     | column_dtype'''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        statement_list = []
        if type(p[1]) !=list:
            statement_list.append(p[1])
        else:
            statement_list = p[1]
        statement_list.append(p[3])
        p[0] = statement_list

def p_column_dtype(p):
    '''column_dtype : ID COLON dtype'''
    p[0]=(p[1],p[3])  

def p_dtype(p):
    """dtype : INT
             | FLOAT
             | STR"""
    p[0]=p[1]

def p_drop(p):
    '''drop : DROP DATABASE ID SEMI
            | DROP TABLE ID SEMI'''
    global current_db
    global hdfs_path
    if p[2]=='database':
        fd=open(hdfs_path+"/"+"dblist.db")
        dblist=fd.readlines()
        dblist=list(map(lambda x: x.strip(),dblist))
        fd.close()
        if p[3] in dblist:
            # remove from dblist
            index=dblist.index(p[3])
            dblist.pop(index)
            fd=open(hdfs_path+"/"+"dblist.db",'w')
            for i in dblist:
                fd.write(i+"\n")
            fd.close()
            # delete schema and db folder
            drop("database",hdfs_path+"/"+current_db+"/"+p[3])
            remove(hdfs_path+"/"+p[3]+".schema") 
            print("REMOVED DATABASE ",p[3],"!") 
        else:
            print("NO DATABASE NAMED",p[2],"FOUND !")
    elif p[2]=='table':
        if current_db==None:
            print("NO DATABASE SELECTED !")
        else:
            fd=open(hdfs_path+"/"+current_db+".schema")
            lines=fd.readlines()
            fd.close()
            lines=list(map(lambda x: x.strip(),lines))
            pos=-1
            l=len(lines)
            for i in range(l):
                temp=lines[i].strip().split()
                if temp[0]=='table' and temp[1]==p[3]:
                    pos=i

            if pos==-1:
                print("NO TABLE NAMED",p[3],"FOUND !")
            else:
                pos1=l
                for i in range(pos+1,l):
                    temp=lines[i].strip().split()
                    if temp[0]=='table':
                        pos1=i
                        break
                for i in range(pos,pos1):
                    lines.pop(pos)
                fd=open(hdfs_path+"/"+current_db+".schema","w")
                for i in lines:
                    fd.write(i+"\n")
                fd.close()
                drop("table",hdfs_path+"/"+current_db+"/"+p[3])
                print("DROPPED TABLE",p[3],"FROM DATABASE",current_db,"!")
    else:
        print("COULD NOT DROP DATABASE !")

def p_select(p):
    """select : SELECT columns FROM ID WHERE logical_not_expression SEMI"""
    global current_db
    global hdfs_path
    if current_db==None:
            print("NO DATABASE SELECTED !")
    else:
        select(p[2],current_db,p[4],p[6])


def p_columns(p):
    """columns : columns COMMA column
               | column"""
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        statement_list = []
        if type(p[1]) !=list:
            statement_list.append(p[1])
        else:
            statement_list = p[1]
        statement_list.append(p[3])
        p[0] = statement_list

def p_column(p):
    """column  : MAX LPAREN ID RPAREN
               | COUNT LPAREN ID RPAREN
               | SUM LPAREN ID RPAREN
               | ID
               | MAX LPAREN ID RPAREN AS ID
               | COUNT LPAREN ID RPAREN AS ID
               | SUM LPAREN ID RPAREN AS ID
               | ID AS ID"""
    if len(p)==5:
        p[0]=column(p[3],p[1],None)
    elif len(p)==2:
        p[0]=column(p[1],None,None)
    elif len(p)==7:
        p[0]=column(p[3],p[1],p[6])
    else:
        p[0]=column(p[1],None,p[3])

# logical-not-expression

def p_logical_not_expression_1(t):
    """logical_not_expression : logical_or_expression"""
    t[0]=t[1]

def p_logical_not_expression_2(t):
    """logical_not_expression : NOT logical_not_expression"""
    t[0]=exp('not',None,t[2],t[1])

# logical-or-expression

def p_logical_or_expression_1(t):
    'logical_or_expression : logical_and_expression'
    t[0]=t[1]


def p_logical_or_expression_2(t):
    'logical_or_expression : logical_or_expression OR logical_and_expression'
    t[0]=exp('lop',t[1],t[3],t[2])

# logical-and-expression


def p_logical_and_expression_1(t):
    'logical_and_expression : equality_expression'
    t[0]=t[1]


def p_logical_and_expression_2(t):
    'logical_and_expression : logical_and_expression AND equality_expression'
    t[0]=exp('lap',t[1],t[3],t[2])



# equality-expression:
def p_equality_expression_1(t):
    'equality_expression : relational_expression'
    t[0]=t[1]


def p_equality_expression_2(t):
    """equality_expression : equality_expression EQ relational_expression
                           | equality_expression EQUALS relational_expression"""
    t[0]=exp('rop',t[1],t[3],t[2])


def p_equality_expression_3(t):
    'equality_expression : equality_expression NE relational_expression'
    t[0]=exp('rop',t[1],t[3],t[2])


# relational-expression:
def p_relational_expression_1(t):
    'relational_expression : additive_expression'
    t[0]=t[1]


def p_relational_expression_2(t):
    'relational_expression : relational_expression LT additive_expression'
    t[0]=exp('rop',t[1],t[3],t[2])


def p_relational_expression_3(t):
    'relational_expression : relational_expression GT additive_expression'
    t[0]=exp('rop',t[1],t[3],t[2])


def p_relational_expression_4(t):
    'relational_expression : relational_expression LE additive_expression'
    t[0]=exp('rop',t[1],t[3],t[2])


def p_relational_expression_5(t):
    'relational_expression : relational_expression GE additive_expression'
    t[0]=exp('rop',t[1],t[3],t[2])


# additive-expression

def p_additive_expression_1(t):
    'additive_expression : multiplicative_expression'
    t[0]=t[1]


def p_additive_expression_2(t):
    'additive_expression : additive_expression PLUS multiplicative_expression'
    t[0]=exp('aop',t[1],t[3],t[2])


def p_additive_expression_3(t):
    'additive_expression : additive_expression MINUS multiplicative_expression'
    t[0]=exp('aop',t[1],t[3],t[2])

# multiplicative-expression


def p_multiplicative_expression_1(t):
    'multiplicative_expression : val'
    t[0]=t[1]


def p_multiplicative_expression_2(t):
    'multiplicative_expression : multiplicative_expression TIMES val'
    t[0]=exp('aop',t[1],t[3],t[2])


def p_multiplicative_expression_3(t):
    'multiplicative_expression : multiplicative_expression DIVIDE val'
    t[0]=exp('aop',t[1],t[3],t[2])


def p_multiplicative_expression_4(t):
    'multiplicative_expression : multiplicative_expression MOD val'
    t[0]=exp('aop',t[1],t[3],t[2])


def p_val(p):
    """val : ID
           | ICONST
           | FCONST
           | SCONST"""
    p[0]=p[1]

def p_exit(p):
    """exit : EXIT LPAREN RPAREN
            | QUIT LPAREN RPAREN
            | EXIT LPAREN RPAREN SEMI
            | QUIT LPAREN RPAREN SEMI"""
    exit()

def p_error(p):
    if p:
        print("Syntax error at '%s' !" % p.value,)
    else:
        print("Syntax error at EOL !")

yacc.yacc()