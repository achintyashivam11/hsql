from parser import *

fd=open(hdfs_path+"/"+"dblist.db",'a')
fd.close()

while 1:
    try:
        s = input('hsql > ')
    except EOFError:
        break
    if not s:
        continue
    yacc.parse(s)