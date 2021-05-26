
# coding: utf-8

# In[5]:


import MySQLdb
from sqlalchemy import create_engine,event
import pandas as pd
import time
import datetime as dt
from pandas.tseries.offsets import BDay
#import odo
import calendar

from pytz import timezone
tz = timezone('America/New_York')
                
class fin_data(object):
    
    def __init__(self):

        self.host = "primary.cono8wvajotx.us-east-2.rds.amazonaws.com"
        self.port = 3306
        self.username = "hj22215"
        self.password = "TYrepoh756"
        self.dbname = "primaryDb"
        
        self.con = MySQLdb.connect(self.host, self.username, self.password, self.dbname)
        self.cur = self.con.cursor()
        self.df  = pd.DataFrame()
    
    def refresh(self):
        self.con = MySQLdb.connect(self.host, self.username, self.password, self.dbname)
        self.cur = self.con.cursor()
        
    def new_table(self, table_name, parm_str):
        self.refresh()
        command = "Create Table " + table_name + " (" + parm_str + ")"
        self.cur.execute(command)
        self.con.commit()
        
    def select(self, sql):
        self.refresh()
        df = pd.read_sql_query(sql, self.con)
        return df
        
    def insert(self, sql, values):
        self.refresh()
        sql = sql.replace("?","%s")
        sql = sql.replace("insert or ignore","insert ignore")
        #self.cur.fast_executemany = True
        self.cur.executemany(sql,values)
        self.con.commit()
        
    def insertNoRefresh(self, sql, values):
        sql = sql.replace("?","%s")
        sql = sql.replace("insert or ignore","insert ignore")
        #self.cur.fast_executemany = True
        self.cur.executemany(sql,values)
        self.con.commit()
        
    def createValStr(self, table, numCol, ignore):
    
        valStr = '('
        for i in range(numCol):
            valStr += '?,'
        valStr = valStr[:len(valStr) - 1] + ')'
        
        if ignore:
            ins = "insert ignore "
        else:
            ins = "insert "
            
        return ins + "into " + table + " values " + valStr
    
    def bulk_Insert(self, table, data, ignore = True):
        
        columns = data.columns.tolist()
        valStr = self.createValStr(table, len(columns), ignore)
        
        self.refresh()
        
        insert = []
        inserted = 0
        maxInsNum = 20000
        for x, item in data.iterrows():
            insRow = []
            for col in columns:
                insRow.append(str(item[col]))
            insRow = tuple(insRow)
            insert.append(insRow)
            
            if x!= 0 and x%maxInsNum == 0:
                self.insert(valStr, insert)
                inserted = 1
                insert = []
                
        self.insert(valStr, insert)
    
    def pdInsert(self, table_name, data):
        #self.exe("""ALTER TABLE """ + table_name + """ DISABLE KEYS""")
        self.refresh()
        uri = 'mysql://'+self.username+':'+self.password+'@'+self.host+'/'+self.dbname+''
        engine = create_engine(uri)
    
        @event.listens_for(engine, 'before_cursor_execute')
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True
                
        data[:1].to_sql(table_name, engine, if_exists = "append", chunksize = None, index = False) 
        engine.dispose()
        
        #self.exe("""ALTER TABLE """ + table_name + """ DISABLE KEYS""")
        
        #data[1:].to_csv('tmp.csv', index = False)
        odo.odo(data[1:], str(uri) + '::' + str(table_name), local='LOCAL')
        
    def update(self, sql):
        self.refresh()
        self.cur.execute(sql)
        self.con.commit()
        
    def delete(self, sql):
        self.refresh()
        self.cur.execute(sql)
        self.con.commit()
        
    def view(self, sql):
        self.refresh()
        for row in self.cur.execute(sql):
            print (row)
    
    def exe(self, sql):
        self.refresh()
        self.cur.execute(sql)
        self.con.commit()
        
    def close(self):
        self.con.close()
    
    def getDate(self, date):
        
        if type(date) == str:
            now = dt.datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
        else:
            now = date
        month = now.month

        if month >= 2 and month <= 5:
            date = '02' + str(now.year) + '_' + '05' + str(now.year)
        elif month >= 6 and month <= 9:
            date = '06' + str(now.year) + '_' + '09' + str(now.year)
        elif month >= 10:
            date = '10' + str(now.year) + '_' + '01' + str(now.year + 1)
        elif month == 1:
            date = '10' + str(now.year - 1) + '_' + '01' + str(now.year)
        
        return date
    
    def getMaxDate(self, dbTbl):
        
        date = dbTbl[dbTbl.rfind('_') + 1:]
        month = int(date[:2]) if date[0] != '0' else int(date[1:2])
        year = int(date[2:])
        endDay = calendar.monthrange(year, month)[1]
        
        return date[2:] + '-' + date[:2] + '-' + str(endDay) + " 15:59:00"
    
    def pickDbTable(self, date, create = 1):
        
        date = self.getDate(date)
        
        if create and len(self.select("select TABLE_NAME from information_schema.tables where table_schema in ('primaryDb') and TABLE_NAME ='tblHist_Data_" + date + "'")) == 0:
    
            self.new_table('tblHist_Data_' + date, "symbol text, timestamp datetime, open real, high real, low real, close real, volume integer, processed boolean, dateSymInd bigint")
            self.exe("CREATE UNIQUE INDEX histTimeSym_" + date + " ON tblHist_Data_" + date + " (timestamp, symbol(5))") 
        
        return date
    
class logger(object):
    def log(self, logType, functionName, logMessage, close, date = ''):
        if date == '':
            date = str(dt.datetime.now(tz))[:19]
        dateIndex = date[:19].replace(':','').replace('-','').replace(' ','')
        
        db = fin_data()
        db.insert("insert into tblLogData values (%s,%s,%s,%s,%s,%s)", [(dateIndex, date, logType, functionName, logMessage, close)])
        db.close()


# In[1]:


#!jupyter nbconvert --to script cust_objs.ipynb


# In[ ]:




