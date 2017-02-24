# --* -- encoding: utf-8 -*-
#author : lgh
import types
import MySQLdb
import  traceback
class mysqldb:
    conn = ''
    cur = ''
    lastSql =''
    def __init__(self, host, user, passwd, db, tableName='', port=3306):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        self.connection()
        self.tableName = tableName
        if tableName != '':
            self.__GetFiled(self.tableName)

    # init mysql connetction
    def connection(self):
        try:
            self.conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, port=self.port)
            self.cur = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        except MySQLdb.Error, e:
            print "MYSQL ERROR %d %s" % (e.args[0], e.args[1])
            exit(1)

    def __GetFiled(self, tableName):
        sql = 'select * from ' + tableName + ' limit 1'
        self.cur.execute(sql)

        key = [k[0] for k in self.cur.description]

        self.filed = key

    # insert
    # arr type dict
    # return inser_id
    def Insert(self, arr):
        if not type(arr) is types.DictionaryType:
            return 'arr  not is  dict'
        fileds = []
        value = []
        for key in arr:
            if not key in self.filed:
                continue

            fileds.append("`" + str(key) + "`")
            value.append("'" + MySQLdb.escape_string(str(arr[key])) + "'")
        filedKey = ','.join(fileds)
        values = ','.join(value)
        sql = "INSERT INTO  " + self.tableName + " (" + filedKey + ") " + "values" + " (" + values + ")"
        self.lastSql = sql
        try:
            self.cur.execute(sql)
            s = self.conn.insert_id()
            self.conn.commit()
            return s
        except MySQLdb.Error, e:
            print "MYSQL ERROR %d %s" % (e.args[0], e.args[1])
            exit(1)

    def Select(self, where={},order="",limit=""):
        strs = None
        if len(where):
            strs = self.where(where)
        if strs != None:
            sql = "select * from " + self.tableName + " where " + strs[0] + order + limit
        else:
            sql = 'select * from ' + self.tableName + " "+order +" "+ limit
         #排序
        # if  order:
        #     sql = sql + order
        # if limit:
        #     sql = sql + limit
        self.lastSql = sql
        try:
            self.cur.execute(sql)
            data = self.cur.fetchall()
            return data
        except:
            return False

    # delete
    # condition is str dict []
    # return row
    def delete(self, condiction):
        strs = self.where(condiction)
        if len(strs):

            sql = 'delete from ' + self.tableName + " where " + strs[0]
            self.lastSql = sql
            try:
                row = self.cur.execute(sql)
                return row
            except:
                return 'false'
        return 'condiction is not null'

    def update(self, condiction, where={}):

        strs = None
        if len(where):
            strs = self.where(where)
        if type(condiction) is types.DictionaryType:
            s = []
            for key in condiction:
                if not key in self.filed:
                    continue
                s.append(key + "=" + "'" + MySQLdb.escape_string(condiction[key]) + "'")
            if len(s) > 1:
                s = [' , '.join(s)]

            if strs != None:
                sql = 'update ' + self.tableName + ' set ' + s[0] + ' where ' + strs[0]
            else:
                sql = 'update ' + self.tableName + ' set ' + s[0]
            self.lastSql = sql
            try:
                row = self.cur.execute(sql)
                return row
            except:
                return 'false'

    # where
    # options
    def where(self, options=''):
        if type(options) is types.DictionaryType:
            arr = []
            for key in options:
                if not key in self.filed:
                    continue
                str = key + "= '" + MySQLdb.escape_string(options[key])+"'"
                arr.append(str)
            if len(arr) > 1:
                strs = [' and '.join(arr)]

                return strs
            return arr
        return 'type not is dict'


    #find single data
    def findOne(self,where={},order=""):
        strs = None
        if len(where):
            strs = self.where(where)

        if strs != None:
            sql = "select * from " + self.tableName + " where " + strs[0] + order + " limit 1"
        else:
            sql = 'select * from ' + self.tableName + " " + order + " limit 1"
        self.lastSql = sql

        try:
            self.cur.execute(sql)
            row = self.cur.fetchall()
            return row[0] if len(row) > 0 else {}
        except:
            return False
    #sql 语句查询
    def query(self,sql):
        if len(sql) == 0:
            raise Exception("sql is null")
        try:
            self.cur.execute(sql)
            row = self.cur.fetchall()
            return row;
        except Exception, e:
            print e.args
    # get last sql
    def getLastSql(self):
        return self.lastSql
    # close mysql connection
    def __del__(self):
        self.cur.close()
        self.conn.close()
#db = mysqldb("127.0.0.1", "root", "", "test", "lottery")
# lastInfo = db.findOne({},"order by time desc")
# print db.getLastSql()
# print lastInfo




