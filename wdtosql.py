#!/usr/bin/python3
import pymssql
class MSSQL:
    def __init__(self,host,user,pwd,db):
        self.host=host
        self.user=user
        self.pwd=pwd
        self.db=db
    def __GetConnect(self):
        self.conn=pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset='utf8')
        cur=self.conn.cursor()
        if not cur:
            raise(NameError,"connect database failed")
        else:
            return cur

    def ExecQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()
        self.conn.close()
        return resList
    def ExecNonQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()


def main():
    mssql = MSSQL("47.93.119.89", "aifin", "huzhiyuan5018", "wind")
    res = mssql.ExecQuery("select top 5 * from abspayment")
    print(res)


if __name__ == "__main__":
    main()
