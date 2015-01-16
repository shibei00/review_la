#-*- coding:utf-8 -*-
import MySQLdb
import traceback

def insert_into_members(conn):
    sql = 'select distinct member_id, count(*) from review_info_incomplete group by member_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            member_id = row[0]
            count = row[1]
            cur2 = conn.cursor()
            sql2 = 'insert into member_info_incomplete(member_id, review_number) values(%s, %s)'
            cur2.execute(sql2, (member_id, str(count)))
            conn.commit()
    except:
        traceback.print_exc()
    
if __name__=='__main__':
    conn = MySQLdb.connect(host='seis10', user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    #construct review_info_incomplete table
    insert_into_members(conn)