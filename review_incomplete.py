import MySQLdb
import traceback

if __name__=='__main__':
    f = open('result_list.txt')
    line_list = f.readlines()
    f.close()
    conn = MySQLdb.connect(host='seis10', user='bshi', passwd='20141031shib', db='bshi', charset='utf8')    
    for line in line_list:
        info_list = [x.strip() for x in line.split()]
        member_id = info_list[0]
        cur = conn.cursor()
        sql = 'select * from review_info where member_id=%s'
        try:
            cur.execute(sql, member_id)
            rows = cur.fetchall()
            cur.close()
            for row in rows:
                cur2 = conn.cursor()
                sql2 = 'insert into review_info values(%s, %s, %s, %s, %s, %s)'
                cur2.execute(sql2, row[1], row[2], row[3], row[4], row[5], row[6])
                cur.close()
        except:
            conn.rollback()
    conn.commit()
    conn.close()

