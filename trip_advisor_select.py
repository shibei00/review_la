import MySQLdb
import traceback

new_table_name = 'trip_advisor_info_incomplete'
member_threshold = 5
product_threshold = 5

if __name__=='__main__':
    conn = MySQLdb.connect(host='seis10', user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    sql = 'select member_id, count(*) from ' + new_table_name + ' group by member_id having count(*) < ' + str(member_threshold)
    sql2 = 'select product_id, count(*) from ' + new_table_name + ' group by product_id having count(*) < ' + str(product_threshold)
    sql3 = 'delete from ' + new_table_name + ' where member_id=%s'
    sql4 = 'delete from ' + new_table_name + ' where product_id=%'
    cur = conn.cursor()
    cur.execute(sql)
    rows1 = cur.fetchall()
    cur.execute(sql2)
    rows2 = cur.fetchall()
    while(rows1 and rows2):
        for row in rows1:
            member_id = row[0]
            cur.execute(sql3, member_id)
            conn.commit()

        for row in rows2:
            product_id = row[0]
            cur.execute(sql4, product_id)
            conn.commit()

        cur.execute(sql)
        rows1 = cur.fetchall()
        cur.execute(sql2)
        rows2 = cur.fetchall()
        print 'The length of rows1:' + str(len(rows1))
        print 'The length of rows2:' + str(len(rows2))

        