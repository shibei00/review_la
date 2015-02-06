import MySQLdb
import traceback

new_table_name = 'amazon_audio_test_info'
member_threshold = 2
product_threshold = 2

if __name__=='__main__':
    conn = MySQLdb.connect(host='seis10', user='bshi', port=3306, passwd='20141031shib', db='bshi', charset='utf8')
    sql = 'select member_id, count(*) from ' + new_table_name + ' group by member_id having count(*) < ' + str(member_threshold)
    sql2 = 'select product_id, count(*) from ' + new_table_name + ' group by product_id having count(*) < ' + str(product_threshold)
    sql3 = 'delete from ' + new_table_name + ' where member_id=%s'
    sql4 = 'delete from ' + new_table_name + ' where product_id=%s'
    cur = conn.cursor()
    cur.execute(sql)
    rows1 = cur.fetchall()
    cur.execute(sql2)
    rows2 = cur.fetchall()
    while(rows1 and rows2):
        for row in rows1:
            member_id = row[0]
            cur.execute(sql3, (member_id,))
            conn.commit()

        for row in rows2:
            product_id = row[0]
            cur.execute(sql4, (product_id,))
            conn.commit()

        cur.execute(sql)
        rows1 = cur.fetchall()
        cur.execute(sql2)
        rows2 = cur.fetchall()
        print 'The length of rows1:' + str(len(rows1))
        print 'The length of rows2:' + str(len(rows2))

    sql5 = 'select * from ' + new_table_name + ' order by id asc'
    sql6 = 'update ' + new_table_name + ' set id = %s where id = %s'
    cur.execute(sql5)
    rows = cur.fetchall()
    for i in xrange(0, len(rows)):
        id = rows[i][0]
        cur.execute(sql6, (i+1, id))