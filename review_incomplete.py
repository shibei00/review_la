import MySQLdb
import traceback

if __name__=='__main__':
    seedproduct = ['0895260174',]
    seedmember = []
    result_row = {}
    
    conn = MySQLdb.connect(host='seis10', user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    sql = 'select * from review_info where member_id = %s'
    sql2 = 'select * from review_info where product_id = %s'

    count = 0

    while(len(result_row.keys()) < 20000):
        for p in seedproduct:
            cur = conn.cursor()
            cur.execute(sql2, (p,))
            rows = cur.fetchall()
            for row in rows:
                member_id = row[1]
                cur2 = conn.cursor()
                cur2.execute(sql, (member_id,))
                rows_member = cur2.fetchall()
                if len(rows_member) > 10 and len(rows_member) < 100:
                    result_row[row] = 1
                    seedmember.append(member_id)
        seedproduct = []
        if len(result_row.keys()) > 20000:
            break
            
        for p in seedmember:
            cur = conn.cursor()
            cur.execute(sql, p)
            rows = cur.fetchall()
            for row in rows():
                product_id = row[2]
                cur2 = conn.cursor()
                cur2.execute(sql2, (product_id,))
                rows_product = cur2.fetchall()
                if len(rows_product) > 10 and len(rows_product) < 100:
                    result_row[row] = 1
                    seedproduct.append(product_id)
        seedmember = []


        try:
            for row in result_row.keys():
                cur2 = conn.cursor()
                sql2 = 'insert into review_info_incomplete(member_id, product_id, date, rating, title, body) values(%s, %s, %s, %s, %s, %s)'
                cur2.execute(sql2, (row[1], row[2], row[3], row[4], row[5], row[6]))
        except:
            conn.rollback()
            traceback.print_exc()
    conn.commit()
    conn.close()

