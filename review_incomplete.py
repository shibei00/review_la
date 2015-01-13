import MySQLdb
import traceback

if __name__=='__main__':
    
    conn = MySQLdb.connect(host='seis10', user='bshi', passwd='20141031shib', db='bshi', charset='utf8')

    seedproduct = []
    seedmember = []
    result_row = {}
    product_dict = {}
    member_dict = {}
    
    sql = 'select * from review_info where member_id = %s'
    sql2 = 'select * from review_info where product_id = %s'

    sql3 = 'select product_id, count(*) from review_info group by product_id order by count(*) desc limit 0, 50'
    cur = conn.cursor()
    cur.execute(sql3)
    rows = cur.fetchall()
    for r in rows:
        seedproduct.append(r[0])
    
    count = 0

    while(len(result_row.keys()) < 15000):
        for p in seedproduct:
            count1 = 0
            if p in product_dict:
                break
            product_dict[p]=1
            cur = conn.cursor()
            cur.execute(sql2, (p,))
            rows = cur.fetchall()
            for row in rows:
                member_id = row[1]
                cur2 = conn.cursor()
                cur2.execute(sql, (member_id,))
                rows_member = cur2.fetchall()
                if len(rows_member) > 3:
                    result_row[row[0]] = row
                    seedmember.append(member_id)
                    print 'member_id:' + member_id
                    count1 += 1
                    if count1 > 300:
                        break
        seedproduct = []

        else:
            print 'the number is:' + str(len(result_row.keys()))

        for p in seedmember:
            count2 = 0
            if p in member_dict:
                break
            member_dict[p] = 1
            cur = conn.cursor()
            cur.execute(sql, (p,))
            rows = cur.fetchall()
            for row in rows:
                product_id = row[2]
                cur2 = conn.cursor()
                cur2.execute(sql2, (product_id,))
                rows_product = cur2.fetchall()
                if len(rows_product) > 3:
                    result_row[row[0]] = row
                    seedproduct.append(product_id)
                    print 'product_id:' + product_id
                    count2 += 1
                    if count2 > 200:
                        break
        seedmember = []
        print 'the number is:' + str(len(result_row.keys()))

    try:
        for k in result_row.keys():
            row = result_row[k]
            cur2 = conn.cursor()
            sql2 = 'insert into review_info_incomplete(member_id, product_id, date, rating, title, body) values(%s, %s, %s, %s, %s, %s)'
            cur2.execute(sql2, (row[1], row[2], row[3], row[4], row[5], row[6]))
    except:
        conn.rollback()
        traceback.print_exc()
    conn.commit()
    conn.close()

