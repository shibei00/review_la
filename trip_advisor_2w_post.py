#-*- coding:utf-8 -*-
import MySQLdb
import traceback
import json
import datetime

def read_file(file_name):
    f = open(file_name)
    lines = f.readlines()
    return lines

if __name__=='__main__':
    conn = MySQLdb.connect(host='seis10', user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    f_name = '/misc/projdata4/info_fil/bshi/Code/review_lda/trip_advisor_2w_raw_new.txt'
    f_lines = read_file(f_name)
    sql = 'update trip_advisor_2w_info set is_exist=1 where member_id=%s and product_id=%s and rating=%s and helpful_score=%s'
    for line in f_lines:
        t_list = line.split('\t')
        if len(t_list) > 1:
            member_id = t_list[0]
            product_id = t_list[1]
            rating = t_list[2]
            help_score = t_list[3]
            cur = conn.cursor()
            cur.execute(sql, (member_id, product_id, rating, help_score))
            conn.commit()

    sql2 = 'delete from trip_advisor_2w_info where is_exist=0'
    cur2 = conn.cursor()
    cur2.execute(sql2)
    conn.commit()

    cur = conn.cursor()
    sql5 = 'select * from trip_advisor_2w_info order by id asc'
    sql6 = 'update trip_advisor_2w_info set id = %s where id = %s'
    cur.execute(sql5)
    rows = cur.fetchall()
    for i in xrange(0, len(rows)):
        id = rows[i][0]
        cur.execute(sql6, (i+1, id))
    conn.commit()