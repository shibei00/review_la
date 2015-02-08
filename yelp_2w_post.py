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
    f_name = '/misc/projdata4/info_fil/bshi/Code/review_lda/yelp_2w_raw_new.txt'
    f_lines = read_file(f_name)
    sql = 'select * from yelp_2w_info'
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    for i in xrange(0, len(f_lines)):
        if i % 2 == 0:
            j = i / 2
            r = rows[j]
            member_id = r[1]
            product_id = r[2]
            t_list = (f_lines[i]).split('\t')
            if member_id==t_list[0] and product_id==t_list[1]:
                pass
            else:
                print member_id, t_list[0], product_id, t_list[1], r[0]
                break
    '''sql = 'update trip_advisor_2w_info set is_exist=1 where member_id=%s and product_id=%s and rating=%s and helpful_score=%s and DUP=%s and EXT=%s and DEV=%s and ETF=%s and RA=%s'
    for line in f_lines:
        t_list = line.split('\t')
        if len(t_list) > 1:
            member_id = t_list[0]
            product_id = t_list[1]
            rating = t_list[2]
            help_score = t_list[3]
            DUP = t_list[4]
            EXT = t_list[5]
            DEV = t_list[6]
            ETF = t_list[7]
            RA = t_list[8]
            cur = conn.cursor()
            cur.execute(sql, (member_id, product_id, rating, help_score, DUP, EXT, DEV, ETF, RA))
            conn.commit()

    sql2 = 'delete from trip_advisor_2w_info where is_exist=0'
    cur2 = conn.cursor()
    cur2.execute(sql2)
    conn.commit()'''

    '''cur = conn.cursor()
    sql5 = 'select * from trip_advisor_2w_info order by id asc'
    sql6 = 'update trip_advisor_2w_info set id = %s where id = %s'
    cur.execute(sql5)
    rows = cur.fetchall()
    for i in xrange(0, len(rows)):
        id = rows[i][0]
        cur.execute(sql6, (i+1, id))
    conn.commit()'''