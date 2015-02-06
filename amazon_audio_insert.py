#-*- coding:utf-8 -*-
import MySQLdb
import traceback
import json
import datetime
from nltk.tokenize import sent_tokenize

insert_table_name = 'amazon_audio_info'
product_table = 'amazon_audio_product'

def read_file(file_name):
    f = open(file_name)
    lines = f.readlines()
    return lines

def insert_product(conn, product_id, sales_rank, label, sales_price):
    sql = 'insert into ' + product_table + '(product_id, sales_rank, label, sales_price) values(%s, %s, %s, %s)'
    cur = conn.cursor()
    cur.execute(sql, (product_id, sales_rank, label, sales_price))
    cur.commit()
    conn.commit()

def select_review(conn, product_id):
    sql = 'select * from review_info where product_id=%s'
    cur = conn.cursor(MySQLdb.cursors.DictCursor)
    cur.execute(sql, (product_id,))
    cur.commit()
    conn.commit()
    r_list = cur.fetchall()
    return r_list
    
def insert_review(conn):
    pass
    
if __name__=='__main__':
    f_name = '/misc/projdata4/info_fil/bshi/Data/review/bing_liu/productInfoXML-reviewed-AudioCDs.txt'
    conn = MySQLdb.connect(host='seis10', user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    f_lines = read_fine(f_name)
    product_dict = {}
    for line in f_lines:
        t_lines = [l.strip() for l in line.split('\t')]
        if len(t_lines)==4:
            product_id = t_lines[0]
            sales_rank = t_lines[1]
            label = t_lines[2]
            sales_price = t_lines[3]
            product_dict[product_id] = 1
            insert_product(conn, product_id, sales_rank, label, sales_price)
    for key in product_dict:
        r_list = select_review(conn, key)
        for r in r_list:
            member_id = r['member_id']
            product_id = r['product_id']
            date = r['date']
            rating = r['rating']
            title = r['title']
            body = r['body']