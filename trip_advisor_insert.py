#-*- coding:utf-8 -*-
import MySQLdb
import traceback
import json
import datetime
from nltk.tokenize import sent_tokenize

insert_table_name = 'trip_advisor_info'

def read_file(file_name):
    f = open(file_name)
    lines = f.readlines()
    return lines

if __name__=='__main__':
    conn = MySQLdb.connect(host='seis10', user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    f_name = '/misc/projdata4/info_fil/bshi/Data/review/tripadvisor_jiweili/review.txt'
    f_lines = read_file(f_name)
    sql = 'insert into ' + insert_table_name + '(member_id, product_id, rating, title, body, date, helpful_score) values(%s, %s, %s, %s, %s, %s, %s)'
    cur = conn.cursor()
    for line in f_lines:
        json_data = json.loads(line)
        rating = json_data['ratings']['overall']
        title = json_data['title']
        body = json_data['text']
        author = json_data['author']
        author_id = author['id']
        help_ful_votes = json_data['num_helpful_votes']
        date_time = datetime.datetime.strptime(json_data['date'], '%B %d, %Y')
        product_id = json_data['offering_id']
        if author_id:
            cur.execute(sql, (author_id, product_id, rating, title, body, date_time, help_ful_votes))
            conn.commit()

