#-*- coding:utf-8 -*-
import MySQLdb
import traceback
import datetime

if __name__=='__main__':
    f = open('/misc/projdata4/info_fil/bshi/Data/review/bing_liu/reviewsNew.txt', 'r')
    line_list = f.readlines()
    f.close()
    conn = MySQLdb.connect(host='seis10', user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    member_id_dict = {}
    try:
        for i in xrange(0, len(line_list)):
            info_list = [x.strip() for x in line_list[i].split('\t')]
            try:
                member_id = info_list[0]
                product_id = info_list[1]
                date_str = info_list[2]
                date_obj = None
                if date_str:
                    date_obj = datetime.datetime.strptime(date_str, '%B %d, %Y')
                help_fb_num = info_list[3]
                fb_num = info_list[4]
                rating = float(info_list[5])
                r_title = info_list[6]
                r_body = ''
                if len(info_list) > 7:
                    r_body = info_list[7]
                if r_body and date_obj:
                    cur2 = conn.cursor()
                    m_sql2 = 'insert into review_info(member_id, product_id, date, rating, title, body, help_score) values(%s, %s, %s, %s, %s, %s, %s)'
                    cur2.execute(m_sql2, (member_id, product_id, date_obj, rating, r_title, r_body, help_fb_num))
                    cur2.close()
                    conn.commit()
            except:
                traceback.print_exc()
    except:        
        traceback.print_exc()
    conn.close()
