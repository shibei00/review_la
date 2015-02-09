#-*- coding:utf-8 -*-
import MySQLdb
import json
import traceback

review_table = 'trip_advisor_2w_info'
member_table = 'trip_advisor_2w_member'
product_table = 'trip_advisor_2w_product'
output_name = 'trip_advisor_2w_raw_profile.txt'

def read_file(file_name):
    f = open(file_name)
    lines = f.readlines()
    return lines

def read_member_product_info(all_member_info, all_product_info):
    lines = read_file('/misc/projdata4/info_fil/bshi/Data/review/tripadvisor_jiweili/review.txt')
    for line in lines:
        json_data = json.loads(line)
        author = json_data['author']
        author_id = author['id']
        all_member_info[author_id] = str(author)
    lines = read_file('/misc/projdata4/info_fil/bshi/Data/review/tripadvisor_jiweili/offering.txt')
    for line in lines:
        json_data = json.loads(line)
        product_id = json_data['id']
        print product_id
        all_product_info[product_id] = line
        
def output_txt(conn, file_name):
    raw_f = open(file_name, 'w')
    try:
        sql = 'select * from ' + review_table
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for r in rows:
            sql2 = 'select * from ' + member_table +' where member_id=%s'
            sql3 = 'select * from ' + product_table + ' where product_id=%s'
            member_id = r['member_id']
            product_id = r['product_id']
            id = r['id']
            raw_body = r['body'].replace('\n', ' ').replace('\r', '')
            post_body= r['post_body'].replace('\n', ' ').replace('\r', '')
            DUP = r['DUP']
            EXT = r['EXT']
            DEV = r['DEV']
            ETF = r['ETF']
            RA = r['RA']
            is_J = r['is_J']
            is_burst = r['is_burst']
            helpful_score = r['helpful_score']
            rating = r['rating']
            cur.execute(sql2, (member_id,))
            r_member = cur.fetchone()
            CS = r_member['CS']
            MNR = r_member['MNR']
            BST = r_member['BST']
            RFR = r_member['RFR']
            cur.execute(sql3, (product_id,))
            r_product = cur.fetchone()
            p_CS = r_product['p_CS']
            p_MNR = r_product['p_MNR']
            p_BST = r_product['p_BST']
            p_RFR = r_product['p_RFR']
            t_list = [member_id, product_id, rating, helpful_score, DUP, EXT, DEV, ETF, RA, CS, MNR, BST, RFR, is_J, is_burst, p_CS, p_MNR, p_BST, p_RFR]
            content_raw = '\t'.join(str(x) for x in t_list)
            content_raw += '\n' + raw_body.strip() + '\n'
            content_raw += all_member_info[member_id] + '\n'
            content_raw += all_product_info[product_id] + '\n'
            if type(content_raw) == unicode:
                content_raw = content_raw.encode('utf-8')
            else:
                content_raw = content_raw.decode('ascii').encode('utf-8')
            raw_f.write(content_raw)
    except:
        traceback.print_exc()
    raw_f.close()

if __name__=='__main__':
    conn = MySQLdb.connect(host='seis10.se.cuhk.edu.hk', port=3306, user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    all_member_info = {}
    all_product_info = {}
    read_member_product_info(all_member_info, all_product_info)
    output_txt(conn, output_name)