#-*- coding:utf-8 -*-
import MySQLdb
import traceback
import json
import datetime

review_table = 'yelp_2w_info'
member_table = 'member_2w_info'
product_table = 'product_2w_info'

def read_file(file_name):
    f = open(file_name)
    lines = f.readlines()
    return lines

def get_review_list(conn, member_review_dict, product_review_dict):
    sql = 'select * from ' + review_table
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for r in rows:
            review_id = r['id']-1
            member_id = r['member_id']
            product_id = r['product_id']
            review_list.append(review_id)
            if member_id in member_review_dict:
                member_review_dict[member_id].append(int(review_id))
            else:
                member_review_dict[member_id] = []
                member_review_dict[member_id].append(int(review_id))
            if product_id in product_review_dict:
                product_review_dict[product_id].append(int(review_id))
            else:
                product_review_dict[product_id] = []
                product_review_dict[product_id].append(int(review_id))
    except:
        traceback.print_exc()
    
if __name__=='__main__':
    conn = MySQLdb.connect(host='seis10', port=3306, user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    cur = conn.cursor(MySQLdb.cursors.DictCursor)
    review_score_dict = {}
    member_score_dict = {}
    product_score_dict = {}
    member_review_dict = {}
    product_review_dict = {}
    try:
        get_review_list(conn, member_review_dict, product_review_dict)
        sql_1 = 'select * from ' + review_table
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            id = row['id']
            DUP = row['DUP']
            EXT = row['EXT']
            DEV = row['DEV']
            ETF = row['ETF']
            RA = row['RA']
            sum = DUP + EXT + DEV + ETF + RA
            review_score_dict[str(id-1)] = sum
        sql_2 = 'select * from ' + member_table
        cur.execute(sql_2)
        rows = cur.fetchall()
        for row in rows:
            member_id = row['member_id']
            CS = row['CS']
            MNR = row['MNR']
            BST = row['BST']
            RFR = row['RFR']
            sum = CS + MNR + BST + RFR
            member_score_dict[member_id] = sum
        sql_3 = 'select * from ' + product_table
        cur.execute(sql_3)
        rows = cur.fetchall()
        for row in rows:
            product_id = row['product_id']
            p_CS = row['p_CS']
            p_MNR = row['p_MNR']
            p_BST = row['p_BST']
            p_RFR = row['p_RFR']
            sum = p_CS + p_MNR + p_BST + p_RFR
            product_score_dict[product_id] = sum
        sorted_review_score_list = sorted(reviews_score_dict.items(), key=operator.itemgetter(1), reverse=True)
        sorted_member_score_list = sorted(member_score_dict.items(), key=operator.itemgetter(1), reverse=True)
        sorted_product_score_list = sorted(product_score_dict.items(), key=operator.itemgetter(1), reverse=True)
        for i in xrange(0, 3):
            percent = 5.0 * (i+1) / 100.0
            t_len = len(sorted_review_score_list) * percent
            start_list = sorted_review_score_list[0:int(t_len)]
            end_list = sorted_review_score_list[int(-1 * t_len):]
            start_t_list = [str(x[0]) + ' ' + str(x[1]) for x in start_list]
            end_t_list = [str(x[0]) + ' ' + str(x[1]) for x in end_list]
            start_content = '\n'.join(start_t_list)
            end_content = '\n'.join(end_t_list)
            save_file(str(i) + '_1_fs.txt', start_content)
            save_file(str(i) + '_0_fs.txt', end_content)
        member_content = ''
        product_content = ''
        for item in sorted_member_score_list:
            member = item[0]
            r_list = member_review_dict[member]
            t_list = [member, str(item[1]), str(len(r_list))]
            member_content += '\t'.join(t_list) + '\t' + ' '.join([str(x) for x in r_list]) + '\n'

        for item in sorted_product_score_list:
            product = item[0]
            r_list = product_review_dict[product]
            t_list = [product, str(item[1]), str(len(r_list))]
            product_content += '\t'.join(t_list) + '\t' + ' '.join([str(x) for x in r_list]) + '\n'

        save_file('user_spam_fs.txt', member_content)
        save_file('product_spam_fs.txt', product_content)
    except:
        traceback.print_exc()
    