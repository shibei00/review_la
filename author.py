#-*- coding:utf-8 -*-
import MySQLdb
import traceback

if __name__=='__main__':
    f = open('/misc/projdata4/info_fil/bshi/Data/review/bing_liu/amazon-member-shortSummary.txt', 'r')
    line_list = f.readlines()
    f.close()
    conn = MySQLdb.connect(host='seis10', user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    count = 0
    for i in xrange(0, len(line_list)):
        if line_list[i].strip()=='MEMBER INFO':
            info_line=line_list[i+1]
            t_info_list = [x.strip() for x in info_line.split('\t')]
            try:
                member_id = t_info_list[0]
                member_name = t_info_list[1]
                review_number = int(t_info_list[2])
                if review_number >= 20:
                    count += review_nubmer
                    cur = conn.cursor()
                    try:
                        i_sql = 'insert into member_info(member_id, member_name, review_number) values (%s, %s, %s)'
                        cur.execute(i_sql, (member_id, member_name, review_number))
                        conn.commit()
                    except:
                        conn.rollback()
            except:
                traceback.print_exc()
    conn.close()