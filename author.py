#-*- coding:utf-8 -*-
import MySQLdb

if __name__=='__main__':
    f = open('amazon-member-shortSummary.txt', 'r')
    line_list = f.readlines()
    f.close()
    for i in xrange(0, len(line_list)):
        if line_list[i]=='MEMBER INFO':
            info_line=line_list[i+1]
            t_info_list = [x.strip() for x in info_line.split('\t')]
            member_id = t_info_list[0]
            review_number = int(t_info_list[2])
            print member_id, review_number
            if review_number >= 3:
                
                #save_into_db(t_info_list[0], t_info_list[1], t_info_list[2])
    for line in line_list:
        if line.strip()=='MEMBER INFO':
            info_line = line_list