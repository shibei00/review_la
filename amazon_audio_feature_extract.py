#-*- coding:utf-8 -*-
import MySQLdb
import traceback
import nltk
import string
import datetime
import MySQLdb.cursors
from nltk.stem.porter import *
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

word_dict = {}
stemmer = PorterStemmer()
review_table = 'amazon_audio_2w_info'
member_table = 'amazon_audio_2w_member'
product_table = 'amazon_audio_2w_product'

def insert_into_members(conn):
    sql = 'select distinct member_id, count(*) from ' + review_table+ ' group by member_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            member_id = row[0]
            count = row[1]
            cur2 = conn.cursor()
            sql2 = 'insert into ' + member_table + '(member_id, review_number) values(%s, %s)'
            cur2.execute(sql2, (member_id, str(count)))
            conn.commit()
    except:
        traceback.print_exc()
    print 'Extract members completed!'

def insert_into_products(conn):
    sql = 'select distinct product_id, count(*) from ' + review_table +' group by product_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[0]
            count = row[1]
            cur2 = conn.cursor()
            sql2 = 'insert into ' + product_table+'(product_id, review_number) values(%s, %s)'
            cur2.execute(sql2, (product_id, str(count)))
            conn.commit()
    except:
        traceback.print_exc()
    print 'Extract products completed!'
    
def preprocess(conn):
    sql = 'select * from ' + review_table
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            body = row['body']
            cur2 = conn.cursor(MySQLdb.cursors.DictCursor)
            update_body = str_process(body)
            sql2 = 'update ' + review_table + ' set post_body = %s where id = %s'
            cur2.execute(sql2, (update_body, row['id']))
        conn.commit()
    except:
        traceback.print_exc()

pro_dict = dict((ord(char), None) for char in string.punctuation)
def str_process(s):
    text = s.lower()
    text_no_punctuation = ''
    if type(text)==unicode:
        text_no_punctuation = text.translate(pro_dict)
    if type(text)==str:
        text_no_punctuation = text.translate(None, string.punctuation)        
    tokens = nltk.word_tokenize(text_no_punctuation)
    filter_stop_text = [w for w in tokens if not w in stopwords.words('english')]
    stemmed = []

    for item in filter_stop_text:
        stemmed.append(stemmer.stem(item))
    return ' '.join(stemmed)
    
def extract_CS(conn):
    sql = 'select * from ' + member_table
    tfidf_vectorizer = TfidfVectorizer()    
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            member_id = row['member_id']
            words = []
            sql2 = 'select * from ' + review_table + ' where member_id = %s'
            cur2 = conn.cursor(MySQLdb.cursors.DictCursor)
            cur2.execute(sql2, (member_id,))
            reviews = cur2.fetchall()
            for review in reviews:
                body = review['post_body']
                words.append(body)
            max_value = 0.0
            try:
                tfidf_matrix = tfidf_vectorizer.fit_transform(words).todense()
                max_value = 0.0
                for i in xrange(0, tfidf_matrix.shape[0]):
                    sim = cosine_similarity(tfidf_matrix[i:i+1], tfidf_matrix)
                    for j in xrange(i+1, tfidf_matrix.shape[0]):
                        max_value = max(max_value, sim[0][j])
            except ValueError:
                max_value = 0.0
            sql3 = 'update ' + member_table + ' set CS=%s where member_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (max_value, member_id))
            conn.commit()
    except:
        traceback.print_exc()

def extract_p_CS(conn):
    sql = 'select * from ' + product_table
    tfidf_vectorizer = TfidfVectorizer()    
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row['product_id']
            words = []
            sql2 = 'select * from '+ review_table +' where product_id = %s'
            cur2 = conn.cursor(MySQLdb.cursors.DictCursor)
            cur2.execute(sql2, (product_id,))
            reviews = cur2.fetchall()
            for review in reviews:
                body = review['post_body']
                words.append(body)
            max_value = 0.0
            try:
                tfidf_matrix = tfidf_vectorizer.fit_transform(words).todense()
                max_value = 0.0
                for i in xrange(0, tfidf_matrix.shape[0]):
                    sim = cosine_similarity(tfidf_matrix[i:i+1], tfidf_matrix)
                    for j in xrange(i+1, tfidf_matrix.shape[0]):
                        max_value = max(max_value, sim[0][j])
            except ValueError:
                max_value = 0.0
            sql3 = 'update '+ product_table +' set p_CS=%s where product_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (max_value, product_id))
            conn.commit()                
    except:
        traceback.print_exc()
        
def extract_MNR(conn):
    sql = 'select * from ' + member_table
    sql_max_all = 'select distinct member_id, date, count(*) from ' + review_table +' group by member_id, date order by count(*) desc'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur2 = conn.cursor()
        cur2.execute(sql_max_all)
        max_review_num = cur2.fetchone()[2]
        for row in rows:
            member_id = row[1]
            sql2 = 'select distinct member_id, date, count(*) from '+review_table+' where member_id=%s group by member_id, date order by count(*) desc'
            cur3 = conn.cursor()
            cur3.execute(sql2, (member_id,))
            max_member_review_num = cur3.fetchone()[2]
            MNR = float(max_member_review_num) / float(max_review_num)
            sql4 = 'update '+ member_table +' set MNR=%s where member_id=%s'
            cur4 = conn.cursor()
            cur4.execute(sql4, (MNR, member_id))
            conn.commit()
        pass
    except:
        traceback.print_exc()

def extract_p_MNR(conn):
    sql = 'select * from ' + product_table
    sql_max_all = 'select distinct product_id, date, count(*) from ' + review_table +' group by product_id, date order by count(*) desc'
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        cur2 = conn.cursor()
        cur2.execute(sql_max_all)
        max_review_num = cur2.fetchone()[2]
        for row in rows:
            product_id = row['product_id']
            sql2 = 'select distinct count(*) from ' + review_table + ' where product_id=%s group by product_id, date order by count(*) desc'
            cur3 = conn.cursor()
            cur3.execute(sql2, (product_id,))
            max_product_review_num = cur3.fetchone()[0]
            p_MNR = float(max_product_review_num) / float(max_review_num)
            sql4 = 'update ' + product_table + ' set p_MNR=%s where product_id=%s'
            cur4 = conn.cursor()
            cur4.execute(sql4, (p_MNR, product_id))
            conn.commit()
        pass
    except:
        traceback.print_exc()
    
def extract_BST(conn):
    sql = 'select * from ' + member_table
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            member_id = row['member_id']
            sql2 = 'select * from ' + review_table +' where member_id = %s'
            cur2 = conn.cursor(MySQLdb.cursors.DictCursor)
            cur2.execute(sql2, (member_id,))
            reviews = cur2.fetchall()
            min_date = reviews[0]['date']
            max_date = min_date
            for review in reviews:
                r_date = review['date']
                min_date = min(min_date, r_date)
                max_date = max(max_date, r_date)
            timedelta = (max_date - min_date).days
            BST = 0.0
            if timedelta > 28:
                BST = 0.0
            else:
                BST = 1 - float(timedelta) / 28.0
            sql3 = 'update ' + member_table + ' set BST=%s where member_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (BST, member_id))
            conn.commit()
    except:
        traceback.print_exc()

def extract_p_BST(conn):
    sql = 'select * from ' + product_table
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row['product_id']
            sql2 = 'select * from ' + review_table +' where product_id = %s'
            cur.execute(sql2, (product_id,))
            reviews = cur.fetchall()
            min_date = reviews[0]['date']
            max_date = min_date
            for review in reviews:
                r_date = review['date']
                min_date = min(min_date, r_date)
                max_date = max(max_date, r_date)
            timedelta = (max_date - min_date).days
            BST = 0.0
            if timedelta > 28:
                BST = 0.0
            else:
                BST = 1 - float(timedelta) / 28.0
            sql3 = 'update ' + product_table + ' set p_BST=%s where product_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (BST, product_id))
            conn.commit()
    except:
        traceback.print_exc()


def extract_RFR(conn):
    sql = 'select * from ' + member_table
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            member_id = row['member_id']
            member_review_number = row['review_number']
            sql2 = 'select count(*) from ' + review_table + ' where member_id = %s and p_is_first = 1'
            cur2 = conn.cursor()
            cur2.execute(sql2, (member_id,))
            count = cur2.fetchone()
            RFR = float(count[0]) / float(member_review_number)
            sql3 = 'update ' + member_table + ' set RFR=%s where member_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (RFR, member_id))
            conn.commit()
    except:
        traceback.print_exc()
    pass

def extract_p_RFR(conn):
    sql = 'select * from ' + product_table
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row['product_id']
            product_review_number = row['review_number']
            sql2 = 'select count(*) from ' + review_table + ' where product_id = %s and m_is_first = 1'
            cur2 = conn.cursor()
            cur2.execute(sql2, (product_id,))
            count = cur2.fetchone()
            RFR = float(count[0]) / float(product_review_number)
            sql3 = 'update ' + product_table + ' set p_RFR=%s where product_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (RFR, product_id))
            conn.commit()
    except:
        traceback.print_exc()


def set_m_is_first(conn):
    sql = 'select member_id, min(date) from ' + review_table +' group by member_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            member_id = row[0]
            min_date = row[1]
            cur2 = conn.cursor()
            sql2 = 'update ' + review_table + ' set m_is_first=%s where member_id=%s and date=%s'
            cur2.execute(sql2, (1, member_id, min_date))
            conn.commit()
    except:
        traceback.print_exc()
    
def extract_DUP(conn):
    sql = 'select distinct product_id from ' + review_table + ' group by product_id'
    tfidf_vectorizer = TfidfVectorizer(min_df=1)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[0]
            sql2 = 'select * from ' + review_table + ' where product_id=%s'
            cur2 = conn.cursor(MySQLdb.cursors.DictCursor)
            cur2.execute(sql2, (product_id,))
            reviews = cur2.fetchall()
            words = []
            id_list = []
            for r in reviews:
                id = r['id']
                body = r['post_body']
                words.append(body)
                id_list.append(id)
            sql3 = 'update ' + review_table +' set DUP=%s where id=%s'
            try:    
                tfidf_matrix = tfidf_vectorizer.fit_transform(words)
                for i in xrange(0, len(words)):
                    sim = cosine_similarity(tfidf_matrix[i:i+1], tfidf_matrix)
                    flag = 0
                    for j in xrange(0, len(words)):
                        if i!=j:
                            if sim[0][j] > 0.7:
                                flag = 1
                    cur.execute(sql3, (flag, id_list[i]))
                    conn.commit()
            except ValueError:
                for i in xrange(0, len(words)):
                    cur.execute(sql3, (0, id_list[i]))
                    conn.commit()
    except:
        traceback.print_exc()

def extract_EXT(conn):
    sql = 'select * from ' + review_table
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        EXT = 0
        for r in rows:
            id = r['id']
            rating = r['rating']
            if rating==5 or rating==1:
                EXT = 1
            else:
                EXT = 0
            sql2 = 'update ' + review_table + ' set EXT=%s where id=%s'
            cur.execute(sql2, (EXT, id))
            conn.commit()
    except:
        traceback.print_exc()

def extract_DEV(conn):
    sql = 'select distinct product_id from ' + review_table + ' group by product_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur2 = conn.cursor(MySQLdb.cursors.DictCursor)
        for row in rows:
            product_id = row[0]
            sql2 = 'select * from ' + review_table + ' where product_id=%s'
            cur2.execute(sql2, (product_id,))
            reviews = cur2.fetchall()
            dict_star = {}
            dict_member = {}
            sum = 0
            for r in reviews:
                sum += r['rating']
                if r['member_id'] in dict_star:
                    dict_star[r['member_id']] += r['rating']
                    dict_member[r['member_id']] += 1
                else:
                    dict_star[r['member_id']] = r['rating']
                    dict_member[r['member_id']] = 1
            DEV = 0
            for r in reviews:
                if len(dict_member.keys())==1:
                    DEV = 0
                else:
                    exp_r = (sum - dict_star[r['member_id']]) / float(len(reviews)-dict_member[r['member_id']])
                    if (r['rating'] - exp_r)/4.0 > 0.63:
                        DEV = 1
                    else:
                        DEV = 0
                sql3 = 'update ' + review_table + ' set DEV=%s where id=%s'
                cur.execute(sql3, (DEV, r['id']))
                conn.commit()
    except:
        traceback.print_exc()
    
def set_review_is_first(conn):
    sql = 'select distinct product_id, min(date) from ' + review_table +' group by product_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[0]
            min_date = row[1]
            cur2 = conn.cursor()
            sql2 = 'update ' + review_table + ' set p_is_first=%s where product_id=%s and date=%s'
            cur2.execute(sql2, (1, product_id, min_date))
            conn.commit()
    except:
        traceback.print_exc()

def extract_ETF(conn):
    sql = 'select distinct product_id from ' + review_table + ' group by product_id'
    try:
        cur = conn.cursor()
        cur2 = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[0]
            sql2 = 'select * from '+ review_table + ' where product_id=%s and p_is_first=1'
            cur2.execute(sql2, (product_id,))
            r = cur2.fetchone()
            A_p = r['date']
            sql3 = 'select member_id, max(date) from ' + review_table +' where product_id=%s group by member_id'
            cur.execute(sql3, (product_id,))
            reviews = cur.fetchall()
            for review in reviews:
                member_id = review[0]
                max_date = review[1]
                ETF = 0.0
                flag_etf = 0
                if (max_date-A_p).days / 30.0 > 7:
                    ETF = 0.0
                else:
                    ETF = 1.0 - (max_date-A_p).days / (30.0 * 7)
                if ETF > 0.69:
                    flag_etf = 1
                sql4 = 'update ' + review_table + ' set ETF=%s where product_id=%s and member_id=%s'
                cur.execute(sql4,(flag_etf, product_id, member_id))
                conn.commit()
    except:
        traceback.print_exc()

def extract_RA(conn):
    sql1 = 'select member_id, product_id, max(rating), min(rating), count(*) from ' +review_table +' group by product_id, member_id'
    try:
        cur = conn.cursor()
        cur.execute(sql1)
        rows = cur.fetchall()
        for r in rows:
            member_id = r[0]
            product_id = r[1]
            max_rating = r[2]
            min_rating = r[3]
            rp_num = r[4]
            ra_score = float(rp_num) * (1.0-0.25*(max_rating-min_rating))
            RA = 0
            if ra_score > 2.01:
                RA = 1
            else:
                RA = 0
            sql2 = 'update ' + review_table + ' set RA=%s where member_id=%s and product_id=%s'
            cur.execute(sql2, (RA, member_id, product_id))
            conn.commit()
    except:
        traceback.print_exc()

def extract_is_J(conn):
    sql = 'select * from ' + product_table
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row['product_id']
            product_review_number = row['review_number']
            sql2 = 'select * from ' + review_table + ' where product_id = %s'
            cur2 = conn.cursor(MySQLdb.cursors.DictCursor)
            cur2.execute(sql2, (product_id,))
            reviews = cur2.fetchall()
            score_dict = {}
            score_dict[1] = 0
            score_dict[2] = 0
            score_dict[3] = 0
            score_dict[4] = 0
            score_dict[5] = 0
            is_J = 0
            for review in reviews:
                score_dict[review['rating']] += 1
            if min(score_dict[1], score_dict[5]) > max(score_dict[2], score_dict[3], score_dict[4]):
                is_J = 1
            else:
                is_J = 0
            for review in reviews:
                sql3 = 'update ' + review_table + ' set is_J=%s where id=%s'
                cur2.execute(sql3, (is_J, review['id']))
                conn.commit()
    except:
        traceback.print_exc()

def extract_is_burst(conn):
    sql = 'select * from ' + product_table
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row['product_id']
            product_review_number = row['review_number']
            sql2 = 'select * from ' + review_table + ' where product_id=%s order by date asc'
            cur2 = conn.cursor(MySQLdb.cursors.DictCursor)
            cur2.execute(sql2, (product_id,))
            reviews = cur2.fetchall()
            for r in reviews:
                begin_date = r['date']
                end_date = begin_date + datetime.timedelta(days=10)
                sql3 = 'select count(*) from ' + review_table + ' where product_id=%s and date > %s and date < %s'
                cur2.execute(sql3, (product_id, begin_date, end_date))
                t_r = cur2.fetchall()
                if len(t_r) > 20:
                    is_burst = 1
                    sql4 = 'update ' + review_table + ' set is_burst=%s where product_id=%s and date > %s and date < %s'
                    cur2.execute(sql4, (is_burst, product_id, begin_date, end_date))
                    conn.commit()
                    break
    except:
        traceback.print_exc()
    pass

def output_txt(conn, file_name):
    raw_file_name = file_name + '_raw.txt'
    post_file_name = file_name + '_post.txt'
    raw_f = open(raw_file_name, 'w')
    post_f = open(post_file_name, 'w')
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
            content_post = '\t'.join(str(x) for x in t_list)
            content_raw += '\n' + raw_body.strip() + '\n'
            content_post += '\n' + post_body.strip() + '\n'
            if type(content_raw) == unicode:
                content_raw = content_raw.encode('utf-8')
            else:
                content_raw = content_raw.decode('ascii').encode('utf-8')
            if type(content_post) == unicode:
                content_post = content_post.encode('utf-8')
            else:
                content_post = content_post.decode('ascii').encode('utf-8')
            raw_f.write(content_raw)
            post_f.write(content_post)
    except:
        traceback.print_exc()
    raw_f.close()
    post_f.close()
    
if __name__=='__main__':
    conn = MySQLdb.connect(host='seis10.se.cuhk.edu.hk', port=3306, user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    #construct review_info_incomplete table
    insert_into_members(conn)
    print 'insert member completed'
    #preprocess body of the review
    preprocess(conn)
    print 'preprocess completed.'
    #extract the feature of CS
    extract_CS(conn)
    print 'CS completed'
    #extract the feature of MNR
    extract_MNR(conn)
    print 'MNR completed'
    #extract the feature of BST
    extract_BST(conn)
    print 'BST completed'
    set_review_is_first(conn)
    print 'set review first completed'
    #extract the feature of RFR
    extract_RFR(conn)
    print 'RFR completed'
    #extract the feature of DUP
    extract_DUP(conn)
    print 'DUP completed'
    #extract the feature of EXT
    extract_EXT(conn)
    print 'EXT completed'
    #extract the feature of DEV
    extract_DEV(conn)
    print 'DEV completed'
    #extract the feature of ETF
    extract_ETF(conn)
    print 'ETF completed'
    extract_RA(conn)
    print 'RA completed'
    #construct product_info_incomplete table
    insert_into_products(conn)
    print 'insert product completed'
    extract_p_CS(conn)
    print 'p CS completed'
    extract_p_MNR(conn)
    print 'p MNR completed'
    extract_p_BST(conn)
    print 'p BST completed'
    set_m_is_first(conn)
    print 'set m first completed'
    extract_p_RFR(conn)
    print 'p RFR completed'
    extract_is_J(conn)
    print 'J completed'
    extract_is_burst(conn)
    print 'burst completed'
    output_txt(conn, 'trip_advisor_2w')
    print str_process('I have eaten!. Nice to meet you.')