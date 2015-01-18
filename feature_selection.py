#-*- coding:utf-8 -*-
import MySQLdb
import traceback
import nltk
import string
import datetime
from nltk.stem.porter import *
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

word_dict = {}
stemmer = PorterStemmer()

def insert_into_members(conn):
    sql = 'select distinct member_id, count(*) from review_info_incomplete group by member_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            member_id = row[0]
            count = row[1]
            cur2 = conn.cursor()
            sql2 = 'insert into member_info_incomplete(member_id, review_number) values(%s, %s)'
            cur2.execute(sql2, (member_id, str(count)))
            conn.commit()
    except:
        traceback.print_exc()
    print 'Extract members completed!'

def insert_into_products(conn):
    sql = 'select distinct product_id, count(*) from review_info_incomplete group by product_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[0]
            count = row[1]
            cur2 = conn.cursor()
            sql2 = 'insert into product_info_incomplete(product_id, review_number) values(%s, %s)'
            cur2.execute(sql2, (product_id, str(count)))
            conn.commit()
    except:
        traceback.print_exc()
    print 'Extract products completed!'
    
def preprocess(conn):
    sql = 'select * from review_info_incomplete'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            body = row[6]
            cur2 = conn.cursor()
            update_body = str_process(body)
            sql2 = 'update review_info_incomplete set body = %s where id = %s'
            cur2.execute(sql2, (update_body, row[0]))
        conn.commit()
    except:
        traceback.print_exc()

def str_process(s):
    text = s.lower().encode('utf8')
    text_no_punctuation = text.translate(None, string.punctuation)
    tokens = nltk.word_tokenize(text_no_punctuation)
    filter_stop_text = [w for w in tokens if not w in stopwords.words('english')]
    stemmed = []

    for item in filter_stop_text:
        stemmed.append(stemmer.stem(item))
    return ' '.join(stemmed)
    
def extract_CS(conn):
    sql = 'select * from member_info_incomplete'
    tfidf_vectorizer = TfidfVectorizer()    
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            member_id = row[1]
            words = []
            sql2 = 'select * from review_info_incomplete where member_id = %s'
            cur2 = conn.cursor()
            cur2.execute(sql2, (member_id,))
            reviews = cur2.fetchall()
            for review in reviews:
                body = review[6]
                words.append(body)
            tfidf_matrix = tfidf_vectorizer.fit_transform(words).todense()
            max_value = 0.0
            for i in xrange(0, tfidf_matrix.shape[0]):
                sim = cosine_similarity(tfidf_matrix[i:i+1], tfidf_matrix)
                for j in xrange(i+1, tfidf_matrix.shape[0]):
                    max_value = max(max_value, sim[0][j])
            sql3 = 'update member_info_incomplete set CS=%s where member_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (max_value, member_id))
            conn.commit()
    except:
        traceback.print_exc()

def extract_p_CS(conn):
    sql = 'select * from product_info_incomplete'
    tfidf_vectorizer = TfidfVectorizer()    
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[1]
            words = []
            sql2 = 'select * from review_info_incomplete where product_id = %s'
            cur2 = conn.cursor()
            cur2.execute(sql2, (product_id,))
            reviews = cur2.fetchall()
            for review in reviews:
                body = review[6]
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
            sql3 = 'update product_info_incomplete set p_CS=%s where product_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (max_value, product_id))
            conn.commit()                
    except:
        traceback.print_exc()
        
def extract_MNR(conn):
    sql = 'select * from member_info_incomplete'
    sql_max_all = 'select distinct member_id, date, count(*) from review_info_incomplete group by member_id, date order by count(*) desc'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur2 = conn.cursor()
        cur2.execute(sql_max_all)
        max_review_num = cur2.fetchone()[2]
        for row in rows:
            member_id = row[1]
            sql2 = 'select distinct count(*) from review_info_incomplete where member_id=%s group by member_id, date order by count(*) desc'
            cur3 = conn.cursor()
            cur3.execute(sql2, (member_id,))
            max_member_review_num = cur3.fetchone()[0]
            MNR = float(max_member_review_num) / float(max_review_num)
            sql4 = 'update member_info_incomplete set MNR=%s where member_id=%s'
            cur4 = conn.cursor()
            cur4.execute(sql4, (MNR, member_id))
            conn.commit()
        pass
    except:
        traceback.print_exc()

def extract_p_MNR(conn):
    sql = 'select * from product_info_incomplete'
    sql_max_all = 'select distinct product_id, date, count(*) from review_info_incomplete group by product_id, date order by count(*) desc'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur2 = conn.cursor()
        cur2.execute(sql_max_all)
        max_review_num = cur2.fetchone()[2]
        for row in rows:
            product_id = row[1]
            sql2 = 'select distinct count(*) from review_info_incomplete where product_id=%s group by product_id, date order by count(*) desc'
            cur3 = conn.cursor()
            cur3.execute(sql2, (product_id,))
            max_product_review_num = cur3.fetchone()[0]
            p_MNR = float(max_product_review_num) / float(max_review_num)
            sql4 = 'update product_info_incomplete set p_MNR=%s where product_id=%s'
            cur4 = conn.cursor()
            cur4.execute(sql4, (p_MNR, product_id))
            conn.commit()
        pass
    except:
        traceback.print_exc()
    
def extract_BST(conn):
    sql = 'select * from member_info_incomplete'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            member_id = row[1]
            sql2 = 'select * from review_info_incomplete where member_id = %s'
            cur2 = conn.cursor()
            cur2.execute(sql2, (member_id,))
            reviews = cur2.fetchall()
            min_date = reviews[0][3]
            max_date = min_date
            for review in reviews:
                r_date = review[3]
                min_date = min(min_date, r_date)
                max_date = max(max_date, r_date)
            timedelta = (max_date - min_date).days
            BST = 0.0
            if timedelta > 28:
                BST = 0.0
            else:
                BST = 1 - float(timedelta) / 28.0
            sql3 = 'update member_info_incomplete set BST=%s where member_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (BST, member_id))
            conn.commit()
    except:
        traceback.print_exc()

def extract_p_BST(conn):
    sql = 'select * from product_info_incomplete'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[1]
            sql2 = 'select * from review_info_incomplete where product_id = %s'
            cur2 = conn.cursor()
            cur2.execute(sql2, (product_id,))
            reviews = cur2.fetchall()
            min_date = reviews[0][3]
            max_date = min_date
            for review in reviews:
                r_date = review[3]
                min_date = min(min_date, r_date)
                max_date = max(max_date, r_date)
            timedelta = (max_date - min_date).days
            BST = 0.0
            if timedelta > 28:
                BST = 0.0
            else:
                BST = 1 - float(timedelta) / 28.0
            sql3 = 'update product_info_incomplete set p_BST=%s where product_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (BST, product_id))
            conn.commit()
    except:
        traceback.print_exc()


def extract_RFR(conn):
    sql = 'select * from member_info_incomplete'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            member_id = row[1]
            member_review_number = row[2]
            sql2 = 'select count(*) from review_info_incomplete where member_id = %s and is_first = 1'
            cur2 = conn.cursor()
            cur2.execute(sql2, (member_id,))
            count = cur2.fetchone()
            RFR = float(count[0]) / float(member_review_number)
            sql3 = 'update member_info_incomplete set RFR=%s where member_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (RFR, member_id))
            conn.commit()
    except:
        traceback.print_exc()
    pass

def extract_p_RFR(conn):
    sql = 'select * from product_info_incomplete'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[1]
            product_review_number = row[2]
            sql2 = 'select count(*) from review_info_incomplete where product_id = %s and p_is_first = 1'
            cur2 = conn.cursor()
            cur2.execute(sql2, (product_id,))
            count = cur2.fetchone()
            RFR = float(count[0]) / float(product_review_number)
            sql3 = 'update product_info_incomplete set p_RFR=%s where product_id=%s'
            cur3 = conn.cursor()
            cur3.execute(sql3, (RFR, product_id))
            conn.commit()
    except:
        traceback.print_exc()
    pass

def set_p_is_first(conn):
    sql = 'select member_id, min(date) from review_info_incomplete group by member_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            member_id = row[0]
            min_date = row[1]
            cur2 = conn.cursor()
            sql2 = 'update review_info_incomplete set p_is_first=%s where member_id=%s and date=%s'
            cur2.execute(sql2, (1, member_id, min_date))
            conn.commit()
    except:
        traceback.print_exc()

    pass
    
def extract_DUP(conn):
    sql = 'select distinct product_id from review_info_incomplete group by product_id'
    tfidf_vectorizer = TfidfVectorizer(min_df=1)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[0]
            sql2 = 'select * from review_info_incomplete where product_id=%s'
            cur.execute(sql2, (product_id,))
            reviews = cur.fetchall()
            words = []
            id_list = []
            for r in reviews:
                id = r[0]
                body = r[6]
                words.append(body)
                id_list.append(id)
            sql3 = 'update review_info_incomplete set DUP=%s where id=%s'
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
    sql = 'select * from review_info_incomplete'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        EXT = 0
        for r in rows:
            id = r[0]
            rating = r[4]
            if rating==5 or rating==1:
                EXT = 1
            else:
                EXT = 0
            sql2 = 'update review_info_incomplete set EXT=%s where id=%s'
            cur.execute(sql2, (EXT, id))
            conn.commit()
    except:
        traceback.print_exc()

def extract_DEV(conn):
    sql = 'select distinct product_id from review_info_incomplete group by product_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[0]
            sql2 = 'select * from review_info_incomplete where product_id=%s'
            cur.execute(sql2, (product_id,))
            reviews = cur.fetchall()
            dict_star = {}
            dict_member = {}
            sum = 0
            for r in reviews:
                sum += r[4]
                if r[1] in dict_star:
                    dict_star[r[1]] += r[4]
                    dict_member[r[1]] += 1
                else:
                    dict_star[r[1]] = r[4]
                    dict_member[r[1]] = 1
            DEV = 0
            for r in reviews:
                if len(dict_member.keys())==1:
                    DEV = 0
                else:
                    exp_r = (sum - dict_star[r[1]]) / float(len(reviews)-dict_member[r[1]])
                    if (r[4] - exp_r)/4.0 > 0.63:
                        DEV = 1
                    else:
                        DEV = 0
                sql3 = 'update review_info_incomplete set DEV=%s where id=%s'
                cur.execute(sql3, (DEV, r[0]))
                conn.commit()
    except:
        traceback.print_exc()
    
    
def set_review_is_first(conn):
    sql = 'select distinct product_id, min(date) from review_info_incomplete group by product_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[0]
            min_date = row[1]
            cur2 = conn.cursor()
            sql2 = 'update review_info_incomplete set is_first=%s where product_id=%s and date=%s'
            cur2.execute(sql2, (1, product_id, min_date))
            conn.commit()
    except:
        traceback.print_exc()

def extract_ETF(conn):
    sql = 'select distinct product_id from review_info_incomplete group by product_id'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[0]
            sql2 = 'select * from review_info_incomplete where product_id=%s and is_first=1'
            cur.execute(sql2, (product_id,))
            r = cur.fetchone()
            A_p = r[3]
            sql3 = 'select member_id, max(date) from review_info_incomplete where product_id=%s group by member_id'
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
                sql4 = 'update review_info_incomplete set ETF=%s where product_id=%s and member_id=%s'
                cur.execute(sql4,(flag_etf, product_id, member_id))
                conn.commit()
    except:
        traceback.print_exc()

def extract_RA(conn):
    sql1 = 'select member_id, product_id, max(rating), min(rating), count(*) from review_info_incomplete group by product_id, member_id'
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
            sql2 = 'update review_info_incomplete set RA=%s where member_id=%s and product_id=%s'
            cur.execute(sql2, (RA, member_id, product_id))
            conn.commit()
    except:
        traceback.print_exc()

def extract_is_J(conn):
    sql = 'select * from product_info_incomplete'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[1]
            product_review_number = row[2]
            sql2 = 'select * from review_info_incomplete where product_id = %s'
            cur2 = conn.cursor()
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
                score_dict[review[4]] += 1
            if min(score_dict[1], score_dict[5]) > max(score_dict[2], score_dict[3], score_dict[4]):
                is_J = 1
            else:
                is_J = 0
            for review in reviews:
                sql3 = 'update review_info_incomplete set is_J=%s where id=%s'
                cur2.execute(sql3, (is_J, review[0]))
                conn.commit()
    except:
        traceback.print_exc()
    pass

def extract_is_burst(conn):
    sql = 'select * from product_info_incomplete'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            product_id = row[1]
            product_review_number = row[2]
            sql2 = 'select * from review_info_incomplete where product_id=%s order by date asc'
            cur2 = conn.cursor()
            cur2.execute(sql2, (product_id,))
            reviews = cur2.fetchall()
            for r in reviews:
                begin_date = r[3]
                end_date = begin_date + datetime.timedelta(days=10)
                sql3 = 'select count(*) from review_info_incomplete where product_id=%s and date > %s and date < %s'
                cur2.execute(sql3, (product_id, begin_date, end_date))
                t_r = cur2.fetchall()
                if t_r > 20:
                    is_burst = 1
                    sql4 = 'update review_info_incomplete set is_burst=%s where product_id=%s and date > %s and date < %s'
                    cur2.execute(sql4, (is_burst, product_id, begin_date, end_date))
                    conn.commit()
                    break
    except:
        traceback.print_exc()
    pass

def output_txt(conn):
    f = open('review_feature.txt', 'w')
    try:
        sql = 'select * from review_info_incomplete'        
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for r in rows:
            sql2 = 'select * from member_info_incomplete where member_id=%s'
            sql3 = 'select * from product_info_incomplete where product_id=%s'
            member_id = r[1]
            product_id = r[2]
            body= r[6]
            DUP = r[7]
            EXT = r[8]
            DEV = r[9]
            ETF = r[10]
            RA = r[11]
            is_J = r[14]
            is_burst = r[15]
            cur.execute(sql2, (member_id,))
            r_member = cur.fetchone()
            CS = r_member[3]
            MNR = r_member[4]
            BST = r_member[5]
            RFR = r_member[6]
            cur.execute(sql3, (product_id,))
            r_product = cur.fetchone()
            p_CS = r_product[3]
            p_MNR = r_product[4]
            p_BST = r_product[5]
            p_RFR = r_product[6]
            t_list = [member_id, product_id, DUP, EXT, DEV, ETF, RA, CS, MNR, BST, RFR, is_J, is_burst, p_CS, p_MNR, p_BST, p_RFR]
            content = '\t'.join(str(x) for x in t_list)
            content += '\n' + body.strip() + '\n'
            f.write(content)
    except:
        traceback.print_exc()
    f.close()
if __name__=='__main__':
    conn = MySQLdb.connect(host='127.0.0.1', port=9990, user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    #construct review_info_incomplete table
    #insert_into_members(conn)
    #preprocess body of the review
    #preprocess(conn)
    #extract the feature of CS
    #extract_CS(conn)
    #extract the feature of MNR
    #extract_BST(conn)
    #set_review_is_first(conn)
    #extract the feature of RFR
    #extract_RFR(conn)
    #extract the feature of DUP
    #extract_DUP(conn)
    #extract the feature of EXT
    #extract_EXT(conn)
    #extract the feature of DEV
    #extract_DEV(conn)
    #extract the feature of ETF
    #extract_ETF(conn)
    #extract_RA(conn)
    #construct product_info_incomplete table
    #insert_into_products(conn)
    #extract_p_CS(conn)
    #extract_p_MNR(conn)
    #extract_p_BST(conn)
    #set_p_is_first(conn)
    #extract_p_RFR(conn)
    #extract_is_J(conn)
    #extract_is_burst(conn)
    output_txt(conn)
    print str_process('I have eaten!. Nice to meet you.')