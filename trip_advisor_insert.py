import MySQLdb
import traceback
import json

def read_file(file_name):
    f = open(file_name)
    lines = f.readlines()
    return lines

if __name__=='__main__':
    f_name = '/misc/projdata4/info_fil/bshi/Data/review/tripadvisor_jiweili/review.txt'
    f_lines = read_file(f_name)
    for line in f_lines:
        json_data = json.loads(line)
        rating = json_data['ratings']['overall']
        title = json_data['title']
        body = json_data['text']
        author = json_data['author']
        author_id = author['id']
        help_ful_votes = json_data['num_helpful_votes']
        date_time = json_data['date']
        review_id = json_data['id']
        print review_id, rating, author_id, help_ful_votes, date_time
