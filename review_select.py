#-*- coding:utf-8 -*-
from random import randint

if __name__=='__main__':
    f = open('result.txt', 'r')
    line_list = f.readlines()
    f.close()
    result_dict = {}
    for i in xrange(0, 400):
        t = randint(0, len(line_list))
        result_dict[line_list[t]] = 1

    f2 = open('result_list.txt', 'wb')
    count = 0
    for key in result_dict:
        f2.write(str(key))
        count += int(key.split()[1])
    print count
    f2.close()
        