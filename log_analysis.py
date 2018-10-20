#!/usr/bin/env python3

import psycopg2
from datetime import datetime


def top_three_articles():
    conn=psycopg2.connect("dbname=news")
    cursor=conn.cursor()
    cursor.execute("select title,count(*) as sum from log,articles where log.path=CONCAT('/article/', articles.slug) group by title order by sum desc limit 3")
    result=cursor.fetchall()
    conn.close()
    print("Top 3 Articles-\n")
    i=1
    for data in result:
        print(str(i) + ") " + data[0] + " - " + str(data[1]) + " Views\n")
        i+=1
    print("-----------------")




def top_authors():
    conn=psycopg2.connect("dbname=news")
    cursor=conn.cursor()
    cursor.execute("select name,count(*) as views from authors,articles,log where authors.id=articles.author and log.path=CONCAT('/article/', articles.slug) group by name order by views desc")
    result=cursor.fetchall()
    conn.close()
    print("Top Authors-\n")
    i=1
    for data in result:
        print(str(i) + ") " + data[0] + " - " + str(data[1]) + " Views\n")
        i+=1
    print("-----------------")


def one_perc_error():
    conn=psycopg2.connect("dbname=news")
    cursor=conn.cursor()
    cursor.execute("select a.time::date as day,(count(*)*100/requests::decimal) as per from log as a, (select count(*) as requests,b.time::date as day2 from log as b group by day2 ) as total where a.status!='200 OK' and a.time::date=total.day2 group by day,requests order by per desc")
    result=cursor.fetchall()
    conn.close()
    print("Day errors were greater than 1%-\n")
    i=1
    for data in result:
        if data[1]>1:
            print(str(i) + ") " + str(data[0].strftime("%B %d, %Y")) + " - " + str(data[1]) + "%\n")
            i+=1
        break
    print("-----------------")

top_three_articles()
top_authors()
one_perc_error()
