from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pymysql
import sys
import time
from apscheduler.schedulers.background import BackgroundScheduler

def insert_realtime ():
    url = 'http://datalab.naver.com/keyword/realtimeList.naver?where=main'

    hdr = {
        'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; ko; rv:1.9.2.8)'
                      ' Gecko/20100722 Firefox/3.6.8 IPMS/A640400A-14D460801A1-000000426571',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3', 'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8', 'Connection': 'keep-alive'}

    req = urllib.request.Request(url, headers=hdr)

    source = urllib.request.urlopen(req)

    if source is not None:
        conn = pymysql.connect(host=sys.argv[1], user=sys.argv[2], password=sys.argv[3], database=sys.argv[4], charset='utf8')

        conn.autocommit(True)
        cur = conn.cursor()

        soup = BeautifulSoup(source, 'lxml')

        link = soup.findAll('ul', attrs={'class': 'rank_list'})[0]
        # print(link)

        sql = 'DELETE FROM realtime'
        cur.execute(sql)

        for list_area in link.findAll('a', attrs={'class': 'list_area'}):
            rank = list_area.find('em').text
            rank = int(rank)
            title = list_area.find('span', attrs={'class': 'title'}).text

            try:
                sql = 'INSERT INTO realtime VALUES(null, "%s", "%d")'\
                      %(title, rank)
                # print(sql)
                cur.execute(sql)

            except Exception as err:
                print('[Naver]Main Error! ' + str(err))
            #     return

        # 크롤러가 아닌척 하기.
        time.sleep(1)


# try:
#     insert_realtime()
# except Exception as err:
#     err_message = str(err)

scheduler = BackgroundScheduler()
scheduler.add_job(func=insert_realtime, trigger='interval', minutes=1)
scheduler.start()

while (True):
    time.sleep(10000)
