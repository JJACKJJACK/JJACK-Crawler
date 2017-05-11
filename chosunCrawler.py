from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pymysql
import sys
import re
import time
from apscheduler.schedulers.background import BackgroundScheduler

'''
 # AUTH: Moon 
 # DATE: 17.05.01
 # DESC: 입력받은 url을 열어 긁어온 html 코드에서 기사의 제목, 기사 원본 url, 내용의 일부, 이미지 url, 작성 날짜, 기자 정보를 얻어오는 함수.
 # PARAM: 1. url: 조선일보 최신 기사 페이지 url
          2. i: 최신 기사 페이지의 번호. 1이 가장 최신 기사를 나타낸다.
'''
def insert_article (url, i):
    # print(url + str(i))
    # 최신 기사 페이지와 페이지 번호를 이용하여 HTTP 응답 객체를 얻어옴.
    source = urllib.request.urlopen(url + str(i))

    # 정상적으로 객체를 얻어왔는지 확인.
    if source is not None:
        try:
            # System arguments로 mysql connection을 구하기 위한 값들을 추가한다.
            # 순서대로 호스트(localhost), mysql user(admin 또는 root), mysql password(rkawk123 또는 null), mysql database(jjack)
            conn = pymysql.connect(host=sys.argv[1], user=sys.argv[2], password=sys.argv[3], database=sys.argv[4], charset='utf8')
            cur = conn.cursor()

            # html 소스코드를 얻기 위해 HTTP 응답 객체를 이용.
            soup = BeautifulSoup(source, 'lxml')

            for link in soup.findAll('dl', attrs={'class': 'list_item'}):

                # 제목, 기사 url 덤프.
                title_and_url_dump = link.find('dt')
                # 제목.
                title = title_and_url_dump.find('a').text
                # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
                title = title.replace('\'', '\\\'').replace('\"', '\\\"')
                #print(title)

                # 기사 url
                article_url = title_and_url_dump.find('a').get('href')
                # print(article_url)

                # 이미지 url
                image_dump = link.find('dd', attrs={'class': 'thumb'})
                if image_dump is not None:
                    image_url = image_dump.find('img').get('src')
                else:
                    image_url = None

                # print(image_url)

                # 내용.
                desc_dump = link.find('dd', attrs={'class': 'desc'})
                desc = desc_dump.find('a').text.strip()
                # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
                desc = desc.replace('\'', '\\\'').replace('\"', '\\\"')
                # print(desc)

                # 날짜.
                date = get_date_by_new_link(article_url)
                # print(date)


                # 기자.
                author_dump = link.find('dd', attrs={'class': 'date_author'})
                reporter = author_dump.find('span', attrs={'class': 'author'})
                if reporter is not None:
                    reporter = reporter.text.strip()
                else:
                    reporter = None
                # print(reporter)

                sql = 'INSERT INTO article VALUES(null, "%s", "%s", "%s", "%s", "%s", "%s", 0, "%s")' %(title, desc, article_url, reporter,'조선일보', image_url, date)
                # print(sql)

                cur.execute(sql)
                cur.connection.commit()

        except Exception as err:
            print('Main Error!' + str(err))
            # return

'''
 # AUTH: Moon 
 # DATE: 17.05.09
 # DESC: 입력받은 url을 이용하여 기사 원본을 열어 기사의 작성 시간을 받아옴.
 # PARAM: 1. url: 기사 원본 url
 # RETURN: 기사 작성 시간.
'''
def get_date_by_new_link (url):
    source = urllib.request.urlopen(url)

    date = ""

    if source is not None:
        try:
            soup = BeautifulSoup(source, 'lxml')

            date_dump = soup.find('div', attrs={'class': 'date_ctrl_2011'})
            date = date_dump.find('p').text

            # 공백 등 불필요한 문자 제거.
            date = date.strip().replace('\r\n', '')

            # 날짜 패턴을 주고 그 패턴에 맞는 문자열을 가져옴.
            # 패턴 선언.
            pattern = re.compile('[0-9]{4}.[0-9]{2}.[0-9]{2}\s[0-9]{2}.[0-9]{2}')
            # date 변수에 입력된 문자열 중 패턴에 맞는 문자열이 있는지 확인.
            searcher = pattern.search(date)
            date = searcher.group()

            return date

        except Exception as err:
            print('Reporter Error! ' + str(err))
            return ""

    return date


def interval_func ():
    # 조선일보 기사 페이지 url
    # 마지막 쿼리에서 pn의 값이 페이지 번호가 된다. Ex) pn=5  ->  5페이지.
    chosun_url = 'http://news.chosun.com/svc/list_in/list.html?source=1&pn='

    # 톄스트를 위해 최신 기사 1페이지부터 6페이지까지 수집.
    for i in range(1, 2):
        insert_article(chosun_url, i)

# 주기적으로 코드를 실행시키기 위한 Scheduler 객체를 얻어옴.
scheduler = BackgroundScheduler()
# scheduler에 작업을 추가함. 주기는 3시간이다.
scheduler.add_job(interval_func, 'interval', hours=10)
# scheduler 시작.
scheduler.start()

# 프로세스가 종료되면 scheduler도 소멸되므로 프로세스가 계속 동작할 수 있도록 하는 무한루프.
while (True):
    time.sleep(10000000)
