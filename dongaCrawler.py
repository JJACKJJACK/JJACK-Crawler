from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pymysql
import sys
import time
from apscheduler.schedulers.background import BackgroundScheduler

'''
 # AUTH: Moon 
 # DATE: 17.05.01
 # DESC: 입력받은 url을 열어 긁어온 html 코드에서 기사의 제목, 기사 원본 url, 내용의 일부, 이미지 url, 작성 날짜, 기자 정보를 얻어오는 함수.
 # PARAM: 1. url: 동아일보 최신 기사 페이지 url
          2. i: 최신 기사 페이지의 번호. 1이 가장 최신 기사를 나타낸다.
'''
def insert_article (url, i):
    # 동아일보는 url 중간에 페이지 번호가 있기 때문에 replace 함수를 이용하여 페이지 번호를 적용.
    # replace를 쉽게 적용하기 위해 url 페이지 번호 입력 부분에 '!@#' 문자열을 추가해 놓은 상태.
    url = url.replace('!@#', str(i))
    # print(url)

    # 최신 기사 페이지와 페이지 번호를 이용하여 HTTP 응답 객체를 얻어옴.
    source = urllib.request.urlopen(url)

    # 정상적으로 객체를 얻어왔는지 확인.
    if source is not None:
        try:
            # System arguments로 mysql connection을 구하기 위한 값들을 추가한다.
            # 순서대로 호스트(localhost), mysql user(admin 또는 root), mysql password(rkawk123 또는 null), mysql database(jjack)
            conn = pymysql.connect(host=sys.argv[1], user=sys.argv[2], password=sys.argv[3], database=sys.argv[4], charset='utf8')
            cur = conn.cursor()

            # html 소스코드를 얻기 위해 HTTP 응답 객체를 이용.
            soup = BeautifulSoup(source, 'lxml')

            for link in soup.findAll('div', attrs={'class': 'articleList'}):

                # 기사 정보 덤프.
                article_info_dump = link.find('div', attrs={'class': 'rightList'}).find('a')
                # 제목.
                title = article_info_dump.find('span', attrs={'class': 'tit'}).text
                # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
                title = title.replace('\'', '\\\'').replace('\"', '\\\"')
                # print(title)

                # 기사 url
                article_url = article_info_dump.get('href')
                # print(article_url)

                # 내용.
                desc_dump = article_info_dump.find('span', attrs={'class': 'txt'})
                desc = desc_dump.text.strip()
                # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
                desc = desc.replace('\'', '\\\'').replace('\"', '\\\"')
                # print(desc)

                # 날짜.
                date = article_info_dump.find('span', attrs={'class': 'date'}).text
                # print(date)


                # 이미지 url
                image_dump = link.find('div', attrs={'class': 'thumb'})
                if image_dump is not None:
                    image_url = image_dump.find('img').get('src')
                else:
                    image_url = None
                # print(image_url)

                # 기자.
                # 동아일보의 경우 최신 기사 리스트에 기자의 이름을 따로 보여주지 않고 있기 때문에
                # 기사의 원본 url으로 기사 원본을 열어 기자의 이름을 가져온다.
                reporter = get_reporter_by_new_link(article_url)
                # print(reporter)

                sql = 'INSERT INTO article VALUES(null, "%s", "%s", "%s", "%s", "%s", "%s", 0, "%s")' %(title, desc, article_url, reporter,'동아일보', image_url, date)
                # print(sql)

                cur.execute(sql)
                cur.connection.commit()

        except Exception as err:
            print('Main Error! ' + str(err))
            # return


'''
 # AUTH: Moon 
 # DATE: 17.05.01
 # DESC: 입력받은 url을 이용하여 기사 원본을 열어 기자의 이름을 받아오는 함수.
 # PARAM: 1. url: 기사 원본 url
 # RETURN: 기자의 이름 또는 공백.
'''
def get_reporter_by_new_link (url):
    source = urllib.request.urlopen(url)

    reporter = ""

    if source is not None:
        try:
            soup = BeautifulSoup(source, 'lxml')

            # 한 기사에 기자가 여럿이거나 특파원이 포함된 경우가 있다.
            # 반복을 통해 reporter 변수에 모두 추가한다.
            for reporter_dump in soup.find('span', attrs={'class': 'report'}).findAll('a'):
                reporter = reporter + reporter_dump.text

            return reporter

        except Exception as err:
            print('Reporter Error! ' + str(err))
            return ""

    return reporter


def interval_func ():
    # 동아일보 기사 페이지 url
    # p의 값이 페이지 번호가 된다. Ex) p=5  ->  5페이지.
    # p의 값을 쉽게 넣기 위해 문자열 '!@#' 을 넣어둔 상태.
    donga_url = 'http://news.donga.com/List?p=!@#&prod=news&ymd=&m=NP'

    # 동아일보 최신 기사 페이지의 번호는 1, 21, 41, 61, ... 과 같이 값이 한번에 20씩 증가한다.
    for i in range(1, 9999999, 20):
        insert_article(donga_url, i)

# 주기적으로 코드를 실행시키기 위한 Scheduler 객체를 얻어옴.
scheduler = BackgroundScheduler()
# scheduler에 작업을 추가함. 주기는 3시간이다.
scheduler.add_job(interval_func, 'interval', hours=3)
# scheduler 시작.
scheduler.start()

# 프로세스가 종료되면 scheduler도 소멸되므로 프로세스가 계속 동작할 수 있도록 하는 무한루프.
while (True):
    time.sleep(10000000)
