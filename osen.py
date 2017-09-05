from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pymysql
import sys
import time
from datetime import date, timedelta

def insert_article (url, i, d):
    url = url.replace('!@#', str(i))
    url = url.replace('$%^', str(d))

    category_file = open('./log/osenCategoryLog', 'a')

    # 헤더 정의
    hdr = {
        'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; ko; rv:1.9.2.8)'
                      ' Gecko/20100722 Firefox/3.6.8 IPMS/A640400A-14D460801A1-000000426571',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3', 'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8', 'Connection': 'keep-alive'}

    # Request 객체 생성
    req = urllib.request.Request(url, headers=hdr)

    # Request 객체를 이용하여 HTTP 응답 객체를 얻어옴.
    source = urllib.request.urlopen(req)

    # 정상적으로 객체를 얻어왔는지 확인.
    if source is not None:
        # System arguments로 mysql connection을 구하기 위한 값들을 추가한다.
        # 순서대로 호스트(localhost), mysql user(admin 또는 root), mysql password(rkawk123 또는 null), mysql database(jjack)
        conn = pymysql.connect(host=sys.argv[1], user=sys.argv[2], password=sys.argv[3], database=sys.argv[4], charset='utf8')
        conn.autocommit(True)
        cur = conn.cursor()

        # html 소스코드를 얻기 위해 HTTP 응답 객체를 이용.
        soup = BeautifulSoup(source, 'lxml')

        tbody_soup = soup.find('tbody')
        tr_soup = tbody_soup.find('tr')

        if tr_soup is not None:
            for link in tbody_soup.findAll('tr'):
                # 제목 및 url 덤프
                title_and_url_dump = link.find('div', attrs={'class': 'searchObj'}).find('a')
                # 제목.
                title = title_and_url_dump.text
                # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
                title = title.replace('\'', '\\\'').replace('\"', '\\\"')
                # 문장 앞 뒤 공백 제거
                title = title.strip()
                # print(title)

                # 기사 url
                article_url = title_and_url_dump.get('href')
                article_url = 'http://osen.mt.co.kr' + article_url
                # print(article_url)

                # 내용.
                desc = link.find('p', attrs={'class': None}).text
                # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
                desc = desc.replace('\'', '\\\'').replace('\"', '\\\"')
                desc = desc.replace('\r\n', '')
                # print(desc)

                # 이미지 url
                image_url = link.find('img').get('src')
                # print(image_url)

                # 기자, 작성일.
                reporter_and_date_dump = link.find('p', attrs={'class': 'searchWriter'})
                reporter_and_date = reporter_and_date_dump.text
                reporter = reporter_and_date[7:14]
                # print(reporter)

                date = reporter_and_date[16:32]
                # print(date)

                category = get_category_by_new_link(article_url)
                # print('원래 카테고리 : ' + category)

                category_sql = 'SELECT DISTINCT * FROM category'
                cur.execute(category_sql)

                rows = cur.fetchall()

                main_category_flag = True
                sub_category_flag = True
                category_id = -1
                etc_id = -1

                # 카테고리 1차 필터링.
                for row in rows:
                    # 메인 카테고리를 기준으로.
                    if row[2] is None:
                        # 일치하는 카테고리가 있다면
                        if category in row[1] or row[1] in category:
                            category_id = row[0]
                            main_category_flag = False

                # 1차 필터링을 거쳤다면.
                if main_category_flag:
                    # 카테고리 2차 필터링.
                    for row in rows:
                        if row[2] is not None:
                            # 일치하는 카테고리가 있다면
                            if category in row[2] or row[2] in category:
                                sub_category_flag = False
                                category = row[1]
                                category_id = row[0]

                    # 2차 필터링을 거쳤다면.
                    if sub_category_flag:
                        for row in rows:
                            if '기타' in row[1]:
                                etc_id = row[0]
                        # print('기타:' + category)
                        category = category + '\n'
                        category_file.write(category)
                        category_id = etc_id
                        # category = '기타'

                # print(category)
                # print(category_id)

                try:
                    sql = 'INSERT INTO article VALUES(null, "%s", "%s", "%s", "%s", "%s", "%s", 0, "%s", "%d")'\
                          %(title, desc, article_url, reporter, '오센', image_url, date, category_id)
                    # print(sql)
                    cur.execute(sql)
                    sql = 'INSERT INTO article_index VALUES(null, "%s")' %(title)
                    cur.execute(sql)

                except Exception as err:
                    print('[osen]Main Error! ' + str(err))
                    # return

        else:
            category_file.close()
            # print('Empty : ' + url)
            return -1

        # 크롤러가 아닌척 하기.
        time.sleep(1)

    category_file.close()
    return 1

def get_category_by_new_link (url):
    # 헤더 정의
    hdr = {
        'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; ko; rv:1.9.2.8)'
                      ' Gecko/20100722 Firefox/3.6.8 IPMS/A640400A-14D460801A1-000000426571',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3', 'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8', 'Connection': 'keep-alive'}

    # Request 객체 생성
    req = urllib.request.Request(url, headers=hdr)

    # Request 객체를 이용하여 HTTP 응답 객체를 얻어옴.
    source = urllib.request.urlopen(req)

    category = ''

    if source is not None:
        try:
            soup = BeautifulSoup(source, 'lxml')

            category = soup.find('strong', attrs={'class': None}).text
            # print(category)

        except Exception as err:
            print('[osen]Reporter Error! ' + str(err))
            return category

    return category


# Exception Log를 기록할 파일 열기.
# f = open('./log/osenLog', 'a')

# 오센 기사 페이지 url
# offset의 값을 쉽게 넣기 위해 문자열 '!@#' 을 넣어둔 상태.
# date의 값을 쉽게 넣기 위해 문자열 '$%^' 을 넣어둔 상태.
osen_url = 'http://osen.mt.co.kr/all/?offset=!@#&date=$%^'

day = date.today().strftime('%Y%m%d')
sub_value = 1

for i in range(0, 99999999):
    # 오센 최신 기사 페이지의 번호는 0, 15, 30, 45, ... 과 같이 값이 한번에 15씩 증가한다.
    for j in range(0, 99999999, 15):
        try:
            isEmpty = insert_article(osen_url, j, day)
            if isEmpty == -1:
                day = (date.today() - timedelta(sub_value)).strftime('%Y%m%d')
                sub_value = sub_value + 1
                break

        except Exception as err:
            # 만약 Exception이 발생할 경우 파일에 쓰기.
            err_message = str(err)
            # f.write(err_message)

# 파일 닫기.
# f.close()


