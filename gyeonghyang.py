from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pymysql
import sys
import time

'''
 # AUTH: Moon 
 # DATE: 17.05.02
 # DESC: 입력받은 url을 열어 긁어온 html 코드에서 기사의 제목, 기사 원본 url, 내용의 일부, 이미지 url, 작성 날짜, 기자 정보를 얻어오는 함수.
 # PARAM: 1. url: 경향신문 최신 기사 페이지 url
          2. i: 최신 기사 페이지의 번호. 1이 가장 최신 기사를 나타낸다.
'''
def insert_article (url, i):
    # print(url + str(i))

    category_file = open('./log/gyeonghyangCategoryLog', 'a')

    # 헤더 정의
    hdr = {
        'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; ko; rv:1.9.2.8)'
                      ' Gecko/20100722 Firefox/3.6.8 IPMS/A640400A-14D460801A1-000000426571',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3', 'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8', 'Connection': 'keep-alive'}

    # Request 객체 생성
    req = urllib.request.Request(url + str(i), headers=hdr)

    # Request 객체를 이용하여 HTTP 응답 객체를 얻어옴.
    source = urllib.request.urlopen(req)

    # 정상적으로 객체를 얻어왔는지 확인.
    if source is not None:
        # System arguments로 mysql connection을 구하기 위한 값들을 추가한다.
        # 순서대로 호스트(localhost), mysql user(admin 또는 root), mysql password(rkawk123 또는 null), mysql database(jjack)
        conn = pymysql.connect(host=sys.argv[1], user=sys.argv[2], password=sys.argv[3], database=sys.argv[4], charset='utf8')
        conn.autocommit(True)
        cur = conn.cursor()

        # 경향신문 기사 리스트 특성 상 페이지 당 기사를 10개씩만 가져와야 하는데 그것을 위한 변수.
        count = 0

        # html 소스코드를 얻기 위해 HTTP 응답 객체를 이용.
        soup = BeautifulSoup(source, 'lxml')

        for link in soup.find('div', attrs={'class': 'news_list'}).findAll('li'):

            if count == 10:
                break

            # 제목, 원본 url 덤프.
            title_and_url_dump = link.find('strong', attrs={'class': 'hd_title'})
            # 제목.
            title = title_and_url_dump.find('a').text
            # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
            title = title.replace('\'', '\\\'').replace('\"', '\\\"')
            # 문장 앞 뒤 공백 제거
            title = title.strip()
            # print(title)

            # 기사 url
            article_url = title_and_url_dump.find('a').get('href')
            # print(article_url)

            # 이미지 url
            image_dump = link.find('span', attrs={'class': 'thumb'})
            if image_dump is not None:
                image_url = image_dump.find('img').get('src')
            else:
                image_url = ''
            # print(image_url)

            # 내용.
            desc_dump = link.find('span', attrs={'class': 'lead'})
            desc = desc_dump.text.strip()
            # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
            desc = desc.replace('\'', '\\\'').replace('\"', '\\\"')
            # 개행 문자 제거.
            desc = desc.replace('\r\n', '')
            # print(desc)

            # 날짜, 기자 덤프.
            date_and_author_dump = link.find('span', attrs={'class': 'byline'})
            # 날짜.
            date = date_and_author_dump.find('em', attrs={'class': 'letter'}).text
            # 불필요한 공백 제거.
            date = date.replace('. ', '.')
            # print(date)

            # 기자.
            # 경향신문 사이트는 기자 이름이 있는 태그의 클래스명이 없기 때문에 None
            reporter = date_and_author_dump.find('em', attrs={'class': None})
            if reporter is not None:
                reporter = reporter.text.strip()
            else:
                reporter = ''
            # print(reporter)

            # 카테고리.
            category = get_category_by_new_link(article_url)
            # print(category)

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
                    # 일치하는 카테고리가 있다면.
                    if category in row[1] or row[1] in category:
                        category_id = row[0]
                        main_category_flag = False

            # 1차 필터링을 거쳤다면.
            if main_category_flag:
                # 카테고리 2차 필터링.
                for row in rows:
                    if row[2] is not None:
                        # 일치하는 카테고리가 있다면.
                        if category in row[2] or row[2] in category:
                            sub_category_flag = False
                            category = row[1]
                            category_id = row[0]

                # 2차 필터링을 거쳤다면.
                if sub_category_flag:
                    for row in rows:
                        if '기타' in row[1]:
                            etc_id = row[0]
                    # print('기타: ' + category)
                    category = category + '\n'
                    category_file.write(category)
                    category_id = etc_id
                    # category = '기타'

            # print(category)
            # print(category_id)
            # print()

            try:
                sql = 'INSERT INTO article VALUES(null, "%s", "%s", "%s", "%s", "%s", "%s", 0, "%s", "%d")'\
                      %(title, desc, article_url, reporter, '경향신문', image_url, date, category_id)
                # print(sql)
                cur.execute(sql)

            except Exception as err:
                print('[GH] Main Error! ' + str(err))
                # return

            # 기사를 가져왔으니 변수 + 1
            count = count + 1

            # 크롤러가 아닌척 하기.
            time.sleep(1)

    category_file.close()

'''
 # AUTH: Moon 
 # DATE: 17.05.23
 # DESC: 입력받은 url을 이용하여 기사 원본을 열어 카테고리를 받아오는 함수.
 # PARAM: 1. url: 기사 원본 url
 # RETURN: 1. category: 카테고리 또는 공백.
'''
def get_category_by_new_link(url):
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

            # 카테고리
            category_dump = soup.find('div', attrs={'class': 'sec_title'})
            if category_dump is not None:
                category = category_dump.find('a').text

        except Exception as err:
            print('[GH]Category Error! ' + str(err))
            return category

    return category


# Exception Log를 기록할 파일 열기.
# f = open('./log/gyeonghyangLog', 'a')

# 경향신문 기사 페이지 url
# 쿼리에서 page의 값이 페이지 번호가 된다. Ex) page=5  ->  5페이지.
gyeonghyang_url = 'http://news.khan.co.kr/kh_recent/index.html?&page='

# 최신 기사 1페이지부터 n페이지까지 수집.
for i in range(1, 99999999):
    try:
        insert_article(gyeonghyang_url, i)
    except Exception as err:
        # 만약 Exception이 발생할 경우 파일에 쓰기.
        err_message = str(err)
        # f.write(err_message)

# 파일 닫기.
# f.close()
