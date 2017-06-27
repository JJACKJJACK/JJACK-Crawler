from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pymysql
import sys
import time

'''
 # AUTH: Moon 
 # DATE: 17.05.01
 # DESC: 입력받은 url을 열어 긁어온 html 코드에서 기사의 제목, 기사 원본 url, 내용의 일부, 이미지 url, 작성 날짜, 기자 정보를 얻어오는 함수.
 # PARAM: 1. url: 조선일보 최신 기사 페이지 url
          2. i: 최신 기사 페이지의 번호. 1이 가장 최신 기사를 나타낸다.
'''
def insert_article (url, i):
    # print(url + str(i))

    # 헤더 정의
    hdr = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36'
                      ' (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'none',
        'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.6,en;q=0.4'
    }

    category_file = open('./log/chosunCategoryLog', 'a')

    # Request 객체 생성
    req = urllib.request.Request(url + str(i), headers=hdr)

    # Request 객체를 이용하여 HTTP 응답 객체를 얻어옴.
    source = urllib.request.urlopen(req)

    # 정상적으로 객체를 얻어왔는지 확인.
    if source is not None:
        conn = pymysql.connect(host=sys.argv[1], user=sys.argv[2], password=sys.argv[3], database=sys.argv[4], charset='utf8')
        # 순서대로 호스트(localhost), mysql user(admin 또는 root), mysql password(rkawk123 또는 null), mysql database(jjack)
        # System arguments로 mysql connection을 구하기 위한 값들을 추가한다.
        conn.autocommit(True)
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
            # 문장 앞 뒤 공백 제거
            title = title.strip()
            # print(title)

            # 기사 url
            article_url = title_and_url_dump.find('a').get('href')
            # print(article_url)

            # 이미지 url
            image_dump = link.find('dd', attrs={'class': 'thumb'})
            if image_dump is not None:
                image_url = image_dump.find('img').get('src')
            else:
                image_url = ''

            # print(image_url)

            # 내용.
            desc_dump = link.find('dd', attrs={'class': 'desc'})
            desc = desc_dump.find('a').text.strip()
            # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
            desc = desc.replace('\'', '\\\'').replace('\"', '\\\"')
            # print(desc)

            # 날짜, 기자 덤프
            date_and_reporter_dump = link.find('dd', attrs={'class': 'date_author'})
            reporter = date_and_reporter_dump.find('span', attrs={'class': 'author'})
            if reporter is not None:
                reporter = reporter.text.strip()
            else:
                reporter = ''
            # print(reporter)

            # 날짜.
            date = date_and_reporter_dump.find('span', attrs={'class': 'date'}).text
            # 요일 삭제.
            date = date[0:10]
            # print(date)

            # 카테고리.
            category = get_category_by_new_link(article_url)

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
                sql = 'INSERT INTO article VALUES(null, "%s", "%s", "%s", "%s", "%s", "%s", 0, "%s", "%d")' \
                      %(title, desc, article_url, reporter,'조선일보', image_url, date, category_id)
                # print(sql)
                cur.execute(sql)

            except Exception as err:
                print('[Chosun]Main Error!' + str(err))
                # return

            # 크롤러 아닌척 하기.
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
            for link in soup.findAll('script'):
                str_link = str(link)
                index = str_link.find('C1_rp')
                if index is not -1:
                    for i in range(5, 100):
                        if str_link[index+i:index+i+1] is not str('\r'):
                            category = category + str_link[index+i:index+i+1]
                        else:
                            break

                    category = category.replace(';', '')
                    category = category.replace('=', '')
                    category = category.replace('"', '')
                    category = category.replace(' ', '')

        except Exception as err:
            print('[Chosun]Category Error! ' + str(err))
            return category

    return category


# Exception Log를 기록할 파일 열기.
# f = open('./log/chosunLog', 'a')

# 조선일보 기사 페이지 url
# 마지막 쿼리에서 pn의 값이 페이지 번호가 된다. Ex) pn=5  ->  5페이지.
chosun_url = 'http://news.chosun.com/svc/list_in/list.html?source=1&pn='

# 톄스트를 위해 최신 기사 1페이지부터 6페이지까지 수집.
for i in range(1, 99999999):
    try:
        insert_article(chosun_url, i)
    except Exception as err:
        # 만약 Exception이 발생할 경우 파일에 쓰기.
        err_message = str(err)
        print(err_message)
        # f.write(err_message)

# 파일 닫기.
# f.close()

