from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pymysql
import sys
import re
import time

'''
 # AUTH: Moon 
 # DATE: 17.05.29
 # DESC: 입력받은 url을 열어 긁어온 html 코드에서 기사의 제목, 기사 원본 url, 내용의 일부, 이미지 url, 작성 날짜, 기자 정보를 얻어오는 함수.
 # PARAM: 1. url: 스포츠조선 최신 기사 페이지 url
          2. i: 최신 기사 페이지의 번호. 1이 가장 최신 기사를 나타낸다.
'''
def insert_article (url, i):
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

        # html 소스코드를 얻기 위해 HTTP 응답 객체를 이용.
        soup = BeautifulSoup(source, 'lxml')

        # 기사 리스트 영역만을 가져옴.
        section_list = soup.find('div', attrs={'class': 'contlist'})

        for link in section_list.findAll('li'):
            # 기사 제목 및 url 덤프.
            title_and_url_dump = link.find('dt', attrs={'class': None}).find('a')
            # 제목.
            title = title_and_url_dump.text
            # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
            title = title.replace('\'', '\\\'').replace('\"', '\\\"')
            # 문장 끝에 있는 개행 문자를 제거.
            title = title.replace('\n', '')
            # 공백 제거
            title = title.strip()
            # print(title)

            # 기사 url
            article_url = title_and_url_dump.get('href')
            # 스포츠조선는 기사 원본의 전체가 아닌 일부 경로만을 가지고 있으므로 앞부분을 채워준다.
            article_url = 'http://sports.chosun.com/' + article_url
            # print(article_url)

            # 내용 및 날짜 덤프
            desc_and_date_dump = link.find('dd', attrs={'class': None})
            desc = desc_and_date_dump.find('a').text
            # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
            desc = desc.replace('\'', '\\\'').replace('\"', '\\\"')
            # 문장의 앞, 뒤에 있는 공백을 제거.
            desc = desc.strip()
            # 문장에 개행이 여러 개 있을 경우 개행을 하나로 바꿔준다.
            pattern = re.compile(r'\n\n+')
            desc = re.sub(pattern, ' ', desc)
            # print(desc)

            # 날짜.
            date = desc_and_date_dump.find('span').text
            # print(date)


            # 이미지 url
            image_dump = link.find('dt', attrs={'class': 'photo'})
            if image_dump is not None:
                image_url = image_dump.find('a').find('img').get('src')
            else:
                image_url = ''
            # print(image_url)

            # TODO: 기자명이 출력되는 태그가 불규칙적이라 얻어오지 못했다. 추후에 수정.
            # 기자.
            # 스포츠조선의 경우 최신 기사 리스트에 기자의 이름을 따로 보여주지 않고 있기 때문에
            # 기사의 원본 url으로 기사 원본을 열어 기자의 이름을 가져온다.
            # reporter = get_reporter_by_new_link(article_url)
            reporter = ''
            # print(reporter)

            # 카테고리.
            category = get_category_by_new_link(article_url)
            # print(category)

            try:
                sql = 'INSERT INTO article VALUES(null, "%s", "%s", "%s", "%s", "%s", "%s", 0, "%s", "%s")' \
                      %(title, desc, article_url, reporter, '스포츠조선', image_url, date, category)
                # print(sql)
                cur.execute(sql)

            except Exception as err:
                print('[SportsChosun]Main Error! ' + str(err.args[1]))
                # return

            # 크롤러가 아닌척 하기.
            # time.sleep(1)


'''
 # AUTH: Moon 
 # DATE: 17.05.29
 # DESC: 입력받은 url을 이용하여 기사 원본을 열어 기사의 카테고리를 받아오는 함수.
 # PARAM: 1. url: 기사 원본 url
 # RETURN: 1. category: 기자의 이름 또는 공백.
'''
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

            # 카테고리.
            # 스포츠조선의 경우 '대분류 > 소분류' 로 카테고리를 표기하므로 소분류를 얻어오도록 한다.
            # [0]: 대분류, [1]: >, [2]: 소분류
            category_dump = soup.find('div', attrs={'class': 'pageN'}).find('ul').findAll('li')[2]
            category = category_dump.find('a').text

            return category

        except Exception as err:
            print('[SportsChosun]Reporter Error! ' + str(err))
            return category

    return category


# # Exception Log를 기록할 파일 열기.
# f = open('./log/sportsChosunLog', 'a')

# 스포츠조선 기사 페이지 url
# p의 값이 페이지 번호가 된다. Ex) p=5  ->  5페이지.
# p의 값을 쉽게 넣기 위해 문자열 '!@#' 을 넣어둔 상태.
sports_chosun_url = 'http://sports.chosun.com/latest/main.htm?page='

# 스포츠조선 최신 기사 1페이지부터 n페이지까지 반복.
for i in range(1, 3):
    try:
        insert_article(sports_chosun_url, i)
    except Exception as err:
        # 만약 Exception이 발생할 경우 파일에 쓰기.
        err_message = str(err)
        print('[SportsChosun] Error : ' + err_message)
        # f.write(err_message)

# 파일 닫기.
# f.close()
