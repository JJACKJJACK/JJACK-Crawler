from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pymysql
import sys
import re

'''
 # AUTH: Moon 
 # DATE: 17.05.01
 # DESC: 입력받은 url을 열어 긁어온 html 코드에서 기사의 제목, 기사 원본 url, 내용의 일부, 이미지 url, 작성 날짜, 기자 정보를 얻어오는 함수.
 # PARAM: 1. url: 한겨레 최신 기사 페이지 url
          2. i: 최신 기사 페이지의 번호. 1이 가장 최신 기사를 나타낸다.
'''
def insert_article (url, i):
    # 한겨레는 url 중간에 페이지 번호가 있기 때문에 replace 함수를 이용하여 페이지 번호를 적용.
    # replace를 쉽게 적용하기 위해 url 페이지 번호 입력 부분에 '!@#' 문자열을 추가해 놓은 상태.
    url = url.replace('!@#', str(i))
    # print(url)

    # 최신 기사 페이지와 페이지 번호를 이용하여 HTTP 응답 객체를 얻어옴.
    source = urllib.request.urlopen(url)

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
        section_list = soup.find('div', attrs={'class': 'section-list-area'})

        for link in section_list.findAll('div', attrs={'class': 'list'}):

            # 기사 정보 덤프.
            article_info_dump = link.find('div', attrs={'class': 'article-area'})
            # 제목.
            title = article_info_dump.find('h4', attrs={'class': 'article-title'}).text
            # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
            title = title.replace('\'', '\\\'').replace('\"', '\\\"')
            # 문장 끝에 있는 개행 문자를 제거.
            title = title.replace('\n', '')
            # 공백 제거
            title = title.strip()
            # print(title)

            # 기사 url
            article_url = article_info_dump.find('h4', attrs={'class': 'article-title'}).find('a').get('href')
            # 한겨레는 기사 원본의 전체가 아닌 일부 경로만을 가지고 있으므로 앞부분을 채워준다.
            article_url = 'http://www.hani.co.kr/arti' + article_url
            # print(article_url)

            # 내용.
            desc_dump = article_info_dump.find('p', attrs={'class': 'article-prologue'})
            desc = desc_dump.find('a').text
            # sql 쿼리가 정상적으로 실행될 수 있도록 ' 문자와 " 문자를 이스케이프 시킨다.
            desc = desc.replace('\'', '\\\'').replace('\"', '\\\"')
            # 문장 끝에 있는 개행 문자를 제거.
            desc = desc.replace('\r\n', '')
            # 문장의 앞, 뒤에 있는 공백을 제거.
            desc = desc.strip()
            # 문장 중간에 공백이 여러 개 있을 경우 공백을 하나로 바꿔준다.
            pattern = re.compile(r'\s\s+')
            desc = re.sub(pattern, ' ', desc)
            # print(desc)

            # 날짜.
            date_dump = article_info_dump.find('p', attrs={'class': 'article-prologue'})
            date = date_dump.find('span', attrs={'class': 'date'}).text
            # print(date)


            # 이미지 url
            image_dump = article_info_dump.find('span', attrs={'class': 'article-photo'}).find('a')
            if image_dump is not None:
                image_url = image_dump.find('img').get('src')
            else:
                image_url = None
            # print(image_url)

            # TODO: 기자명이 출력되는 태그가 불규칙적이라 얻어오지 못했다. 추후에 수정.
            # 기자.
            # 한겨레의 경우 최신 기사 리스트에 기자의 이름을 따로 보여주지 않고 있기 때문에
            # 기사의 원본 url으로 기사 원본을 열어 기자의 이름을 가져온다.
            # reporter = get_reporter_by_new_link(article_url)
            reporter = ''
            # print(reporter)

            try:
                sql = 'INSERT INTO article VALUES(null, "%s", "%s", "%s", "%s", "%s", "%s", 0, "%s")' %(title, desc, article_url, reporter,'한겨레', image_url, date)
                # print(sql)
                cur.execute(sql)

            except Exception as err:
                print('Main Error! ' + str(err.args[1]))
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


# 한겨레 기사 페이지 url
# p의 값이 페이지 번호가 된다. Ex) p=5  ->  5페이지.
# p의 값을 쉽게 넣기 위해 문자열 '!@#' 을 넣어둔 상태.
han_url = 'http://www.hani.co.kr/arti/list!@#.html'

# 한겨레 최신 기사 1페이지부터 n페이지까지 반복.
for i in range(1, 3):
    insert_article(han_url, i)
