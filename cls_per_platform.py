import os

from selenium import webdriver
import time
from datetime import datetime, timedelta
import re
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
import multiprocessing as mp


def dict_append_as_list(dict1, dict2): # key별로 list가 할당되어 있고 이걸 기준으로 dataframe만드는데 연결을 위함.
    for key, value in dict2.items():
        if key in dict1:
            dict1[key].extend(value)
        else:
            dict1[key] = value
    return dict1


class site:
    """Super Class"""
    def __init__(self):
        self.driver = None
        # 외부로 보낼 결과물
        self.scrape_dict = {}
        # 멀티프로세싱 단위(개인적으로는 가장 먼저 코어 수 만큼 갈라져나가는 정도가 적절).
        # spawning은 우선순위가 낮으니 일단 for loop 돌아가게 한다.
        # self.multi = []  # 카카오스테이지의 경우 장르가 적절
        # 각 페이지 별로 원하는 정보는 다름
        self.col_name_list = []

        # 에피소드가 페이지 형태 제공 될 경우 사용(댓글이 페이지 형태로 제공되는 경우도 일단은 재활용 고려).
        self.page_list = None
        self.page_list_selector = None
        self.current_page_num = 0
        self.current_page_num_selector = None
        self.max_page_num = 0  # 당장 브라우저에서 보이는 것 중에서 가장 큰 페이지 값을 의미. 정말 최대는 아님.
        self.max_page_num_selector = None
        self.next_btn = None
        self.next_btn_selector= '' # 페이지형태가 아니라도 쓰임

        self.going_to_page_num = 0 # current_page_num가 늘어났는데 실제 페이지는 늘지 않는지 확인하는 용도.

        self.title_list = None  # 작품 그 자체. 조회수, 에피소드의 수 등을 체크하는 기준.
        self.title_list_selector = ''
        # 각 작품의 조회수가 변치 않으면 그건 댓글이 늘지 않은 것과 동일하다고 간주하고 해당 리스트를 다음 프로세스로 넘김.

    def open_browser(self, i_th, option_json):
        self.driver = webdriver.Chrome(option_json['chrome_driver_path'])
        self.driver.get(option_json['contest_site_list'][i_th]) # 실제론 여길 손볼것

    def search_query(self, q, i_th, option_json):  # 사실상 오버라이딩용
        self.open_browser(i_th, option_json) # 만약에 그냥 사이트 주소에 쿼리 칠 수 있으면 문제없지만,
        # 쿼리 못치면 open_browser 함수를 거치던가 하는식이 되는게 무난.
        # print(q)

    def find_pagelist(self):  #오버라이딩용 # 그러나 최대한 xpath나 selector로 찾는건 공통될 것
        try:
            self.page_list = self.driver.find_element(by=By.XPATH, value=self.page_list_selector)
            self.current_page_num = int(self.page_list.find_elements(by=By.CLASS_NAME, value=self.current_page_num_selector).text)
            self.max_page_num = int(self.page_list.find_elements(by=By.CLASS_NAME, value=self.max_page_num_selector).text)
            self.title_list = self.driver.find_element(by=By.XPATH, value=self.title_list_selector) \
                .find_elements(by=By.TAG_NAME, value='a')
            self.next_btn = self.page_list.find_element(by=By.CLASS_NAME, value=self.next_btn_selector)
        except StaleElementReferenceException as e :
            print(f"PID : {os.getpid()}\n", e)
            return False

    def scanning_content(self, cont):  #오버라이딩용 # 사이트마다, 작품 목록, 에피소드, 댓글, 여부에 따라 갈림
        pass

    def href_li_get(self, col_name_list, col_selector_list): # 이것도 조금씩 차이 날 것
        href_dict = {
            col_name : [] for col_name in self.col_name_list
        }
        # break_check = False
        for contest in self.content_list:
            title, link, due_date = self.find_pagelist(contest)
            if not ((content_seen.loc[content_seen[column_name[1]]] == title) &
                    # (contest_seen.loc[contest_seen[column_name[2]]] == link) &
                    (content_seen.loc[content_seen[column_name[3]]] == due_date)).empty:
                continue  # 본건 넘김.
            # 처음 보는건 dict의 각 항목마다 list 형태로 한다.
            href_dict[column_name[0]].append(q)
            href_dict[column_name[1]].append(title)
            href_dict[column_name[2]].append(link)
            href_dict[column_name[3]].append(due_date)
        break_check = True

        return href_dict, break_check

    def goto_next_page(self):
        print("max_page_num ", self.max_page_num)
        # for p in page_list.find_elements_by_tag_name('a'):
        #     print(p.text)
        print('current_page_num ', self.current_page_num)
        if self.current_page_num != self.max_page_num:  # 마지막 페이지인지 확인
            self.current_page_num += 1
            #self.driver.execute_script("goPage(" + str(current_page_num) + ");")  # 자바스크립트 함수 호출하는 방식 사용
            time.sleep(3)
            return True
        else:  # 마지막 페이지라면
            self.next_btn.click()

            self.going_to_page_num= self.current_page_num + 1
            for wait_sec in range(60): # n초 정도 로딩 기다리기
                time.sleep(1)  # 기다리는 것 보다 더 좋은건 될 때까지 계속 확인.
                self.scanning_site()
                if self.going_to_page_num == self.current_page_num:  # 완전히 페이지 끝까지 갈 경우 에러 핸들링
                    return True
            return False

    def crawling(self, i_th, option_json, return_dict):  # 전반적 흐름일 뿐 중간에 프로세스 더 추가해도 괜찮다.
        # print(f"scrape start PID : {os.getppid()}")

        check_more = True

        for q in option_json['site_list']:
            self.search_query(q, i_th, option_json)
            check_more = self.scanning_site()  # 필요한 elem 위치 확인

            while check_more:
                href_df, break_check = self.href_li_get(q, option_json['column_name'], option_json['contest_seen'])  # 로드된 웹 요소들 긁어오기.
                tot_href_dict = dict_append_as_list(tot_href_dict, href_df)
                if break_check:
                    # print('break!')
                    break
                check_more = self.goto_next_page()  # 다음 게시물 번호로 이동
        return_dict[option_json['contest_site_name_list'][i_th]] = tot_href_dict
        if not i_th == len(option_json['contest_site_name_list']) - 1 : # 마지막 사이트는 테스트용이다. 드라이버 종료하지 말것.
            # self.driver.close()
            self.driver.quit()




class kakaostage(site):
    # 랭킹에 오르려면
    # - 베스트 지수 : 독자수, 공개한 작품 회차수, 회차 분량 등을 기반으로 생성한 순위 데이터
    # - 실시간 랭킹: 관심작품수, 독자수, 추천수를 집계하여 생성한 순위 데이터
    # - 독자 : 독자수를 기준으로 생성한 순위 데이터
    # - 관심작품 : 관심작품수 데이터를 집계하여 생성한 순위 데이터
    # - 연령별 : 독자수를 연령별로 나눠 집계한 순위 데이터

    # 신인작가 랭킹(실제로는 신규 작가 라고 표기)
    # 아래 조건을 모두 만족한 작품에 한해 신인작가 랭킹에 노출됩니다.
    # 최근 60일 안에 생성한 닉네임(닉네임은 한 ID에 여럿 생성 가능), 최근 30일 안에 최초 1회를 등록한 작품

    def __init__(self, *args):
        super(site, self).__init__()
    """
         'novel_column_name': [
            'stage_serial_num', 'pen_name', 'genre', 'title', 'introduce', 'age_limit',
            'bookmark_cnt', # 관심작품 수. 일자별 변동사항을 아예 별도로 빼서 로그로 만들것. # 로그 기록
            'scrape_date', # date라 적었으나 일단 시간 형태로 저장
            'contest_apply'
        ],
        'episode_column_name': [
            'episode_title', 'episode_serial_num',  # article 모두 감싸는 a 태그에 있음
            'view_cnt', 'recommend_cnt', 'comment_cnt', # 로그 기록
            'upload_date'
        ],
        'comment_column_name':  # 처음에 볼 때 최신순으로 볼것
        ['stage_serial_num', 'nickname', 'scrape_date', 'written_date', "comment_txt",
         'episode',  # 몇 편인지 알 수 없는 경우는 None(나타나는 경우가 제각각이라 규칙을 알 수 없음)
         'likes_cnt',  # 이것도 시간에 따라 조금씩 다른데 일단 댓글 스크랩은 한번만 하므로 변하지 않는 것으로 가정.
         'writer_TF', # 작가가 작성한 댓글. 일단은 스크랩핑하나 분석에선 제외.
         'stagestepler_costactivity' # '#스테이지스테플러_대가성활동'이란 문자열이 포함되면 True
         ]
        ,
        'ranking' :
        [
            'detail_genre', 'ranking_category', 'ranking_category_detail', 'stage_serial_num', 'rank',
            'scrape_date'  # date라 적었으나 일단 시간 형태로 저장
        ]
    """

    def search_query(self, q, i_th, option_json):  # 오버라이딩용
        # self.open_browser(i_th, option_json)
        # self.driver.find_element_by_name('sw').send_keys(q)
        # self.driver.find_element(by=By.XPATH, value='//*[@id="container"]/div[2]/div[1]/div[2]/form/div/div/span[3]/input').click()
        # 전달이 잘 안된다. 귀찮으니 그냥 주소 바꾸는 걸로 우선 구현
        self.driver = webdriver.Chrome(option_json['chrome_driver_path'])
        self.driver.get(option_json['site_list'][i_th])

    def scanning_site(self):  # 그러나 최대한 xpath나 selector로 찾는건 공통될 것
        try:
            self.page_list = self.driver.find_element(by=By.XPATH, value=self.page_list_selector)
            self.current_page_num = int(self.page_list.find_element(by=By.CLASS_NAME, value=self.current_page_num_selector).text)
            self.max_page_num = int(self.page_list.find_elements(by=By.TAG_NAME, value=self.max_page_num_selector)[-2].text)
            self.next_btn = self.page_list.find_element(by=By.XPATH, value=self.next_btn_clssname)
            self.title_list_selector = self.driver.find_element(by=By.XPATH, value=self.).find_elements(by=By.TAG_NAME, value='li')
        except StaleElementReferenceException as e :
            print(e)
            return False

    def scanning_contest(self, col_name):
        # print(contest.find_element(by=By.CLASS_NAME, value=self.title_selector).text)
        title = contest.find_element(by=By.CLASS_NAME, value=self.title_selector).find_element(by=By.TAG_NAME, value='a').text
        link = contest.find_element(by=By.TAG_NAME, value=self.link_selector).get_attribute('href')
        d_day = int(re.findall(r'\d+', contest.find_element(by=By.CLASS_NAME, value='day').text)[0]) - 1

        due_date = datetime.now() + timedelta(days=d_day)
        due_date = due_date.strftime('%Y-%m-%d')
        return title, link, due_date

    def href_li_get(self, q, column_name, contest_seen): # 이것도 조금씩 차이 날 것
        href_dict = {
            column_name[0] : [], column_name[1] : [], column_name[2] : [], column_name[3] : []
        }
        for contest in self.contest_list[1:]:  # 첫 li는 테이블의 항목이라 데이터가 없음.

            title, link, due_date = self.scanning_contest(contest)
            if not ((contest_seen.loc[contest_seen[column_name[1]]] == title) &
                    # (contest_seen.loc[contest_seen[column_name[2]]] == link) &
                    (contest_seen.loc[contest_seen[column_name[3]]] == due_date)).empty:
                continue  # 본건 넘김.
            # 처음 보는건 dict의 각 항목마다 list 형태로 한다.
            href_dict[column_name[0]].append(q)
            href_dict[column_name[1]].append(title)
            href_dict[column_name[2]].append(link)
            href_dict[column_name[3]].append(due_date)
        break_check = True

        return href_dict, break_check
