from selenium import webdriver
import time
from datetime import datetime, timedelta
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import os
import glob
import re
import os
import chromedriver_autoinstaller as AutoChrome
import shutil
import pandas as pd
from multiprocessing import Process
from multiprocessing import Manager
#from pathos.multiprocessing import ProcessingPool as Pool
import cls_per_platform

print("라이브러리 임포트 완료".rjust(20, "="))

# 나중엔 그냥 json으로 관리되도록 할 것

def chromedriver_update():
    chrome_ver      = AutoChrome.get_chrome_version().split('.')[0]
    # print(f'현재 최신 버전은 {chrome_ver}입니다.')
    current_list    = os.listdir(os.getcwd())
    # print(f'전체 객체 확인 : {current_list}')
    chrome_list = []
    for i in current_list:
        path = os.path.join(os.getcwd(), i)
        # print(f'객체 경로 설정 : {path}')
        if os.path.isdir(path):
            # print(f'[폴더확인]')
            if 'chromedriver.exe' in os.listdir(path):
                # print(f'[크롬드라이버확인]')
                chrome_list.append(i)
    # print(f'크롬드라이버가 들어있는 폴더명 : {chrome_list} / 최신버전인 {chrome_ver} 제외' )
    old_version = list(set(chrome_list)-set([chrome_ver]))
    # print(f'구버전이 포함된 폴더명 : {old_version}')

    for i in old_version:
        path = os.path.join(os.getcwd(),i)
        print('구버전이 포함된 폴더의 전체 경로: {path} 삭제 진행' )
        shutil.rmtree(path)

    if not chrome_ver in current_list:
        print("최신 버전 크롬드라이버가 없습니다.")
        print("크롬드라이버 다운로드 실행")
        AutoChrome.install(True)
        print("크롬드라이버 다운로드 완료")
    else : print("크롬드라이버 버전이 최신입니다.")
    return os.path.join(os.getcwd(), chrome_ver, 'chromedriver.exe')


### 목표 : 소설 목록, 댓글, 댓글을 어디까지 봤는지 표시###
### 크게 세 모듈. ## 읽은 결과를 바탕으로 로그를 별도로 분석하는 메인모듈 ## 장르별로 읽어들이는 모듈
## 대시보드에 쓰이는 데이터 정리하고 출력하는 모듈

if __name__=='__main__':
    chrome_driver_path = chromedriver_update()  # 크롬 최신화
    view_date = datetime.now().strftime('%Y-%m-%d')  # %H-%M-%S')
    option_json = {
        'chrome_driver_path' : chrome_driver_path,
        'site_list':[  # site_list[n].format(genre=genre[m]) 같은 방식으로 입력
            'https://pagestage.kakao.com/{genre}/novels?sortType=NEWEST',
        ],
        'site_name_list' : [
            '카카오스테이지'
        ],
        # DB는 크게 2개가 될것. 하나는 플랫폼과 작품 목록(novels 뒤에 소설 번호로 식별되는 체계)에
        # 그 작품에서 어느 댓글까지 읽었는지 추적하는 별도 컬럼(댓글 삭제까지 감안)
        # 다른 하나는 댓글 목록

    }  # 저장은 일단 csv로. 추후 더 확장될 여지를 감안해 구글 빅쿼리에 업로드 고려
    site_class_list = [## 분산처리 절차 : 카카오 스테이지에서 더 확장하긴 어려울것 같지만 그래도 process spawning 사용
        cls_per_platform.kakaostage()
        # kakaopage # 작품이 너무 많아 실시간 변동을 알기 어려움.
        # naver series # 작품이 너무 많아 실시간 변동을 확인하는 것은 포기. 72,036개 작품
        # https://series.naver.com/novel/categoryProductList.series?categoryTypeCode=all
        # 그외 커뮤니티 : 추가적으로 주제 탐색 기능이 필요함. 가급적 후순위.
    ]

    try:
        comment_seen = pd.read_csv('comment_seen.csv')
    except Exception as e:
        print(e)  # 개인 크롤링 데이터 없으면 그냥 넘어가고  새로 만듬
        comment_seen = pd.DataFrame(columns=option_json['column_name'] )
    option_json['comment_seen'] = comment_seen
    process_list = []
    return_dict = Manager().dict()
    for i in range(len(site_class_list)):
        p = Process(target=site_class_list[i].scraping, args=(i, option_json, return_dict)) #{"query": query}))
        p.start()
        process_list.append(p)
    #for process in process_list:

    # print(return_dict.values()) # 새로 읽어들인 것들

    for process in process_list:
        process.join()
    print('Done', flush=True)
    # for x, y in return_dict.items():
    for val in return_dict.values():  # 사이트를 나타내는 key는 사실 큰 쓸모 없음.
        newly_saw_contest = pd.DataFrame(val)
        # newly_saw_contest['view_date'] = view_date
        # newly_saw_contest['status'] = 0  # 이건 내가 확인 후 파이참에서 일일이 봤는지 어떤지 적어야한다
        contest_seen = pd.concat([contest_seen, newly_saw_contest]).drop_duplicates().reset_index(drop=True)

    # print(contest_seen)

    contest_seen.to_csv("contest_seen.csv", index=False)
