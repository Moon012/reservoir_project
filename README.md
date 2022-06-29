[변경사항]
# 데이터베이스 
pisp , satellite 모두 water로 이동

# 테이블명
앞에 wws_ 붙임 (Water Supply Safety 의 약어)

# 컬럼명
copernicus_product_file 테이블
size --------------> file_size
path --------------> file_path
download_date -----> file_download_date

copernicus_product_info 테이블
title -------------> product_title
summary	-----------> product_sumry

뉴스크롤링/ 형태소 분석부분은 이미 맞춰둔 상태
공공데이터포털 연계 부분은 openapi를 가져오는 부분이라 변경하지 않음

# 개발
변수명
기능명
config.py에서 가져와서 사용하도록


[파이썬 명명규칙]

# 함수, 변수, 속성 
    owercase_underscore 형식, 스네이크 표기법(snake_case)를 따름 
    (예시 : def sum_input(x, y):, sum_value = x + y)

# 클래스, 예외
    CapitalizedWord 형식, 파스칼 표기법(PascalCase)를 따름 
    (예시 : class NamingRule:)

# 상수
    상수 이름에 밑줄로 구분된 대문자를 사용할 것을 권장 
    (예시 : NUMBER_OF_USERS = 450)

# 한줄 최대 글자 수
    79권장, \ 줄바꿈





