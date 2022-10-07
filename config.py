#postgresql 접속정보
# db_host = '192.168.123.140'
# db_dbname = 'water'
# db_user = 'water'
# db_password = '1q2w3e4r%^'
# db_port = 5432

db_host = '192.168.123.132'
db_dbname = 'water'
db_user = 'postgres'
db_password = 'pispdb2021'
db_port = 5432


#형태소 분석 관련 변수
jdbc_url = "postgresql://"+db_user+":"+db_password+"@"+db_host+":"+str(db_port)+"/"+db_dbname
regex_limit = 1000
regex_page = 0
regex_user_id = "admin"

#저수지 데이터 api key
open_api_service_key = "gKwMHq7ihGLuc/D41kRJP5xjtNjcl/eQHsOhiaJTbXUpnATpQFaC+Nby8aYFv5No+Pme9T9zuhbGJbrS3zBWMA=="

#earth_explorer 계정정보
earth_explorer_id = "ymseo"
earth_explorer_password = "geopeopleseo4655"

#copernicus 계정정보
copernicus_id = "ymseo"
copernicus_password = "sseo4655"