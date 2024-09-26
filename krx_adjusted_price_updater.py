import requests
import psycopg2
from datetime import datetime, timedelta
# from openvpn_api import Client
# import Client
import random
import time
import sys
#
# def connect_vpn():
#     vpn_config = '/home/ebs-server/python/krx/hwang.ovpn'
#     # VPN 인증 정보
#     username = 'hwang'
#     password = 'ghkddmltjr1!'
#     client = Client()
#     client.connect(config_file=vpn_config, auth=(username, password))
#     return client.connected
#
# # VPN 해제 함수
# def disconnect_vpn(client):
#     client.disconnect()


def get_krx_data(_date,_short_code):
    # vpn_connected = connect_vpn()
    st_date = "19950502"
    # API 엔드포인트 URL
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'

    # HTTP 요청 헤더
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Length': '111',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': '__smVisitorID=zLbJoIJeNEK; JSESSIONID=BPWnWez7YdL1T3NhHeXLlJGv0aPMs8oHPLQKg10Uu1uNXCKVzec8kVZgBun1lVTu.bWRjX2RvbWFpbi9tZGNvd2FwMS1tZGNhcHAwMQ==',
        'Host': 'data.krx.co.kr',
        'Origin': 'http://data.krx.co.kr',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }

    # MariaDB 연결 설정
    db_config = {
        'host': 'localhost',
        'port': '5432',
        'user': 'hwang',
        'password': 'ghkddmltjr1!',
        'database': 'tsdb',

    }

    # MariaDB 연결

    # PostgreSQL 연결
    connection = psycopg2.connect(**db_config)



    try:
        with connection.cursor() as cursor:
            # SQL 쿼리 실행
            db_date = datetime.strptime(_date, '%Y%m%d').strftime('%Y-%m-%d')

            if _short_code:
                cursor.execute("""SELECT DISTINCT short_code, isin_code, korean_name
                     FROM krx_stock_ohlc
                    WHERE base_date = %s
                    AND short_code = %s """, (db_date, _short_code))
            else:
                cursor.execute("""
                    SELECT DISTINCT short_code, isin_code, korean_name
                    FROM krx_stock_ohlc
                    WHERE base_date = %s
                """, (db_date,))

            # 결과 가져오기
            rows = cursor.fetchall()

            # 결과 출력 또는 처리
            for row in rows:
                short_code = row[0]
                isin_code = row[1]
                korean_name = row[2]
                print(row)
                random_sleep()

                short_code_digit = short_code[1:]
                finder = short_code_digit + '/' + korean_name
                body = {
                    'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
                    'locale': 'ko_KR',
                    'tboxisuCd_finder_stkisu0_1': finder,
                    'isuCd': isin_code,
                    'isuCd2': short_code,
                    'codeNmisuCd_finder_stkisu0_1':korean_name,
                    'param1isuCd_finder_stkisu0_1':'ALL',
                    'strtDd':st_date,
                    'endDd':_date,
                    'adjStkPrc_check':'Y',
                    'adjStkPrc':2,
                    'share': 1,
                    'money': 1,
                    'csvxls_isNo': 'false'
                }
                print(body)

                # if vpn_connected:
                response = requests.post(url, data=body, headers=headers)
                if response.status_code == 200:
                    # JSON 데이터 가져오기
                    json_data = response.json()
                    commit_count = 0  # 카운터 초기화
                    for data in json_data['output']:
                        base_date = data['TRD_DD'].replace('/', '-')
                        sql = """UPDATE krx_stock_ohlc SET adjusted_open_price = %s, adjusted_high_price = %s,adjusted_low_price = %s,adjusted_close_price = %s WHERE base_date = %s AND short_code = %s;"""
                        # SQL 문 실행
                        cursor.execute(sql, (data['TDD_OPNPRC'].replace(",", ""), data['TDD_HGPRC'].replace(",", ""),
                                             data['TDD_LWPRC'].replace(",", ""), data['TDD_CLSPRC'].replace(",", ""),
                                             base_date,
                                             short_code))
                        # 대체된 SQL 쿼리 출력
                        print("Executed SQL query:")
                        print(cursor.mogrify(sql,
                                             (data['TDD_OPNPRC'].replace(",", ""), data['TDD_HGPRC'].replace(",", ""),
                                              data['TDD_LWPRC'].replace(",", ""), data['TDD_CLSPRC'].replace(",", ""),
                                              base_date,
                                              short_code)))

                        commit_count += 1  # 카운터 증가
                        if commit_count % 1000 == 0:  # 1000개마다 commit
                            connection.commit()  # 변경사항 커밋
                            commit_count = 0  # 카운터 초기화

                # 남은 데이터에 대한 commit
                if commit_count > 0:
                    connection.commit()


    except Exception as e :
        print(e)
    finally:
        # 연결 닫기
        connection.close()

def random_sleep():
    # 랜덤한 대기 시간 생성 (1~4 사이의 정수)
    sleep_time = random.randint(1, 3)

    # 대기
    time.sleep(sleep_time)

def generate_dates():
    # 시작일과 오늘 날짜 설정
    # start_date = datetime(1995, 5, 2)
    start_date = datetime.now()
    end_date = datetime.now()

    # 날짜 리스트 초기화
    date_list = []

    # 시작일부터 오늘까지의 날짜를 리스트에 추가
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)

    return date_list
def main():
    now = datetime.now()
    print('update adjusted prices start ' + now)


    if len(sys.argv) > 1:
        date_list.append(sys.argv[1])
        short_code = sys.argv[2]
    else :    # 날짜 리스트 생성
        date_list = generate_dates()

    for date in date_list:
        print(date + ' adjusted started')
        get_krx_data(date,short_code)
        print(date +' adjusted ended')
        random_sleep()

    now = datetime.now()
    print('update adjusted prices finished ' + now)


if __name__ == "__main__":
    main()
