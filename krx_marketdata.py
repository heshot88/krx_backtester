import requests
import psycopg2
from datetime import datetime, timedelta
import random
import time
import sys


def get_krx_data(_date):
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
        'Cookie': '__smVisitorID=zLbJoIJeNEK; JSESSIONID=SgBVS8NqakRw0C876nmwgIrFE8sEs6KgDNe9hzaO2R2iacRnzG1Z67T6b3FgoXkq.bWRjX2RvbWFpbi9tZGNvd2FwMS1tZGNhcHAxMQ==',
        'Host': 'data.krx.co.kr',
        'Origin': 'http://data.krx.co.kr',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }

    # POST 요청을 보낼 데이터
    body = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'trdDd': _date,
        'share': 1,
        'money': 1,
        'csvxls_isNo': 'false'
    }

    while True:
        # API에 POST 요청 보내기
        response = requests.post(url, data=body, headers=headers)

        # 요청이 성공했는지 확인
        if response.status_code == 200:
            # JSON 데이터 가져오기
            json_data = response.json()
            # print(json_data)

            # postgres 연결 설정
            db_config = {
                'host': 'localhost',
                'port': '5432',
                'user': 'trading',
                'password': 'athos01281!',
                'database': 'tsdb',

            }

            # PostgreSQL 연결
            connection = psycopg2.connect(**db_config)

            try:
                with connection.cursor() as cursor:
                    # JSON 데이터를 PostgreSQL에 삽입
                    for data in json_data['OutBlock_1']:
                        input_date = datetime.strptime(_date, "%Y%m%d").date()
                        sql = """INSERT INTO krx_stock_ohlc(base_date, short_code, isin_code, korean_name, mkt_id, mkt_name, open_price, high_price, low_price, close_price, acc_trade_value, acc_trade_volume, listed_shares, mkt_cap,close_price_rate,close_price_diff) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s) ON CONFLICT (base_date,short_code) DO UPDATE 
    SET 
        isin_code = EXCLUDED.isin_code,
        korean_name = EXCLUDED.korean_name,
        mkt_id = EXCLUDED.mkt_id,
        mkt_name = EXCLUDED.mkt_name,
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        acc_trade_value = EXCLUDED.acc_trade_value,
        acc_trade_volume = EXCLUDED.acc_trade_volume,
        listed_shares = EXCLUDED.listed_shares,
        mkt_cap = EXCLUDED.mkt_cap,
        close_price_rate = EXCLUDED.close_price_rate,
        close_price_diff = EXCLUDED.close_price_diff;"""
                        try:
                            cursor.execute(sql, (
                                input_date, "A" + data['ISU_SRT_CD'], data['ISU_CD'], data['ISU_ABBRV'], data['MKT_ID'],
                                data['MKT_NM'], data['TDD_OPNPRC'].replace(",", ""), data['TDD_HGPRC'].replace(",", ""),
                                data['TDD_LWPRC'].replace(",", ""), data['TDD_CLSPRC'].replace(",", ""),
                                data['ACC_TRDVAL'].replace(",", ""), data['ACC_TRDVOL'].replace(",", ""),
                                data['LIST_SHRS'].replace(",", ""), data['MKTCAP'].replace(",", ""), data['FLUC_RT'],
                                data['CMPPREVDD_PRC'].replace(",", "")))
                            # print("Executed SQL query:")
                            '''
                            print(cursor.mogrify(sql,(
                            input_date, "A" + data['ISU_SRT_CD'], data['ISU_CD'], data['ISU_ABBRV'], data['MKT_ID'],
                            data['MKT_NM'], data['TDD_OPNPRC'].replace(",", ""), data['TDD_HGPRC'].replace(",", ""),
                            data['TDD_LWPRC'].replace(",", ""), data['TDD_CLSPRC'].replace(",", ""),
                            data['ACC_TRDVAL'].replace(",", ""), data['ACC_TRDVOL'].replace(",", ""),
                            data['LIST_SHRS'].replace(",", ""), data['MKTCAP'].replace(",", ""),  data['FLUC_RT'],data['CMPPREVDD_PRC'].replace(",", ""))))
                            print(cursor.rowcount)
                            '''
                        except Exception as e:
                            print("error:", e)
                    try:
                        update_query = """
                UPDATE krx_stock_ohlc
                SET 
                    adjusted_open_price = CASE 
                                            WHEN adjusted_open_price = 0 THEN open_price 
                                            ELSE adjusted_open_price 
                                          END,
                    adjusted_close_price = CASE 
                                             WHEN adjusted_close_price = 0 THEN close_price 
                                             ELSE adjusted_close_price 
                                           END,
                    adjusted_high_price = CASE 
                                            WHEN adjusted_high_price = 0 THEN high_price 
                                            ELSE adjusted_high_price 
                                          END,
                    adjusted_low_price = CASE 
                                           WHEN adjusted_low_price = 0 THEN low_price 
                                           ELSE adjusted_low_price 
                                         END
                WHERE base_date = %s; """

                        cursor.execute(update_query, (input_date,))
                        # 변경사항을 커밋
                        connection.commit()
                    except Exception as e:
                        print("error2:", e)
            except Exception as e:
                print(e)
            finally:
                # 연결 닫기
                connection.close()
                return True
        else:
            print('HTTP 요청 실패:', response.status_code)
            print('retrying')


def random_sleep():
    # 랜덤한 대기 시간 생성 (1~4 사이의 정수)
    sleep_time = random.randint(1, 4)

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
    print('start')

    # 날짜 리스트 생성

    # 날짜 리스트 초기화
    date_list = []

    if len(sys.argv) > 1 and len(sys.argv[1]) > 7:
        date_list.append(sys.argv[1])
    else:  # 날짜 리스트 생성
        date_list = generate_dates()

    for date in date_list:
        now = datetime.now()
        print(now)
        print(date + ' started')
        get_krx_data(date)
        print(date + ' ended')
        random_sleep()


if __name__ == "__main__":
    main()
