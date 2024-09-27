import os
import asyncio
from sqlalchemy import create_engine
from krx_package.trade_manager_class import TradeManager, StockTradeInfo
from krx_package.common_package import *
import datetime
from openpyxl.styles import NamedStyle
from dotenv import load_dotenv
from krx_strategy.sangwoo_index_strategy_01 import start as sangwoo_01_start
from krx_strategy.sangwoo_index_strategy_01 import get_index_inverse_etf, get_index_etf

def save_to_excel(df, name="result_data"):
    output_file_path = 'D:\\Source\\python\\krx\\result\\' + name + "_" + str(
        datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')) + ".xlsx"

    result_df = df

    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
        result_df.to_excel(writer, sheet_name='결과', index=False)
        workbook = writer.book
        worksheet = writer.sheets['결과']

        # 숫자 서식 스타일 정의
        comma_style = NamedStyle(name="comma_style", number_format="#,##0")
        percentage_style = NamedStyle(name="percentage_style", number_format="0.00%")

        # 스타일을 workbook에 추가
        if "comma_style" not in workbook.named_styles:
            workbook.add_named_style(comma_style)
        if "percentage_style" not in workbook.named_styles:
            workbook.add_named_style(percentage_style)

        # 서식을 적용할 범위 설정
        columns_comma = ["B", "C", "D", "E", "F", "G", "H", "I", "K", "M", "P"]  # 세 자리마다 콤마 서식
        columns_percentage = ["J", "N", "O"]  # 백분율 서식

        # 각 셀에 대해 서식 적용
        for col in columns_comma:
            for row in range(2, len(result_df) + 2):  # 데이터가 2행부터 시작 (헤더 제외)
                cell = worksheet[f"{col}{row}"]
                cell.style = comma_style

        for col in columns_percentage:
            for row in range(2, len(result_df) + 2):
                cell = worksheet[f"{col}{row}"]
                cell.style = percentage_style

            # 열 너비 조정
        for col in worksheet.columns:
            max_length = 0
            column = col[0].column_letter  # 컬럼 이름 가져오기
            for cell in col:
                try:
                    # 셀의 길이 계산 (데이터의 길이와 비교하여 최대값 찾기)
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 10)  # 여유 공간 추가
            worksheet.column_dimensions[column].width = adjusted_width
    print(f"Data with formatting has been successfully saved to {output_file_path}")


def connect_db(db_host=None,db_port=None,db_user=None,db_password=None,db_name=None):

    if db_host is None or len(db_host) <=5 :
        load_dotenv()
        db_host = os.getenv('POSTGRESQL_HOST')
        db_port = os.getenv('POSTGRESQL_PORT')
        db_user = os.getenv('POSTGRESQL_USER')
        db_password = os.getenv('POSTGRESQL_PASSWORD')
        db_name = os.getenv('POSTGRESQL_DB')

    # PostgreSQL 연결 문자열 생성
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # SQLAlchemy 엔진 생성
    try:
        engine = create_engine(db_url)
        connection = engine.connect()  # SQLAlchemy 연결
        # print("DB 연결 성공")
        return connection
    except Exception as e:
        print("DB 연결 실패", e)
        return False


def sangwoo_01(conn,index_name,st_date,money,ohlc_type,initial_ratio=20,buy_ratio=20,sell_ratio=20,sub_buy_ratio=20,sub_sell_ratio=20,buy_fee_rate=0.015
               ,sell_fee_rate=0.2,is_first=True):
    etf_short_code = get_index_etf(index_name)
    inverse_etf_short_code = get_index_inverse_etf(index_name)
    main_etf_info = StockTradeInfo(etf_short_code,
                                   initial_ratio=initial_ratio,
                                   buy_ratio=buy_ratio,
                                   sell_ratio=sell_ratio,
                                   buy_fee_rate=buy_fee_rate,
                                   sell_fee_rate=sell_fee_rate,
                                   is_first=is_first
                                   )
    inverse_etf_info = StockTradeInfo(inverse_etf_short_code,
                                      initial_ratio=sub_buy_ratio,
                                      buy_ratio=sub_buy_ratio,
                                      sell_ratio=sub_sell_ratio,
                                      buy_fee_rate=buy_fee_rate,
                                      sell_fee_rate=sell_fee_rate,
                                      is_first=False
                                      )

    result_df = sangwoo_01_start(conn, index_name, st_date, money, main_etf_info, inverse_etf_info, ohlc_type)

    return result_df




async def main():
    conn = connect_db()
    if not conn:
        print("DB 연결 실패 -- 프로그램 종료 --")
        return
    # pd.set_option('display.max_rows', None)
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_colwidth', None)


    test_list = ["NASDAQ", "KOSPI", "KOSDAQ"]
    st_date = "2023-01-01"  # 시작일자
    money = 1000000000  # 초기자금
    ohlc_type = 'D'  # 일봉

    for index_name in test_list:
        result_df = sangwoo_01(conn,index_name,st_date,money,ohlc_type)
        save_to_excel(result_df, index_name + "_" + ohlc_type + "_result_data")

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
