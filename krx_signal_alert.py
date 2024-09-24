import copy

import pandas as pd
from dotenv import load_dotenv
import os
import asyncio
from sqlalchemy import create_engine
from krx_backtest.indicator_package import *
from krx_backtest.trade_manager_class import TradeManager, StockTradeInfo
import datetime
from openpyxl import Workbook
from openpyxl.styles import NamedStyle
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter

from krx_telegram import TelegramSender

connection = None

TOKEN = '6588514172:AAH5hED9lPuPcMB7VJ8pHvWFWSWQya5aj80'
CHAT_ID = '-1002209543022'


def signal_simple(row):
    if row['RSI_MACD'] > row['RSI_MACD_Signal']:
        return "ADD"
    elif row['RSI_MACD'] < row['RSI_MACD_Signal']:
        return "REDUCE"
    else:
        return None


def calc_differential(row, df):
    # 미분값 계산

    # 현재 행의 인덱스
    idx = row.name

    # 첫 번째 행일 경우 전일 데이터가 없으므로 NaN 반환
    if idx == 0:
        return None

    # 전일 데이터 가져오기
    prev_row = df.iloc[idx - 1]

    # 커스텀 메트릭 계산
    custom_metric = (row['RSI_MACD'] - (prev_row['RSI_MACD'] - 0.001)) / (
            row['RSI_MACD_Signal'] - (prev_row['RSI_MACD_Signal'] - 0.001))

    return custom_metric


# trend 값 계산 함수
def calc_trend(row, df):
    idx = row.name
    # 첫 번째 행은 비교할 이전 행이 없으므로 WEAK 반환
    if idx == 0:
        return None

    # 현재 행과 이전 행의 differential 값을 비교
    prev_row = df.iloc[idx - 1]
    if row['differential'] > 1 and row['differential'] > prev_row['differential']:
        return "STRONG"
    else:
        return "WEAK"


def calc_signal(row):
    if row['ACTION'] == "REDUCE" and row['trend'] == "STRONG":
        result = "REDUCE"
    elif row['ACTION'] == "REDUCE" and row['trend'] == "WEAK":
        result = "ADD"
    elif row['ACTION'] == "ADD" and row['trend'] == "STRONG":
        result = "ADD"
    elif row['ACTION'] == "ADD" and row['trend'] == "WEAK":
        result = "REDUCE"
    elif row['ACTION'] == "ADD":
        result = "ADD"
    else:
        result = None

    return result


# last_action 계산 함수 정의
def calc_last_action(row, df):
    idx = row.name  # 현재 행의 인덱스 가져오기

    # 현재 signal 값
    current_signal = row['signal']

    # 첫 번째 행일 경우, 이전 행이 없으므로 현재 값을 그대로 반환
    if idx == 0:
        return current_signal

    # 전 행의 signal 값 가져오기
    previous_signal = df.iloc[idx - 1]['signal']

    # 이전 행의 signal 값이 None이 아니고, 현재 signal 값과 같은 경우 현재 signal 값 유지
    if previous_signal is not None and previous_signal == current_signal:
        return current_signal
    else:
        # 이전 행의 signal 값이 None이거나, 현재 signal 값과 다르면 None 반환
        return None


def calc_RSI_MACD(df, st_date, column_name):
    # 매개변수 설정
    rsi_period = 14
    short_period = 12
    long_period = 26
    signal_period = 9
    ma_type = 'ema'  # 이평 방법: EMA

    # RSI + MACD 계산
    df['RSI_MACD'], df['RSI_MACD_Signal'] = RSI_MACD(df[column_name], rsi_period, short_period, long_period,
                                                     signal_period, ma_type)

    # df에 signal_simple 함수 적용하여 ACTION 컬럼 추가
    df['ACTION'] = df.apply(signal_simple, axis=1)
    df['differential'] = df.apply(lambda row: calc_differential(row, df), axis=1)

    # trend 값 계산하여 df에 추가
    df['trend'] = df.apply(lambda row: calc_trend(row, df), axis=1)

    df['signal'] = df.apply(calc_signal, axis=1)
    df['last_action'] = df.apply(lambda row: calc_last_action(row, df), axis=1)
    # st_date보다 큰 날짜만 필터링
    filtered_df = df[df['base_date'] > st_date]

    # 결과 출력
    # print(filtered_df[
    #           ['base_date', 'index_value', 'RSI_MACD', 'RSI_MACD_Signal', 'ACTION', 'differential', 'trend', 'signal',
    #            'last_action']])

    return filtered_df


def get_ohlc(df, column_name, type='W'):
    # 데이터프레임이 비어있는 경우 처리
    if df.empty:
        return pd.DataFrame()

        # base_date를 주 단위로 변환하여 'week' 열 생성
    df['base_date'] = pd.to_datetime(df['base_date']).dt.to_period(type).apply(lambda x: x.start_time.date())

    # 주 단위로 그룹화하여 OHLC 값 계산
    weekly_ohlc = df.groupby('base_date').agg(
        open_price=(column_name, 'first'),
        high_price=(column_name, 'max'),
        low_price=(column_name, 'min'),
        close_price=(column_name, 'last')
    ).reset_index()

    # 결과 반환
    return weekly_ohlc


def get_yfinance_symbol(index_name):
    if index_name == 'KOSPI':
        symbol = '^KS11'  # KODEX 인버스
    elif index_name == 'KOSDAQ':
        symbol = '^KQ11'  # KODEX 코스닥150선물인버스
    elif index_name == 'NASDAQ':
        symbol = '^IXIC'  # KODEX 미국나스닥100선물인버스(H)
    else:
        return None

    return symbol


def get_index_values(index_name, st_date):
    global connection
    index_name = index_name.strip()
    st_date = st_date.strip()
    # st_date를 datetime 객체로 변환
    st_date = pd.to_datetime(st_date)

    # st_date에서 30일 전 날짜 계산
    start_date = st_date - datetime.timedelta(days=180)

    symbol = get_yfinance_symbol(index_name)

    # 데이터 가져오기 (index_name과 날짜 조건을 원하는 대로 수정)
    query = """
    SELECT date as base_date, symbol_name as index_name, close as index_value
    FROM symbol_price_view
    WHERE symbol = %s
    AND date >= %s
    ORDER BY date;
    """

    params = (symbol, start_date)
    df = pd.read_sql(query, connection, params=params)

    # base_date를 인덱스로 설정
    df['base_date'] = pd.to_datetime(df['base_date']).dt.date

    return df


def get_index_inverse_etf(index_name):
    if index_name == 'KOSPI':
        short_code = 'A114800'  # KODEX 인버스
    elif index_name == 'KOSDAQ':
        short_code = 'A251340'  # KODEX 코스닥150선물인버스
    elif index_name == 'NASDAQ':
        short_code = 'A409810'  # KODEX 미국나스닥100선물인버스(H)
    else:
        return None

    return short_code


def get_index_etf(index_name):
    if index_name == 'KOSPI':
        short_code = 'A069500'
    elif index_name == 'KOSDAQ':
        short_code = 'A069500'
    elif index_name == 'NASDAQ':
        short_code = 'A304940'
    else:
        return None

    return short_code


def get_krx_etf_values(short_code, st_date):
    global connection
    short_code = short_code.strip()
    st_date = st_date.strip()

    query = """
        SELECT base_date, korean_name, close_price,short_code
        FROM krx_etf_ohlc
        WHERE short_code = %s
        AND base_date >= %s
        ORDER BY base_date;
        """

    params = (short_code, st_date)
    df = pd.read_sql(query, connection, params=params)

    # base_date를 인덱스로 설정
    df['base_date'] = pd.to_datetime(df['base_date']).dt.date

    return df


def save_to_excel(df, name="result_data"):
    output_file_path = 'D:\\Source\\python\\krx\\result\\' + name +"_"+ str(
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
        columns_comma = ["B", "C", "D", "E", "F", "G", "H","I", "K","M", "P"]  # 세 자리마다 콤마 서식
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


def connect_db():
    global connection

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
        print("DB 연결 성공")
        return True
    except Exception as e:
        print("DB 연결 실패", e)
        return False


def back_test(signal_df, main_stock_info, money=1000000000, sub_stock_info=None):
    sub_mode = False
    hold_stock_info = copy.deepcopy(main_stock_info)  # deep copy로 완전 별도로 운영
    hold_stock_info.set_initial_ratio(100)  # 100%

    # buy_and_hold 전략
    hold_trade_manager = TradeManager(money)
    hold_trade_manager.set_stock_info(hold_stock_info)

    # 핵심전략
    trade_manager = TradeManager(money)
    trade_manager.set_stock_info(main_stock_info)

    main_df = signal_df.merge(main_stock_info.ohlc_df, on='base_date', how='left')
    if sub_stock_info is not None:
        trade_manager.set_stock_info(sub_stock_info)
        main_df = main_df.merge(sub_stock_info.ohlc_df, on='base_date', how='left', suffixes=('', '_sub'))
        sub_mode = True

    columns = [
        "일자", "가격", "잔고수량","인버스가격", "인버스 잔고수량", "매입 총액", "평가 금액", "잔고평가", "수수료", "원금대비 수익률",
        "실현손익", "액션", "홀딩 잔고평가", "홀딩수익률", "수익률 비교", "잔액"
    ]

    result_df = pd.DataFrame(columns=columns)

    for idx, row in main_df.iterrows():
        trade_result = None
        sub_trade_result = None
        sub_balance = None
        sub_price = 0

        price = row['close_price']
        if price is None:
            continue

        if sub_mode:
            sub_price = row['close_price_sub']

        if idx == 0:
            hold_trade_manager.buy_stock(hold_stock_info.short_code, price)  # buy and hold 전략 초기화

        # ADD일때 Main 매수, Sub 매도 , REDUCE일때 Main 매도, Sub 매수
        if row['last_action'] == 'ADD':
            trade_result = trade_manager.buy_stock(main_stock_info.short_code, price)  # 매수
            if sub_mode :
                sub_trade_result = trade_manager.sell_stock(sub_stock_info.short_code, sub_price)
        elif row['last_action'] == 'REDUCE':
            trade_result = trade_manager.sell_stock(main_stock_info.short_code, price)  # 매도
            if sub_mode and main_stock_info.is_first is False:
                temp_main_balance= trade_manager.get_balance(main_stock_info.short_code)
                if temp_main_balance.shares <=0 :
                    sub_trade_result = trade_manager.buy_stock(sub_stock_info.short_code, sub_price)

        total_fee = int(trade_result['fee']) if trade_result is not None else 0 + int(sub_trade_result['fee']) if sub_trade_result is not None else 0

        # 가격 업데이트
        hold_trade_manager.calc_stock_profit_rate(main_stock_info.short_code, price)
        trade_manager.calc_stock_profit_rate(main_stock_info.short_code, price)
        main_balance = trade_manager.get_balance(main_stock_info.short_code)

        if sub_mode:
            hold_trade_manager.calc_stock_profit_rate(sub_stock_info.short_code, sub_price)
            trade_manager.calc_stock_profit_rate(sub_stock_info.short_code, sub_price)
            sub_balance = trade_manager.get_balance(sub_stock_info.short_code)

        trade_account = trade_manager.calc_account_profit_rate()  # 현재 평가
        buy_and_hold_account = hold_trade_manager.calc_account_profit_rate()
        new_row = {
            "일자": row['base_date'],
            "가격": row['close_price'],
            "인버스가격": row['close_price_sub'],
            "잔고수량": main_balance.shares,
            "인버스 잔고수량": sub_balance.shares if sub_balance is not None else 0,
            "매입 총액": trade_manager.total_investment,
            "평가 금액": trade_account['total_eval_amount'],
            "잔고평가": trade_manager.remaining_cash + trade_account['total_eval_amount'],
            "잔액": trade_manager.remaining_cash,
            "수수료": total_fee,
            "원금대비 수익률": trade_account['profit_rate'],
            "실현손익": trade_account['total_realized_profit'],
            "액션": row['last_action'],
            "홀딩 잔고평가": buy_and_hold_account['total_eval_amount'],
            "홀딩수익률": buy_and_hold_account['profit_rate'],
            "수익률 비교": trade_account['profit_rate'] - buy_and_hold_account['profit_rate']
        }

        # 새로운 행을 DataFrame으로 변환
        new_row_df = pd.DataFrame([new_row])

        # 모든 값이 NaN인 열 제거
        new_row_df = new_row_df.dropna(how='all', axis=1)

        # 새로운 행이 비어 있지 않을 때만 추가
        if not new_row_df.empty:
            result_df = pd.concat([result_df, new_row_df], ignore_index=True)

    return result_df


async def main():
    global connection
    if not connect_db():
        return

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)

    test_list = ["KOSPI", "KOSDAQ"]

    st_date = "2023-01-01"
    money = 1000000000
    index_name = test_list[0]
    day_index_df = get_index_values(index_name, st_date)

    pd_st_date = pd.to_datetime(st_date).date()
    rsi_macd_df = calc_RSI_MACD(day_index_df, pd_st_date, "index_value")
    print(rsi_macd_df)
    etf_short_code = get_index_etf(index_name)
    day_etf_df = get_krx_etf_values(etf_short_code, st_date)
    inverse_etf_short_code = get_index_inverse_etf(index_name)
    inverse_day_etf_df = get_krx_etf_values(inverse_etf_short_code, st_date)

    stock_etf_day_info = StockTradeInfo(etf_short_code, day_etf_df)
    stock_etf_day_inverse_info = StockTradeInfo(inverse_etf_short_code, inverse_day_etf_df,sell_ratio=100)

    day_result_df = back_test(rsi_macd_df, stock_etf_day_info, money, stock_etf_day_inverse_info)

    # day_merged_df = pd.merge(rsi_macd_df, day_etf_df, on='base_date',
    #                          how='left')

    #
    # # 주봉 기준 백테스트
    # week_index_df = get_ohlc(day_index_df, 'index_value')
    # week_rsi_macd_df = calc_RSI_MACD(week_index_df, pd_st_date, "close_price")
    # week_etf_df = get_ohlc(day_etf_df, 'close_price')
    # week_merged_df = pd.merge(week_rsi_macd_df, week_etf_df, on='base_date',
    #                           how='left')  # how='inner'는 기본
    #
    # week_merged_df['close_price'] = week_merged_df['close_price_y']
    # week_merged_df = week_merged_df.drop(['close_price_x', 'close_price_y'], axis=1)
    # # print(week_merged_df)
    # week_result_df = back_test(week_merged_df)
    #
    # month_index_df = get_ohlc(day_index_df, 'index_value', 'M')
    # month_rsi_macd_df = calc_RSI_MACD(month_index_df, pd_st_date, "close_price")
    # month_etf_df = get_ohlc(day_etf_df, 'close_price', 'M')
    # month_merged_df = pd.merge(month_rsi_macd_df, month_etf_df, on='base_date',
    #                            how='left')  # how='inner'는 기본
    #
    # month_merged_df['close_price'] = month_merged_df['close_price_y']
    # month_merged_df = month_merged_df.drop(['close_price_x', 'close_price_y'], axis=1)
    # print(month_merged_df)
    # # how='inner'는 기본값, 필요에 따라 outer, left, right 변경 가능
    #
    # month_result_df = back_test(month_merged_df)

    telegram_sender = TelegramSender(TOKEN)
    telegram_sender.start()

    # telegram_sender.send_message(CHAT_ID,"하이")
    #
    #
    # # result_df를 엑셀로 저장
    save_to_excel(day_result_df, index_name + "_day_result_data")
    # save_to_excel(week_result_df, index_name + "_week_result_data")
    # save_to_excel(month_result_df, index_name + "_month_result_data")

    connection.close()

    await telegram_sender.wait_until_done()
    telegram_sender.stop()


if __name__ == "__main__":
    asyncio.run(main())
