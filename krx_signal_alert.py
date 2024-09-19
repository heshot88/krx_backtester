import pandas as pd
from dotenv import load_dotenv
import os

from sqlalchemy import create_engine
from krx.krx_backtest.indicator_package import *
from krx.krx_backtest.trade_manager_class import TradeManager
import datetime
from openpyxl import Workbook
from openpyxl.styles import NamedStyle
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter

connection = None


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

    df['action_signal'] = None

    df['signal'] = df.apply(calc_signal, axis=1)
    df['last_action'] = df.apply(lambda row: calc_last_action(row, df), axis=1)
    # st_date보다 큰 날짜만 필터링
    filtered_df = df[df['base_date'] > st_date]

    # 결과 출력
    # print(filtered_df[
    #           ['base_date', 'index_value', 'RSI_MACD', 'RSI_MACD_Signal', 'ACTION', 'differential', 'trend', 'signal',
    #            'last_action']])

    return filtered_df


def get_weekly_ohlc(df, column_name):
    # 데이터프레임이 비어있는 경우 처리
    if df.empty:
        return pd.DataFrame()

        # base_date를 주 단위로 변환하여 'week' 열 생성
    df['base_date'] = pd.to_datetime(df['base_date']).dt.to_period('W').apply(lambda x: x.start_time.date())

    # 주 단위로 그룹화하여 OHLC 값 계산
    weekly_ohlc = df.groupby('base_date').agg(
        open_price=(column_name, 'first'),
        high_price=(column_name, 'max'),
        low_price=(column_name, 'min'),
        close_price=(column_name, 'last')
    ).reset_index()

    # 결과 반환
    return weekly_ohlc


def get_index_values(index_name, st_date):
    global connection
    index_name = index_name.strip()
    st_date = st_date.strip()
    # st_date를 datetime 객체로 변환
    st_date = pd.to_datetime(st_date)

    # st_date에서 30일 전 날짜 계산
    start_date = st_date - datetime.timedelta(days=180)

    # 데이터 가져오기 (index_name과 날짜 조건을 원하는 대로 수정)
    query = """
    SELECT base_date, index_name, index_value
    FROM mkt_index_info
    WHERE index_name = %s
    AND base_date >= %s
    ORDER BY base_date;
    """

    params = (index_name, start_date)
    df = pd.read_sql(query, connection, params=params)

    # base_date를 인덱스로 설정
    df['base_date'] = pd.to_datetime(df['base_date']).dt.date

    return df


def get_krx_etf_values(index_name, st_date):
    global connection
    index_name = index_name.strip()
    st_date = st_date.strip()
    if index_name == 'KOSPI':
        short_code = 'A069500'
    elif index_name == 'KOSDAQ':
        short_code = 'A069500'
    elif index_name == 'NASDAQ':
        short_code = 'A304940'
    else:
        return None

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
    output_file_path = 'D:\\Source\\python\\krx\\result\\' + name + str(
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
        columns_comma = ["B", "C", "D", "E", "F", "G", "I", "K"]  # 세 자리마다 콤마 서식
        columns_percentage = ["H", "L", "M"]  # 백분율 서식

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


def backtest(df, initial_ratio=0.5):
    day_trader = TradeManager(initial_ratio=initial_ratio)

    columns = [
        "일자", "가격", "잔고수량", "매입 총액", "평가 금액", "잔고평가", "수수료", "원금대비 수익률",
        "실현손익", "액션", "홀딩 잔고평가", "홀딩수익률", "수익률 비교"
    ]

    result_df = pd.DataFrame(columns=columns)

    for idx, row in df.iterrows():
        trade_result = None

        price = row['close_price']
        if price is None:
            continue

        if idx == 0:
            day_trader.init_buy_and_hold(price)
        if row['last_action'] == 'ADD':
            trade_result = day_trader.buy_stock(price)
        elif row['last_action'] == 'REDUCE':
            trade_result = day_trader.sell_stock(price)

        profit = day_trader.calc_profit_rate(price)
        buy_and_hold = day_trader.calc_buy_and_hold(price)
        new_row = {
            "일자": row['base_date'],
            "가격": row['close_price'],
            "잔고수량": day_trader.total_shares,
            "매입 총액": day_trader.total_investment,
            "평가 금액": profit['eval_amount'],
            "잔고평가": day_trader.remaining_cash + profit['eval_amount'],
            "수수료": trade_result['fee'] if trade_result is not None else 0,
            "원금대비 수익률": profit['profit_rate'],
            "실현손익": profit['realized_profit'],
            "액션": row['last_action'],
            "홀딩 잔고평가": buy_and_hold['eval_amount'],
            "홀딩수익률": buy_and_hold['profit_rate'],
            "수익률 비교": profit['profit_rate'] - buy_and_hold['profit_rate']
        }

        # 새로운 행을 DataFrame으로 변환
        new_row_df = pd.DataFrame([new_row])

        # 모든 값이 NaN인 열 제거
        new_row_df = new_row_df.dropna(how='all', axis=1)

        # 새로운 행이 비어 있지 않을 때만 추가
        if not new_row_df.empty:
            result_df = pd.concat([result_df, new_row_df], ignore_index=True)

    return result_df


def main():
    if not connect_db():
        return

    st_date = "2023-01-01"
    index_name = "KOSPI"
    day_index_df = get_index_values(index_name, st_date)

    pd_st_date = pd.to_datetime(st_date).date()
    rsi_macd_df = calc_RSI_MACD(day_index_df, pd_st_date, "index_value")
    day_etf_df = get_krx_etf_values(index_name, st_date)

    day_merged_df = pd.merge(rsi_macd_df, day_etf_df, on='base_date',
                             how='left')

    day_result_df = backtest(day_merged_df)

    # 주봉 기준 백테스트
    week_index_df = get_weekly_ohlc(day_index_df, 'index_value')

    week_rsi_macd_df = calc_RSI_MACD(week_index_df, pd_st_date, "close_price")

    print(week_rsi_macd_df)
    week_etf_df = get_weekly_ohlc(day_etf_df, 'close_price')

    week_merged_df = pd.merge(week_rsi_macd_df, week_etf_df, on='base_date',
                              how='left')  # how='inner'는 기본

    week_merged_df['close_price'] = week_merged_df['close_price_y']
    week_merged_df = week_merged_df.drop(['close_price_x', 'close_price_y'], axis=1)
    print(week_merged_df)
    # how='inner'는 기본값, 필요에 따라 outer, left, right 변경 가능

    week_result_df = backtest(week_merged_df)
    # result_df를 엑셀로 저장
    save_to_excel(day_result_df, index_name + "_day_result_data")
    save_to_excel(week_result_df, index_name + "_week_result_data")

    connection.close()


if __name__ == "__main__":
    main()
