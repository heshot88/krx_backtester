from dotenv import load_dotenv
import os

from sqlalchemy import create_engine
from krx.krx_backtest.indicator_package import *
import datetime

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


def calc_RSI_MACD(df, st_date):
    # 매개변수 설정
    rsi_period = 14
    short_period = 12
    long_period = 26
    signal_period = 9
    ma_type = 'ema'  # 이평 방법: EMA

    # RSI + MACD 계산
    df['RSI_MACD'], df['RSI_MACD_Signal'] = RSI_MACD(df['index_value'], rsi_period, short_period, long_period,
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

    # 엑셀 파일로 저장
    output_file_path = 'filtered_data.xlsx'
    # 결과 출력
    filtered_df[['base_date', 'index_value', 'RSI_MACD', 'RSI_MACD_Signal', 'ACTION', 'differential', 'trend',
                 'signal', 'last_action']].to_excel(output_file_path, index=False)

    return filtered_df


def get_index_values(index_name, st_date):
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
    df['base_date'] = pd.to_datetime(df['base_date'])

    return df


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


def main():
    if not connect_db():
        return

    st_date = "2023-01-01"

    df = get_index_values("KOSPI", st_date)

    rsi_macd_df = calc_RSI_MACD(df, st_date)

    connection.close()


if __name__ == "__main__":
    main()
