import datetime
import pandas as pd


def get_index_values(connection, index_name, st_date=None):
    index_name = index_name.strip()
    if st_date is not None :
        st_date = st_date.strip()
        # st_date를 datetime 객체로 변환
        st_date = pd.to_datetime(st_date)

        # st_date에서 30일 전 날짜 계산
        start_date = st_date - datetime.timedelta(days=1460)

    symbol = get_yfinance_symbol(index_name)

    # 데이터 가져오기 (index_name과 날짜 조건을 원하는 대로 수정)
    query = """
    SELECT date as base_date, symbol_name as index_name, open,high,low, close
    FROM symbol_price_view
    WHERE symbol = %s   
    """
    if st_date is not None :
        query += """
        And date >=%s
        """

    query +="""
    ORDER BY date ASC;
    """

    if st_date is not None :
        params = (symbol,start_date)
    else :
        params = (symbol,)

    df = pd.read_sql(query, connection, params=params)

    # base_date를 인덱스로 설정
    df['base_date'] = pd.to_datetime(df['base_date']).dt.date

    return df

def get_krx_etf_values(connection, short_code, st_date):

    short_code = short_code.strip()
    st_date = st_date.strip()

    query = """
        SELECT base_date, korean_name,open_price as open, high_price as high, low_price as low, close_price as close,short_code
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


def get_yfinance_symbol(index_name):
    if index_name == 'KOSPI':
        symbol = '^KS11'
    elif index_name == 'KOSDAQ':
        symbol = '^KQ11'
    elif index_name == 'NASDAQ':
        symbol = '^IXIC'
    elif index_name == 'DOW JONES':
        symbol = '^DJI'
    elif index_name == 'EURO':
        symbol = '^STOXX50E'
    elif index_name == 'HANGSENG':
        symbol = '^HSI'
    elif index_name == 'SHANGHAI':
        symbol = '000001.SS'
    elif index_name == 'NIKKEI':
        symbol = '^N225'
    else:
        return None

    return symbol





def get_ohlc(df, column_name, period='W'):
    """
    :param df: dataframe
    :param column_name: close
    :param period: W, M, Y

    """
    if df.empty:
        return pd.DataFrame()

        # base_date를 주 단위로 변환하여 'week' 열 생성
    df['base_date'] = pd.to_datetime(df['base_date']).dt.to_period(period).apply(lambda x: x.start_time.date())

    # 주 단위로 그룹화하여 OHLC 값 계산
    weekly_ohlc = df.groupby('base_date').agg(
        open=(column_name, 'first'),
        high=(column_name, 'max'),
        low=(column_name, 'min'),
        close=(column_name, 'last')
    ).reset_index()

    # 결과 반환
    return weekly_ohlc

