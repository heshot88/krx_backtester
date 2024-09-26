import copy
import pandas as pd

from krx_backtest.common_package import get_index_values, get_krx_etf_values, get_ohlc
from krx_backtest.indicator_package import *
from krx_backtest.trade_manager_class import TradeManager, StockTradeInfo


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


def back_test(signal_df, main_stock_info, money=1000000000, sub_stock_info=None):
    sub_mode = False
    hold_stock_info = copy.deepcopy(main_stock_info)  # deep copy로 완전 별도로 운영
    hold_stock_info.initial_ratio = 100  # 100% 처음부터 사서 보유

    # buy_and_hold 전략
    hold_trade_manager = TradeManager(money)
    hold_trade_manager.set_stock_info(hold_stock_info)

    # 핵심전략
    trade_manager = TradeManager(money)
    trade_manager.set_stock_info(main_stock_info)

    main_df = signal_df.merge(main_stock_info.ohlc_df, on='base_date', how='left', suffixes=('', '_main'))
    if sub_stock_info is not None:
        trade_manager.set_stock_info(sub_stock_info)
        main_df = main_df.merge(sub_stock_info.ohlc_df, on='base_date', how='left', suffixes=('', '_sub'))
        sub_mode = True

    columns = [
        "일자", "가격", "잔고수량", "인버스가격", "인버스 잔고수량", "매입 총액", "평가 금액", "잔고평가", "수수료", "원금대비 수익률",
        "실현손익", "액션", "홀딩 잔고평가", "홀딩수익률", "수익률 비교", "잔액"
    ]

    result_df = pd.DataFrame(columns=columns)

    for idx, row in main_df.iterrows():
        trade_result = None
        sub_trade_result = None
        sub_balance = None
        sub_price = 0

        price = row['close_main']
        if price is None or pd.isna(price):
            continue

        if sub_mode:
            sub_price = row['close_sub']

        if idx == 0:
            hold_trade_manager.buy_stock(hold_stock_info.short_code, price)  # buy and hold 전략 초기화

        # ADD일때 Main 매수, Sub 매도 , REDUCE일때 Main 매도, Sub 매수
        if row['last_action'] == 'ADD':
            if sub_mode:
                sub_trade_result = trade_manager.sell_stock(sub_stock_info.short_code, sub_price)
            trade_result = trade_manager.buy_stock(main_stock_info.short_code, price)  # 매수

        elif row['last_action'] == 'REDUCE':
            trade_result = trade_manager.sell_stock(main_stock_info.short_code, price)  # 매도
            if sub_mode and main_stock_info.is_first is False:
                temp_main_balance = trade_manager.get_balance(main_stock_info.short_code)
                if temp_main_balance.shares <= 0:
                    sub_trade_result = trade_manager.buy_stock(sub_stock_info.short_code, sub_price)

        total_fee = int(trade_result['fee']) if trade_result is not None else 0 + int(
            sub_trade_result['fee']) if sub_trade_result is not None else 0

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
            "가격": row['close_main'],
            "인버스가격": row['close_sub'] if row.get('close_sub') is not None else 0,
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


def start(conn, index_name, st_date, money, main_etf_info, inverse_etf_info, ohlc_type='D'):
    day_index_df = get_index_values(conn, index_name, st_date)
    pd_st_date = pd.to_datetime(st_date).date()

    main_etf_df = get_krx_etf_values(conn, main_etf_info.short_code, st_date)

    inverse_etf_df = get_krx_etf_values(conn, inverse_etf_info.short_code, st_date)

    if ohlc_type in ['W', 'M', 'Y']:
        ohlc_index_df = get_ohlc(day_index_df, 'close', ohlc_type)
        main_etf_df = get_ohlc(main_etf_df, 'close', ohlc_type)
        inverse_etf_df = get_ohlc(inverse_etf_df, 'close', ohlc_type)
    elif ohlc_type != 'D':
        print('OHLC 압축 단위 오류')
        return None
    else:
        ohlc_index_df = day_index_df

    main_etf_info.ohlc_df = main_etf_df
    inverse_etf_info.ohlc_df = inverse_etf_df

    rsi_macd_df = calc_RSI_MACD(ohlc_index_df, pd_st_date, "close")
    # stock_etf_day_inverse_info = None
    result_df = back_test(rsi_macd_df, main_etf_info, money, inverse_etf_info)

    return result_df
