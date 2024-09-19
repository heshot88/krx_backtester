import pandas as pd


def MA(data, period):
    ma_values = [data.iloc[0]]  # 첫 번째 값은 그대로 사용
    for i in range(1, len(data)):
        pPriceVal = data.iloc[i]
        if pd.isnull(pPriceVal):  # 유효하지 않은 값 처리 (NaN으로 가정)
            if i > 1:
                ma_values.append(ma_values[-1])  # 이전 값을 유지
            else:
                ma_values.append(0)  # 첫 번째 값이 없을 경우 0으로 설정
        else:
            if i > 1:
                vResult = ma_values[-1] + 2 / (period + 1) * (pPriceVal - ma_values[-1])
                ma_values.append(vResult)
            else:
                ma_values.append(pPriceVal)  # 첫 번째 값은 그대로 사용

    return pd.Series(ma_values, index=data.index)

# 사용자 정의 EMA 계산 함수 (MA_EX와 유사한 방식으로 구현)
def EMA(prices, period):
    alpha = 2 / (period + 1)
    ema_values = [prices.iloc[0]]  # 첫 번째 값은 그대로 사용
    for price in prices[1:]:
        ema_values.append((price - ema_values[-1]) * alpha + ema_values[-1])
    return pd.Series(ema_values, index=prices.index)

# RSI 계산 함수 (기본적인 RSI 공식 사용)
def RSI(data, period=14):
    # 변화량 (오늘 - 어제)
    delta = data.diff().fillna(0)

    # 상승폭과 하락폭 구분
    up_amt = delta.clip(lower=0)  # 상승폭: 음수를 0으로 변환
    down_amt = -delta.clip(upper=0)  # 하락폭: 양수를 0으로 변환

    # MA 계산: pLeng보다 작을 경우에는 누적합, 그 외에는 롤링 윈도우 사용
    up_avg = up_amt.rolling(window=period, min_periods=1).mean()
    down_avg = down_amt.rolling(window=period, min_periods=1).mean()

    # RSI 계산
    rs = up_avg / down_avg
    rsi = 100 - (100 / (1 + rs))
    rsi[down_avg == 0] = 100  # 하락이 없는 경우, RSI는 100

    return rsi


# MACD 계산 함수
def MACD(price_data, short_period, long_period, ma_type='ema'):
    if ma_type != 'ema':
        # MA_EX 처리를 해야 한다면 다른 유형의 MA로 확장할 수 있습니다.
        raise NotImplementedError("현재는 EMA(지수이평)만 지원합니다.")

    # 단기 EMA 계산
    short_ema = EMA(price_data, short_period)

    # 장기 EMA 계산
    long_ema = EMA(price_data, long_period)

    # MACD 계산 (단기 EMA - 장기 EMA)
    macd = short_ema - long_ema

    return macd


def RSI_MACD(price_data, rsi_period, short_period, long_period, signal_period, ma_type='ema'):
    # 1. RSI 계산
    rsi_values = RSI(price_data, rsi_period)

    # 2. 단기 및 장기 이동평균 계산 (custom_ma 또는 custom_ema 사용)
    if ma_type == 'ema':
        short_ma = EMA(rsi_values, short_period)
        long_ma = EMA(rsi_values, long_period)
    else:
        short_ma = MA(rsi_values, short_period)
        long_ma = MA(rsi_values, long_period)

    # 3. RSI 기반 MACD 계산
    rsi_macd = short_ma - long_ma

    # 4. 신호선 계산 (MA_EX)
    rsi_macd_signal = EMA(rsi_macd, signal_period)

    return rsi_macd, rsi_macd_signal
