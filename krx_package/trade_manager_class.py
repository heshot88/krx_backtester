from collections import deque


class StockBalance:
    def __init__(self, info, trade_price=0):
        self.price = trade_price
        self.stock_info = info
        self.short_code = info.short_code
        self.purchase_amounts = deque()
        self.purchase_qty = deque()
        self.shares = 0
        self.eval_amount = 0
        self.total_amount = 0
        self.realized_profit = 0
        self.profit_rate = 0

    def add_stock(self, qty, price, fee_rate):
        amount = qty * price
        fee = int(amount * fee_rate)
        self.shares += qty
        self.total_amount += (amount + fee)

        # 매수 금액과 수량 기록
        self.purchase_amounts.append(amount + fee)
        self.purchase_qty.append(qty)
        self.update_eval(price)

        return {"amount": amount, "fee": fee}

    def reduce_stock(self, qty, price, fee_rate):
        total_realized_profit = 0
        fee = 0
        real_sell_amount = 0

        if qty <= self.shares:
            remaining_sell_quantity = qty
            real_sell_amount = qty * price

            # 매도할 수량이 남은 매입 수량에서 차감
            while remaining_sell_quantity > 0 and self.purchase_qty:
                if self.purchase_qty[0] <= remaining_sell_quantity:
                    # 매도할 수량이 남은 매수 수량보다 크거나 같은 경우
                    realized_profit = (price * self.purchase_qty[0]) - self.purchase_amounts[0]  # 실현손익 계산
                    total_realized_profit += realized_profit  # 총 실현손익 업데이트
                    self.total_amount -= self.purchase_amounts[0]
                    remaining_sell_quantity -= self.purchase_qty[0]
                    # 매도된 항목을 deque에서 제거
                    self.purchase_amounts.popleft()
                    self.purchase_qty.popleft()
                else:
                    # 일부만 매도할 경우
                    realized_profit = (price * remaining_sell_quantity) - round(
                        (remaining_sell_quantity / self.purchase_qty[0]) * self.purchase_amounts[0])  # 실현손익 계산
                    total_realized_profit += realized_profit  # 총 실현손익 업데이트

                    # 매도한 만큼의 계산 매매시에는 사사오입
                    sell_amount = round(self.purchase_amounts[0] * (remaining_sell_quantity / self.purchase_qty[0]))

                    self.purchase_amounts[0] -= sell_amount

                    self.total_amount -= sell_amount
                    self.purchase_qty[0] -= remaining_sell_quantity
                    remaining_sell_quantity = 0

            fee = round(real_sell_amount * fee_rate)
            self.shares -= qty
            self.realized_profit += total_realized_profit

            self.update_eval(price)

        return {"amount": real_sell_amount, "realized_profit": total_realized_profit, "fee": fee,
                "remaining_shares": self.shares}

    def update_eval(self, price):
        if self.shares > 0:
            self.eval_amount = price * self.shares
            self.profit_rate = round((1 - (self.eval_amount / self.total_amount)) * 100, 2)
        else:
            self.eval_amount = 0
            self.profit_rate = 0
        return {"short_code": self.short_code, "shares": self.shares,
                "eval_amount": self.eval_amount, "profit_rate": self.profit_rate}

    def eval_snapshot(self):
        return {"short_code": self.short_code, "shares": self.shares, "eval_amount": self.eval_amount,
                "profit_rate": self.profit_rate, "total_amount": self.total_amount,
                "realized_profit": self.realized_profit}


class StockTradeInfo:
    def __init__(self, short_code, ohlc_df=None, initial_ratio=20, buy_ratio=20, sell_ratio=20,
                 buy_fee_rate=0.015, sell_fee_rate=0.2, is_first=True):
        self.short_code = short_code
        self.initial_ratio = initial_ratio
        self.buy_ratio = buy_ratio / 100
        self.sell_ratio = sell_ratio / 100
        self.is_first = is_first
        self.buy_fee_rate = buy_fee_rate / 100
        self.sell_fee_rate = sell_fee_rate / 100
        self.ohlc_df = ohlc_df

    @property
    def initial_ratio(self):
        return self._initial_ratio

    @initial_ratio.setter
    def initial_ratio(self, ratio):
        if ratio > 100:
            ratio = 100
        self._initial_ratio = ratio / 100


class TradeManager:
    def __init__(self, initial_investment=100000000):
        self.initial_investment = initial_investment
        self.remaining_cash = initial_investment  # 초기 잔액
        self.total_investment = 0  # 총 매입 금액
        self.total_realized_profit = 0  # 총 실현손익
        self.stock_balance_map = {}
        self.stock_info_map = {}

    def set_stock_info(self, stock_trade_info):
        self.stock_info_map[stock_trade_info.short_code] = stock_trade_info

    def get_stock_info(self, short_code):
        if self.stock_info_map.get(short_code) is None:
            return None
        else:
            return self.stock_info_map[short_code]

    def get_balance(self, short_code, trade_price=0):
        if self.stock_balance_map.get(short_code) is None:  # 잔고 존재 확인
            info = self.stock_info_map.get(short_code)
            if info:  # 잔고 미존재시 종목정보 존재 확인
                self.stock_balance_map[short_code] = StockBalance(info, trade_price)  # 잔고미존재, 종목정보 존재시 잔고 만들기

        return self.stock_balance_map.get(short_code)

    def get_able_buy_qty(self, short_code, trade_price):
        info = self.get_stock_info(short_code)
        if info is None:
            return 0

        # 최초 매수 여부에 따른 수수료 반영 후 최대 amount 계산
        if info.is_first:
            # 초기 비율을 사용하여 amount 계산
            buy_ratio_amount = self.initial_investment * info.initial_ratio
        else:
            # 매수 비율을 사용하여 amount 계산
            buy_ratio_amount = self.initial_investment * info.buy_ratio

        amount = int(buy_ratio_amount / (1 + info.buy_fee_rate))
        # 수수료를 반영한 amount 계산 후 남은 현금보다 큰지 확인
        if amount > self.remaining_cash:
            amount = int(self.remaining_cash / (1 + info.buy_fee_rate))

        # 수수료가 반영된 amount를 사용하여 가능한 최대 qty 계산
        qty = int(amount / trade_price)

        return qty

    def get_able_sell_qty(self, short_code, trade_price):
        balance = self.get_balance(short_code, trade_price)
        info = self.get_stock_info(short_code)
        amount = int(self.initial_investment * info.sell_ratio)

        qty = int(amount / trade_price)
        if qty > balance.shares:  # 남은 잔고보다 더 팔아야하면 잔고만큼
            qty = balance.shares

        return qty

    def buy_stock(self, short_code, trade_price):
        # 매수 수수료 계산
        balance = self.get_balance(short_code, trade_price)
        info = self.get_stock_info(short_code)

        trade_qty = self.get_able_buy_qty(short_code, trade_price)
        if trade_qty <= 0:
            return None

        buy_result = balance.add_stock(trade_qty, trade_price, info.buy_fee_rate)
        fee = buy_result.get('fee')
        amount = buy_result.get('amount')

        # 매수 후 잔액 계산
        self.remaining_cash -= (amount + fee)
        # 총 매입 금액 계산
        self.total_investment += (amount + fee)

        if info.is_first:
            info.is_first = False

        # 결과 반환 (예시)
        return {
            'remaining_cash': self.remaining_cash,
            'total_investment': self.total_investment,
            'total_realized_profit': self.total_realized_profit,
            'qty': trade_qty,
            'fee': buy_result.get('fee')
        }

    def sell_stock(self, short_code, trade_price):
        balance = self.get_balance(short_code, trade_price)
        info = self.get_stock_info(short_code)
        # 매도 가능한 수량 계산
        trade_qty = self.get_able_sell_qty(short_code, trade_price)
        if trade_qty <= 0:
            return None

        sell_result = balance.reduce_stock(trade_qty, trade_price, info.sell_fee_rate)

        # 총 잔고 수량 업데이트
        self.remaining_cash = self.remaining_cash + sell_result['amount'] - sell_result['fee']

        self.total_realized_profit += sell_result['realized_profit']

        # 결과 반환
        return {
            'short_code': short_code,
            'remaining_cash': self.remaining_cash,
            'investment': self.total_investment,
            'realized_profit': self.total_realized_profit,
            'qty': trade_qty,
            'remaining_shares': sell_result['remaining_shares'],
            'fee': sell_result['fee']
        }

    def calc_account_profit_rate(self):
        total_realized_profit = 0

        total_investment = 0
        total_eval_amount = 0
        count = 0
        profit_rate = 0

        for balance in self.stock_balance_map.values():
            profit = balance.eval_snapshot()
            total_realized_profit += profit['realized_profit']
            total_investment += profit['total_amount']
            total_eval_amount += profit['eval_amount']
            count = count + 1

        if count > 0:
            self.total_realized_profit = total_realized_profit
            self.total_investment = total_investment
            profit_rate =((total_eval_amount + self.remaining_cash + total_realized_profit) /
                                  self.initial_investment) - 1 if self.initial_investment != 0 else 0

        return {

            "total_eval_amount": total_eval_amount,
            "total_investment": self.total_investment,
            "total_realized_profit": total_realized_profit,
            "remaining_cash": self.remaining_cash,
            "profit_rate": profit_rate,
            "count": count
        }

    def calc_stock_profit_rate(self, short_code, trade_price):
        balance = self.get_balance(short_code, trade_price)
        if balance is not None:
            profit = balance.update_eval(price=trade_price)
        else:
            return None

        # 평가 금액과 수익률 반환
        return {
            'short_code': profit['short_code'],
            'shares': profit['shares'],
            'eval_amount': profit['eval_amount'],
            'profit_rate': profit['profit_rate']
        }
