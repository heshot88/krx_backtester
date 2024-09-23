from collections import deque


class StockBalance:
    def __init__(self, short_code, trade_price):
        self.price = trade_price
        self.short_code = short_code
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
        self.total_amount += amount

        # 매수 금액과 수량 기록
        self.purchase_amounts.append(amount)
        self.purchase_qty.append(qty)
        self.update_eval(price)

        return {"amount": amount, "fee": fee}

    def reduce_stock(self, qty, price, fee_rate):
        if qty > self.shares:
            return None

        remaining_sell_quantity = qty
        total_realized_profit = 0
        total_sell_amount = 0
        sell_index = 0
        # 매도할 수량이 남은 매입 수량에서 차감
        while remaining_sell_quantity > 0 and sell_index < len(self.purchase_qty):
            if self.purchase_qty[sell_index] <= remaining_sell_quantity:
                # 매도할 수량이 남은 매수 수량보다 크거나 같은 경우
                realized_profit = (price * self.purchase_qty[sell_index]) - self.purchase_amounts[
                    sell_index]  # 실현손익 계산
                total_realized_profit += realized_profit  # 총 실현손익 업데이트
                total_sell_amount += self.purchase_amounts[sell_index]
                remaining_sell_quantity -= self.purchase_qty[sell_index]
                # 매도된 항목을 deque에서 제거
                self.purchase_amounts.popleft()
                self.purchase_qty.popleft()
            else:
                # 일부만 매도할 경우
                realized_profit = (price * remaining_sell_quantity) - (
                        remaining_sell_quantity / self.purchase_qty[sell_index]) * self.purchase_amounts[
                                      sell_index]  # 실현손익 계산
                total_realized_profit += realized_profit  # 총 실현손익 업데이트

                # 매도한 만큼의 금액 계산
                sell_amount = self.purchase_amounts[sell_index] * (
                        remaining_sell_quantity / self.purchase_qty[sell_index])

                self.purchase_amounts[sell_index] -= sell_amount
                total_sell_amount += sell_amount
                self.purchase_qty[sell_index] -= remaining_sell_quantity
                remaining_sell_quantity = 0

            sell_index += 1

        fee = total_sell_amount * fee_rate
        self.realized_profit += total_realized_profit
        self.total_amount -= (total_sell_amount + fee)
        self.update_eval(price)

        return {"amount": total_sell_amount, "realized_profit": total_realized_profit, "fee": fee}

    def update_eval(self, price):
        self.eval_amount = price * self.shares
        self.profit_rate = (self.eval_amount / self.total_amount) * 100
        return {'eval_amount': self.eval_amount, 'profit_rate': self.profit_rate}


class StockTradeInfo:
    def __init__(self, short_code, name="", initial_ratio=0.2, buy_ratio=0.2, sell_ratio=0.2, is_first=True):
        self.short_code = short_code
        self.name = name
        self.initial_ratio = initial_ratio
        self.buy_ratio = buy_ratio
        self.sell_ratio = sell_ratio
        self.is_first = is_first


class TradeManager:

    def __init__(self, initial_investment=100000000,
                 buy_fee_rate=0.015, sell_fee_rate=0.2):
        self.is_first = True
        self.initial_investment = initial_investment

        self.buy_fee_rate = buy_fee_rate / 100  # 매수 수수료율
        self.remaining_cash = initial_investment  # 초기 잔액
        self.sell_fee_rate = sell_fee_rate / 100
        self.total_investment = 0  # 총 매입 금액
        self.total_shares = 0  # 총 보유 주식 수량
        self.total_realized_profit = 0  # 총 실현손익
        self.stock_balance_map = {}
        self.stock_info_map = {}

    def set_stock_info(self, short_code, name="", initial_ratio=0.2, buy_ratio=0.2, sell_ratio=0.2):
        self.stock_info_map[short_code] = StockTradeInfo(short_code, name, initial_ratio, buy_ratio, sell_ratio)

    def get_stock_info(self, short_code):
        if self.stock_info_map[short_code] is None:
            return None
        else:
            return self.stock_info_map[short_code]

    def get_balance(self, short_code, trade_price):
        if self.stock_balance_map[short_code] is None:
            self.stock_balance_map[short_code] = StockBalance(short_code, trade_price)

        return self.stock_balance_map[short_code]

    def get_able_buy_qty(self, short_code, trade_price):
        info = self.get_stock_info(short_code)
        if info is None:
            return 0

        if info.is_first:
            # 최초 매수
            amount = int(self.initial_investment * info.initial_ratio)
        else:
            amount = int(self.initial_investment * info.buy_ratio)

        qty = int(amount / trade_price)

        return qty

    def get_able_sell_qty(self, short_code, trade_price):
        info = self.get_stock_info(short_code)
        amount = int(self.initial_investment * info.sell_ratio)
        qty = int(amount / trade_price)
        if qty > self.total_shares:
            qty = self.total_shares

        return qty

    def buy_stock(self, short_code, trade_price):
        # 매수 수수료 계산
        balance = self.get_balance(short_code, trade_price)
        info = self.get_stock_info(short_code)

        trade_qty = self.get_able_buy_qty(short_code, trade_price)
        if trade_qty <= 0:
            return None

        buy_result = balance.add_stock(trade_qty, trade_price, self.buy_fee_rate)

        # 매수 후 잔액 계산
        self.remaining_cash -= buy_result.get('amount')
        # 총 매입 금액 계산
        self.total_investment += buy_result.get('amount')
        # 총 잔고 수량 업데이트
        self.total_shares += trade_qty

        if info.is_first:
            info.is_first = False

        # 결과 반환 (예시)
        return {
            'remaining_cash': self.remaining_cash,
            'total_investment': self.total_investment,
            'total_shares': self.total_shares,
            'total_realized_profit': self.total_realized_profit,
            'fee': buy_result.get('fee')
        }

    def sell_stock(self, short_code, trade_price):
        balance = self.get_balance(short_code, trade_price)
        info = self.get_stock_info(short_code)
        # 매도 가능한 수량 계산
        trade_qty = self.get_able_sell_qty(trade_price)
        if trade_qty <= 0:
            return None

        sell_result = balance.reduce_stock(trade_qty,trade_price,self.sell_fee_rate)




        # 총 잔고 수량 업데이트
        self.total_shares -= trade_qty
        sell_fee = int(trade_qty * trade_price * self.sell_fee_rate)
        self.remaining_cash = self.remaining_cash + (trade_qty * trade_price) - sell_fee

        # 결과 반환
        return {
            'remaining_cash': self.remaining_cash,
            'total_investment': self.total_investment,
            'total_shares': self.total_shares,
            'total_realized_profit': self.total_realized_profit,
            'fee': sell_fee
        }

    def calc_profit_rate(self, trade_price):

        # 현재 평가 금액 계산 (보유한 주식 수량 * 현재 가격)
        eval_amount = self.total_shares * trade_price

        # 수익률 계산 (실현손익 + 미실현 손익을 포함한 수익률)
        if self.initial_investment != 0:
            profit_rate = ((eval_amount + self.remaining_cash) / self.initial_investment) - 1
        else:
            profit_rate = 0

        # 평가 금액과 수익률 반환
        return {
            'eval_amount': eval_amount,
            'profit_rate': profit_rate,
            'realized_profit': self.total_realized_profit
        }
