from collections import deque


class TradeManager:
    def __init__(self, initial_investment=100000000, initial_ratio=0.2, buy_ratio=0.2, sell_ratio=0.2,
                 buy_fee_rate=0.015, sell_fee_rate=0.2):
        self.is_first = True
        self.initial_investment = initial_investment
        self.initial_ratio = initial_ratio
        self.buy_ratio = buy_ratio
        self.sell_ratio = sell_ratio
        self.buy_fee_rate = buy_fee_rate  # 매수 수수료율
        self.remaining_cash = initial_investment  # 초기 잔액
        self.sell_fee_rate = sell_fee_rate
        self.total_investment = 0  # 총 매입 금액
        self.total_shares = 0  # 총 보유 주식 수량
        self.total_realized_profit = 0  # 총 실현손익
        self.purchase_amounts = deque()  # 매수 금액 기록 (deque 사용)
        self.purchase_quantities = deque()  # 매수 수량 기록 (deque 사용)
        self.buy_and_hold_shares = 0
        self.buy_and_hold_remaining_cash = 0

    def init_buy_and_hold(self, trade_price):
        qty = self.get_able_buy_and_hold_qty(trade_price)
        self.buy_and_hold_shares = qty
        self.buy_and_hold_remaining_cash = self.initial_investment - (trade_price * qty)

    def calc_buy_and_hold(self, trade_price):
        eval_amount = self.buy_and_hold_shares * trade_price + self.buy_and_hold_remaining_cash
        profit = eval_amount / self.initial_investment

        return {
            'profit_rate': profit,
            'eval_amount': eval_amount,
        }

    def get_able_buy_and_hold_qty(self, trade_price):
        qty = int(self.initial_investment / trade_price)
        return qty

    def get_able_buy_qty(self, trade_price):
        if self.is_first:
            # 최초 매수
            amount = int(self.initial_investment * self.initial_ratio)
        else:
            amount = int(self.initial_investment * self.buy_ratio)

        qty = int(amount / trade_price)

        return qty

    def get_able_sell_qty(self, trade_price):
        amount = int(self.initial_investment * self.sell_ratio)
        qty = int(amount / trade_price)
        if qty > self.total_shares:
            qty = self.total_shares

        return qty

    def buy_stock(self, trade_price):
        # 매수 수수료 계산
        trade_qty = self.get_able_buy_qty(trade_price)
        if trade_qty <= 0:
            return None

        trade_real_amount = trade_qty * trade_price

        buy_fee = trade_real_amount * self.buy_fee_rate

        trade_real_amount = trade_real_amount + buy_fee

        # 매수 후 잔액 계산
        self.remaining_cash -= trade_real_amount
        # 총 매입 금액 계산
        self.total_investment += trade_real_amount
        # 총 잔고 수량 업데이트
        self.total_shares += trade_qty

        # 매수 금액과 수량 기록
        self.purchase_amounts.append(trade_real_amount)
        self.purchase_quantities.append(trade_qty)

        if self.is_first:
            self.is_first = False

        # 결과 반환 (예시)
        return {
            'remaining_cash': self.remaining_cash,
            'total_investment': self.total_investment,
            'total_shares': self.total_shares,
            'purchase_amounts': self.purchase_amounts,
            'purchase_quantities': self.purchase_quantities
        }

    def sell_stock(self, trade_price):
        # 매도 가능한 수량 계산
        trade_qty = self.get_able_sell_qty(trade_price)
        if trade_qty <= 0:
            return None

        remaining_sell_quantity = trade_qty
        sell_index = 0
        # 매도할 수량이 남은 매입 수량에서 차감
        while remaining_sell_quantity > 0 and sell_index < len(self.purchase_quantities):
            if self.purchase_quantities[sell_index] <= remaining_sell_quantity:
                # 매도할 수량이 남은 매수 수량보다 크거나 같은 경우
                realized_profit = (trade_price * self.purchase_quantities[sell_index]) - self.purchase_amounts[
                    sell_index]  # 실현손익 계산
                self.total_realized_profit += realized_profit  # 총 실현손익 업데이트
                self.total_investment -= self.purchase_amounts[sell_index]
                remaining_sell_quantity -= self.purchase_quantities[sell_index]
                # 매도된 항목을 deque에서 제거
                self.purchase_amounts.popleft()
                self.purchase_quantities.popleft()
            else:
                # 일부만 매도할 경우
                realized_profit = (trade_price * remaining_sell_quantity) - (
                        remaining_sell_quantity / self.purchase_quantities[sell_index]) * self.purchase_amounts[
                                      sell_index]  # 실현손익 계산
                self.total_realized_profit += realized_profit  # 총 실현손익 업데이트

                # 매도한 만큼의 금액 계산
                sell_amount = self.purchase_amounts[sell_index] * (
                        remaining_sell_quantity / self.purchase_quantities[sell_index])

                self.purchase_amounts[sell_index] -= sell_amount
                self.total_investment -= sell_amount
                self.purchase_quantities[sell_index] -= remaining_sell_quantity
                remaining_sell_quantity = 0

            sell_index += 1

        # 총 잔고 수량 업데이트
        self.total_shares -= trade_qty

        # 결과 반환
        return {
            'remaining_cash': self.remaining_cash,
            'total_investment': self.total_investment,
            'total_shares': self.total_shares,
            'total_realized_profit': self.total_realized_profit,
            'purchase_amounts': list(self.purchase_amounts),
            'purchase_quantities': list(self.purchase_quantities)
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
            'profit_rate': profit_rate
        }
