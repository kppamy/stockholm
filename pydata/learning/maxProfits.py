import sys
import numpy.random as random
from functools import wraps


def decorate_test(func):
    @wraps(func)
    def decrated(*args, **kwargs):
        print("decorated")
        return func(*args, **kwargs)
    return decrated


class Solution(object):
    def generate_inputs(self):
        prices7 = [2, 1, 2, 0, 1]
        prices5 = [6, 1, 3, 2, 4, 7]
        prices3 = random.random_integers(2, 10, 6)
        prices6 = random.random_integers(2, 10, 6)
        prices4 = [1, 2, 3, 4, 5]
        prices1 = [7, 1, 5, 3, 6, 4]
        prices2 = [7, 6, 4, 3, 1]
        return [prices7, prices5, prices3, prices6, prices4, prices1, prices2]

    def print_results(self, prf):
        print("max profit is: ", str(prf), "\n")

    def format_test(self, func):
        cases = self.generate_inputs()
        for case in cases:
            print(case)
            res = func(case)
            self.print_results(res)

    # 121. Best Time to Buy and Sell Stock
    def maxProfitOneTime(self, prices):
        """
        :type prices: List[int]
        :rtype: int
        """
        minp = sys.maxsize
        prf = 0
        for i, price in enumerate(prices):
            if price < minp:
                minp = price
            elif price - minp > prf:
                prf = price - minp
        return prf

    # 122. Best Time to Buy and Sell Stock II
    def maxProfitMultipleTimes(self, prices):
        """
        :type prices: List[int]
        :rtype: int
        """
        maxp = 0
        for i, price in enumerate(prices):
            if i + 1 < len(prices) and prices[i + 1] - prices[i] > 0:
                maxp = maxp + prices[i + 1] - prices[i]
        return maxp

    def run(self):
        self.format_test(self.maxProfitMultipleTimes)


if __name__ == '__main__':
    solu = Solution()
    solu.run()
