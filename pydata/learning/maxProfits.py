import sys
import numpy.random as random


class Solution(object):
    # 121. Best Time to Buy and Sell Stock
    def maxProfit(self, prices):
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

    def testMaxProfit(self):
        prices1 = [7, 1, 5, 3, 6, 4]
        print(prices1)
        prf = self.maxProfit(prices1)
        print("max profit is: ", str(prf), "\n")

        prices2 = [7, 6, 4, 3, 1]
        print(prices2)
        prf = self.maxProfit(prices2)
        print("max profit is: ", str(prf), "\n")

        prices3 = random.random_integers(2, 10, 6)
        print(prices3)
        prf = self.maxProfit(prices3)
        print("max profit is: ", str(prf), "\n")

    def run(self):
        self.testMaxProfit()


if __name__ == '__main__':
    solu = Solution()
    solu.run()
