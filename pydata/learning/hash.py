from common import format_test
from typing import List


class Solution:
    # 1. Two Sum
    def twoSum(self, nums: List[int], target: int) -> List[int]:
        hm = {}
        for i, val in enumerate(nums):
            other = hm.get(target - val)
            if other is not None:
                return [other, i]
            hm[val] = i

    def generate_inputs(self):
        nums = [-3, 4, 3, 90]
        target = 0
        s00 = [nums, target]

        nums = [0, 4, 3, 0]
        target = 0
        s0 = [nums, target]

        nums = [2, 7, 11, 15]
        target = 9
        s1 = [nums, target]

        nums = [3, 2, 4]
        target = 6
        s2 = [nums, target]

        nums = [3, 3]
        target = 6
        s3 = [nums, target]
        return [s00, s0, s1, s2, s3]


if __name__ == '__main__':
    solu = Solution()
    format_test(solu.twoSum, solu.generate_inputs)
