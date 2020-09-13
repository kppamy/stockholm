import numpy.random as random


class Solution(object):
    # 26. Remove Duplicates from Sorted Array
    def removeDuplicates(self, nums):
        """
        :type nums: List[int]
        :rtype: int
        """
        size = len(nums)
        if size <= 1:
            return size
        slow = 0
        for fast in range(1, size):
            if nums[fast] > nums[fast - 1]:
                slow = slow + 1
                nums[slow] = nums[fast]
        return slow + 1

    # 11. Container With Most Water
    def maxArea(self, height):
        """
        :type height: List[int]
        :rtype: int
        """
        left = 0
        right = len(height) - 1
        are = lambda x, y: (y - x) * min(height[y], height[x])
        maxa = 0
        while left < right:
            maxa = max(maxa, are(left, right))
            if height[left] < height[right]:
                left = left + 1
            else:
                right = right - 1
        return maxa

    def generate_random_array(self):
        arr0 = [1, 8, 6, 2, 5, 4, 8, 3, 7]
        arr1 = [1, 2, 3]
        arr2 = [3, 2, 1]
        arr3 = random.random_integers(-1, 10, 6)
        return [arr0, arr3, arr1, arr2]

    def format_test_area(self, func):
        cases = self.generate_random_array()
        for case in cases:
            print(case)
            size = func(case)
            print(func.__name__ + " res: " + str(size))

    def format_test(self):
        cases = self.generate_sort()
        for case in cases:
            print(case)
            size = self.removeDuplicates(case)
            print("after remove, len: ", str(size))
            print(case)

    def generate_sortedArray(self):
        nums5 = [1, 1, 2, 3]
        nums = [1, 1, 2]
        nums1 = [0, 0, 1, 1, 1, 2, 2, 3, 3, 4]
        nums2 = [0, 0]
        nums3 = []
        nums4 = [1]
        return [nums5, nums, nums1, nums2, nums3, nums4]


if __name__ == '__main__':
    solution = Solution()
    # solution.format_test()
    solution.format_test_area(solution.maxArea)
