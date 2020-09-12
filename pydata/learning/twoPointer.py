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

    def format_test(self):
        cases = self.generate_sortedArray()
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
    solution.format_test()
