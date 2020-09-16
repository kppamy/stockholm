from typing import List
from common import *


class Sort:
    # 349. Intersection of Two Arrays
    def intersectionIntenal(self, nums1: List[int], nums2: List[int]) -> List[int]:
        #  sort 2
        #  remove duplicate 2
        if nums1 == [] or nums2 == []:
            return []
        if nums1 is None or nums2 is None:
            return []
        r1 = list(set(nums1))
        r1.sort()
        r2 = list(set(nums2))
        r2.sort()
        r1.extend(r2)
        r1.sort()
        return list(set([item for item in r1 if r1.count(item) == 2]))

    # 349. Intersection of Two Arrays
    # Runtime: 60 ms, faster than 37.50% of Python3 online submissions for Intersection of Two Arrays.
    # Memory Usage: 13.8 MB, less than 93.37% of Python3 online submissions for Intersection of Two Arrays.
    def intersectionH(self, nums1: List[int], nums2: List[int]) -> List[int]:
        # after remove empty check:
        # Runtime: 60ms-->48 ms, 37.5%--> 65.59%
        # if nums1 == [] or nums2 == []:
        #     return []
        # if nums1 is None or nums2 is None:
        #     return []
        dc = {val: i for i, val in enumerate(nums1)}
        dupl = [item for item in nums2 if dc.get(item) is not None]
        res = []
        [res.extend([item]) for item in dupl if item not in res]
        return res

    # 349. Intersection of Two Arrays
    # Runtime: 56 ms, faster than 41.69% of Python3 online submissions for Intersection of Two Arrays.
    # Memory Usage: 14 MB, less than 46.13% of Python3 online submissions for Intersection of Two Arrays.
    def intersection(self, nums1: List[int], nums2: List[int]) -> List[int]:
        dc = {val: 1 for val in nums1}
        for item in nums2:
            if dc.get(item) is not None and dc[item] == 1:
                dc[item] = dc[item] + 1
        return [key for key in dc if dc[key] == 2]

    # 1122. Relative Sort Array
    # Runtime: 44 ms, faster than 52.86% of Python3 online submissions for Relative Sort Array.
    # Memory Usage: 13.7 MB, less than 96.16% of Python3 online submissions for Relative Sort Array.
    def relativeSortArrayHash(self, arr1: List[int], arr2: List[int]) -> List[int]:
        dc = {}
        for item in arr1:
            if item in dc:
                dc[item] = dc[item] + 1
            else:
                dc[item] = 1
        res = []
        for item in arr2:
            res.extend([item] * dc[item])
            del (dc[item])
        keys = sorted(dc)
        # this is add list instead of items original list
        # res.extend([item] * dc[item] for item in keys)
        for item in keys:
            res.extend([item] * dc[item])
        return res

    # 1122. Relative Sort Array
    # Runtime: 48 ms, faster than 44.30% of Python3 online submissions for Relative Sort Array.
    # Memory Usage: 13.8 MB, less than 89.66% of Python3 online submissions for Relative Sort Array.
    def relativeSortArray(self, arr1: List[int], arr2: List[int]) -> List[int]:
        if not bool(arr1):
            return []
        maxv = max(arr1)
        counter = [0] * (maxv + 1)
        for item in arr1:
            counter[item] = counter[item] + 1
        res = []
        for item in arr2:
            res.extend([item] * counter[item])
            counter[item] = 0
        for i, val in enumerate(counter):
            if val > 0:
                res.extend([i] * val)
        return res

    def generate_relative_sort_arr(self):
        arr1 = [2, 3, 1, 3, 2, 4, 6, 7, 9, 2, 19]
        arr2 = [2, 1, 4, 3, 9, 6]
        cases = [[[], []], [arr1, []], [arr1, arr2]]
        return cases

    def generate_two_arr(self):
        nums1 = [1, 2, 2, 1]
        nums2 = [2, 2]

        nums3 = [4, 9, 5]
        nums4 = [9, 4, 9, 8, 4]

        cases = [[[], []], [nums1, nums2], [nums3, nums4]]
        for i in range(3):
            n1 = random.random_integers(-1, 10, 4)
            n2 = random.random_integers(0, 11, 6)
            cases.append([n1, n2])
        return cases


if __name__ == '__main__':
    sort = Sort()
    # format_test(sort.intersection, sort.generate_two_arr)
    format_test(sort.relativeSortArray, sort.generate_relative_sort_arr)
