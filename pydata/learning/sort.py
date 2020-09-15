from typing import List
from common import *


class Sort:
    # 349. Intersection of Two Arrays
    def intersection(self, nums1: List[int], nums2: List[int]) -> List[int]:
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
    format_test(sort.intersection, sort.generate_two_arr)
