from typing import List

from common import *
import numpy.random as random
from functools import reduce


# Definition for a binary tree node.
class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right


class Tree:
    def buildNode(self, nums, start, end):
        if start == end:
            return TreeNode(nums[start])
        mid = (end + start) // 2
        root = TreeNode(nums[mid])
        if start <= mid - 1:
            left = self.buildNode(nums, start, mid - 1)
            root.left = left
        if mid + 1 <= end:
            right = self.buildNode(nums, mid + 1, end)
            root.right = right
        return root

    # 108. Convert Sorted Array to Binary Search Tree
    def sortedArrayToBST(self, nums: List[int]) -> TreeNode:
        return self.buildNode(nums, 0, len(nums) - 1)

    def generate_sortedArray(self):
        arr0 = [-3, -2, -1, 3, 4, 5, 7, 8]
        arr1 = [-10, -3, 0, 5, 9]
        arr2 = sorted(random.random_integers(-3, 10, 8))
        arr3 = sorted(random.random_integers(-8, 10, 8))
        arr4 = [
            [1, 1, -1, -1, 1],
            [1, 2, 2, 3, -1, -1, 3, 4, -1, -1, 4],
            [3, 9, 20, -1, -1, 15, 7],
            [2, 1, 3],
            [1, 2, 2, 3, 3, -1, -1, 4, 4],
            [10, 5, 15, -1, -1, 6, 20]
        ]
        cases = [arr0, arr1, arr2, arr3]
        cases.extend(arr4)
        return cases

    # 655. Print Binary Tree
    def printTree(self, root: TreeNode) -> List[List[str]]:
        height = self.getHeight(root)
        wid = 2 ** height - 1
        nodes = [['   ' for col in range(wid)] for row in range(height)]
        self.fill(root, nodes, 0, wid - 1, 1)
        rows = [reduce(lambda x, y: x + y, nodes[i]) for i in range(height)]
        for i, item in enumerate(rows):
            print(item,sep=None)
        return nodes

    def fill(self, root: TreeNode, nodes: List[List[str]], start: int, end: int, height: int):
        if root is None:
            return
        if start == end:
            nodes[height - 1][start] = str(root.val)
            return
        pos = (start + end) // 2
        nodes[height - 1][pos] = str(root.val)
        if pos - 1 >= 0:
            self.fill(root.left, nodes, 0, pos - 1, height + 1)
        if pos + 1 <= end:
            self.fill(root.right, nodes, pos + 1, end, height + 1)

    # 104. Maximum Depth of Binary Tree
    def getHeight(self, root: TreeNode) -> int:
        if not root:
            return 0
        left = 0
        if root.left:
            left = self.getHeight(root.left)
        right = 0
        if root.right:
            right = self.getHeight(root.right)
        return max(left, right) + 1

    def format_test(self, func, print_func=None):
        cases = self.generate_sortedArray()
        for case in cases:
            print(case)
            res = func(case)
            print(func.__name__ + ":")
            if print_func is not None:
                print_func(res)
            else:
                if isinstance(res, int):
                    print(str(res))
                else:
                    print(res)


if __name__ == '__main__':
    tree = Tree()
    tree.format_test(tree.sortedArrayToBST, tree.printTree)
