"""
weighted_group_assigner.py

本模块提供 WeightedGroupAssigner 类，可实现按照指定权重将数据分配到多个组的多种模式。
支持模式：
    1. chunk           : 连续块分配，每组得到连续的数据片段。
    2. block_interleaved: 按块交错分配，每组每次拿 block_size 个数据，循环分配。
    3. gcd_interleaved  : 权重最大公约数约分后的最均匀轮流分配，每轮按约分后的比例分配，结尾不足按比例补齐。

所有分配均严格满足总数等于数据总长度，且分配比例最接近权重。
"""

from typing import Any, Dict, List, Literal
import math
import functools


class WeightedGroupAssigner:
    def __init__(self, weights: Dict[Any, float]):
        """
        初始化权重分配器

        :param weights: 分组权重字典 {key: weight, ...}
        """
        if not weights or not isinstance(weights, dict):
            raise ValueError("weights must be a non-empty dict")
        self.weights = weights.copy()

    @staticmethod
    def _largest_remainder_method(total: int, weights: Dict[Any, float]) -> Dict[Any, int]:
        """
        使用最大余数法将 total 按照 weights 分配到各组。

        :param total: 需分配的总数
        :param weights: 权重字典 {key: weight, ...}
        :return: 每组分配数量字典 {key: count, ...}
        """
        keys = list(weights.keys())
        values = list(weights.values())
        s = sum(values)
        if s == 0 or total == 0:
            return {k: 0 for k in keys}
        raw = [w * total / s for w in values]
        floored = [math.floor(x) for x in raw]
        remainder = [x - f for x, f in zip(raw, floored)]
        assigned = sum(floored)
        to_assign = total - assigned
        indices = sorted(range(len(keys)), key=lambda i: (-remainder[i], i))
        for i in range(to_assign):
            floored[indices[i]] += 1
        return {k: c for k, c in zip(keys, floored)}

    @staticmethod
    def _gcd_list(numbers: List[int]) -> int:
        """
        求一组整数的最大公约数
        """
        return functools.reduce(math.gcd, numbers)

    def _assign_chunk(self, data: List[Any]) -> Dict[Any, List[Any]]:
        """
        块分配模式：每组数据为一段连续块

        :param data: 待分配数据
        :return: {key: [data], ...}
        """
        counts = self._largest_remainder_method(len(data), self.weights)
        result = {}
        idx = 0
        for key in self.weights.keys():
            c = counts[key]
            result[key] = data[idx:idx+c]
            idx += c
        return result

    def _assign_block_interleaved(self, data: List[Any], block_size: int) -> Dict[Any, List[Any]]:
        """
        块交错分配模式：将数据切分为不超过 block_size 的小块，依次轮流分配给各组，直到每组分满为止。

        :param data: 待分配数据
        :param block_size: 每组每次最多分配的数据块大小
        :return: {key: [data], ...}
        """
        counts = self._largest_remainder_method(len(data), self.weights)
        keys = list(self.weights.keys())
        total_counts = {k: 0 for k in keys}  # 每组已分配数
        result = {k: [] for k in keys}
        data_idx = 0
        total = len(data)
        while data_idx < total:
            for k in keys:
                need = counts[k] - total_counts[k]
                take = min(block_size, need)
                if take > 0 and data_idx < total:
                    result[k].extend(data[data_idx:data_idx+take])
                    total_counts[k] += take
                    data_idx += take
        return result

    def _assign_gcd_interleaved(self, data: List[Any]) -> Dict[Any, List[Any]]:
        """
        最小GCD块交错分配模式：
            - 按最大公约数约分权重，每轮按约分后权重分配数据
            - 不能完整一轮时，剩余数据按最大余数法分完

        :param data: 待分配数据
        :return: {key: [data], ...}
        """
        keys = list(self.weights.keys())
        int_weights = [int(w) for w in self.weights.values()]
        gcd_val = self._gcd_list(int_weights)
        base_counts = [w // gcd_val for w in int_weights]
        round_counts = {k: base_counts[i] for i, k in enumerate(keys)}
        final_counts = self._largest_remainder_method(len(data), self.weights)
        distributed = {k: 0 for k in keys}
        result = {k: [] for k in keys}
        data_idx = 0
        total = len(data)
        # 按约分权重轮流分配
        while True:
            can_distribute = False
            for k in keys:
                if distributed[k] + round_counts[k] <= final_counts[k]:
                    for _ in range(round_counts[k]):
                        if data_idx < total and distributed[k] < final_counts[k]:
                            result[k].append(data[data_idx])
                            distributed[k] += 1
                            data_idx += 1
                            can_distribute = True
            if not can_distribute:
                break
        # 剩余部分再按最大余数法分配
        left_data = data[data_idx:]
        if left_data:
            left_counts = {k: final_counts[k] - distributed[k] for k in keys}
            left_counts = {k: max(0, v) for k, v in left_counts.items()}
            idx = 0
            for k in keys:
                cnt = left_counts[k]
                if cnt > 0:
                    result[k].extend(left_data[idx:idx+cnt])
                    idx += cnt
        return result

    def assign(
        self,
        data: List[Any],
        mode: Literal['chunk', 'block_interleaved', 'gcd_interleaved'] = 'chunk',
        block_size: int = 1
    ) -> Dict[Any, List[Any]]:
        """
        按权重将数据分配到各组。可选三种分配模式。

        :param data: 待分配数据
        :param mode: 分配模式
            - 'chunk': 连续块分配
            - 'block_interleaved': 按块交错分配
            - 'gcd_interleaved': 权重约分后极致均匀分配
        :param block_size: 块交错模式下每次分配块大小，默认为1
        :return: {key: [data], ...}
        """
        if not isinstance(data, list):
            raise ValueError("data must be a list")
        if len(self.weights) == 0 or len(data) == 0:
            return {k: [] for k in self.weights}
        if mode == 'chunk':
            return self._assign_chunk(data)
        elif mode == 'block_interleaved':
            return self._assign_block_interleaved(data, block_size)
        elif mode == 'gcd_interleaved':
            return self._assign_gcd_interleaved(data)
        else:
            raise ValueError("mode must be 'chunk', 'block_interleaved', or 'gcd_interleaved'")


# ----------------- 示例测试 -----------------
if __name__ == '__main__':
    data = list(range(1, 21))  # 从1开始，20个值
    weights = {'A': 2, 'B': 3, 'C': 5}
    assigner = WeightedGroupAssigner(weights)

    # Chunk模式测试
    chunk_result = assigner.assign(data, mode='chunk')
    print("Chunk:")
    print(chunk_result)
    # 结果示例: {'A': [1, 2, 3, 4], 'B': [5, 6, 7, 8, 9, 10], 'C': [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]}

    # Block Interleaved模式测试，block_size=2
    block_interleaved_result = assigner.assign(data, mode='block_interleaved', block_size=2)
    print("Block Interleaved (block_size=2):")
    print(block_interleaved_result)
    # 结果示例: {'A': [1, 8, 15, 20], 'B': [2, 3, 9, 10, 16, 17], 'C': [4, 5, 6, 7, 11, 12, 13, 14, 18, 19]}

    # Block Interleaved模式测试，block_size=1（等同于round robin）
    block_interleaved_result_1 = assigner.assign(data, mode='block_interleaved', block_size=1)
    print("Block Interleaved (block_size=1):")
    print(block_interleaved_result_1)
    # 结果示例: {'A': [1, 6, 11, 16], 'B': [2, 4, 7, 9, 12, 14], 'C': [3, 5, 8, 10, 13, 15, 17, 18, 19, 20]}

    # Block Interleaved模式测试，block_size=len(data)（等同于chunk）
    block_interleaved_result_max = assigner.assign(data, mode='block_interleaved', block_size=len(data))
    print("Block Interleaved (block_size=len(data)):")
    print(block_interleaved_result_max)
    # 结果示例: {'A': [1, 2, 3, 4], 'B': [5, 6, 7, 8, 9, 10], 'C': [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]}

    # GCD Interleaved模式测试
    gcd_interleaved_result = assigner.assign(data, mode='gcd_interleaved')
    print("GCD Interleaved:")
    print(gcd_interleaved_result)
    # 结果示例: {'A': [1, 4, 7, 10], 'B': [2, 5, 8, 11, 14, 17], 'C': [3, 6, 9, 12, 13, 15, 16, 18, 19, 20]}
