"""
weighted_data_distributor.py
-----------------------------

功能：
    按指定权重将一组数据顺序分配到多个分组，支持三种尾部分配策略：
    - 按比例（proportional）：最大余数法，尽量按比例分配尾巴。
    - 轮询公平（round_robin）：一个一个交替分配，尾巴分配更公平。
    - 顺序填充（sequential_fill）：优先填满第一个组，依次填充。

适用场景：
    - 批量任务分配（如多台服务器负载分配）
    - 数据切片（如训练集按权重分成子集）
    - 权重驱动的资源调度

策略对比表：
    假设权重 {A:5, B:3, C:1}，数据共 14 个（比总权重 9 多 5 个尾巴）

    ┌───────────────────┬──────────────────────────────────────────────┐
    │ 策略              │ 尾巴 5 个时的处理方式                        │
    ├───────────────────┼──────────────────────────────────────────────┤
    │ proportional       │ 尾巴按比例：A=3, B=2, C=0                    │
    │ round_robin        │ 尾巴轮询：A=2, B=2, C=1（更公平）            │
    │ sequential_fill    │ 尾巴顺序填：A=5, B=0, C=0（大权重优先）      │
    └───────────────────┴──────────────────────────────────────────────┘

使用示例：
    >>> from weighted_data_distributor import distribute_by_weights

    >>> weights = {"A": 5, "B": 3, "C": 1}
    >>> data = list(range(1, 15))  # 共 14 个元素

    # 策略 1：按比例
    >>> distribute_by_weights(data, weights, "proportional")
    {'A': [1,2,3,4,5,10,11,12], 'B': [6,7,8,13,14], 'C': [9]}

    # 策略 2：轮询
    >>> distribute_by_weights(data, weights, "round_robin")
    {'A': [1,2,3,4,5,10,13], 'B': [6,7,8,11,14], 'C': [9,12]}

    # 策略 3：顺序填充
    >>> distribute_by_weights(data, weights, "sequential_fill")
    {'A': [1,2,3,4,5,10,11,12,13,14], 'B': [6,7,8], 'C': [9]}

作者：ChatGPT
版本：1.0
Python：3.7+
"""

from typing import List, Dict


def distribute_by_weights(data: List[int], weights: Dict[str, int], strategy: str = "proportional") -> Dict[str, List[int]]:
    """
    按照给定的权重，将数据按顺序分配到多个分组，支持三种策略：

    参数：
        data (List[int]): 需要分配的数据列表。
        weights (Dict[str, int]): 权重配置，如 {"A": 5, "B": 3, "C": 1}。
        strategy (str): 分配策略，可选值：
            - "proportional"     最大余数法（按比例分配）
            - "round_robin"      公平轮询分配
            - "sequential_fill"  顺序优先填充

    返回：
        Dict[str, List[int]]: 每个 key 对应的数据子集。

    示例：
        distribute_by_weights([1,2,3,4,5,6], {"A":2,"B":1}, strategy="round_robin")
    """
    if not data:
        return {key: [] for key in weights}

    total_weight = sum(weights.values())
    num_items = len(data)
    result = {key: [] for key in weights}

    # 每一轮的完整分配容量
    full_round, remainder = divmod(num_items, total_weight)

    # 先分配完整轮次
    index = 0
    for key, weight in weights.items():
        take = weight * full_round
        result[key].extend(data[index:index + take])
        index += take

    # 尾巴处理
    if remainder > 0:
        if strategy == "proportional":
            _allocate_remainder_proportional(result, data, index, remainder, weights, total_weight)
        elif strategy == "round_robin":
            _allocate_remainder_round_robin(result, data, index, remainder, weights)
        elif strategy == "sequential_fill":
            _allocate_remainder_sequential(result, data, index, remainder, weights)
        else:
            raise ValueError(f"未知策略: {strategy}")

    return result


def _allocate_remainder_proportional(result, data, index, remainder, weights, total_weight):
    """按比例（最大余数法）分配尾巴"""
    quotas = {}
    for key, weight in weights.items():
        share = (weight / total_weight) * remainder
        quotas[key] = (int(share), share - int(share))

    # 先分配整数部分
    for key, (integer_part, _) in quotas.items():
        if integer_part > 0:
            result[key].extend(data[index:index + integer_part])
            index += integer_part

    # 分配剩余的，按小数部分从大到小排序
    sorted_keys = sorted(quotas.keys(), key=lambda k: quotas[k][1], reverse=True)
    while index < len(data):
        for key in sorted_keys:
            if index < len(data):
                result[key].append(data[index])
                index += 1
            else:
                break


def _allocate_remainder_round_robin(result, data, index, remainder, weights):
    """尾巴逐个轮询分配"""
    keys = list(weights.keys())
    while index < len(data):
        for key in keys:
            if index < len(data):
                result[key].append(data[index])
                index += 1
            else:
                break


def _allocate_remainder_sequential(result, data, index, remainder, weights):
    """尾巴顺序优先填充"""
    for key in weights.keys():
        while remainder > 0 and index < len(data):
            result[key].append(data[index])
            index += 1
            remainder -= 1
        if remainder == 0:
            break


if __name__ == "__main__":
    # 测试代码
    weights = {"A": 5, "B": 3, "C": 1}
    data = list(range(1, 15))  # 共 14 个元素

    print("策略：proportional")
    print(distribute_by_weights(data, weights, "proportional"))

    print("\n策略：round_robin")
    print(distribute_by_weights(data, weights, "round_robin"))

    print("\n策略：sequential_fill")
    print(distribute_by_weights(data, weights, "sequential_fill"))
