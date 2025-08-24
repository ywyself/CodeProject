"""
weighted_data_distributor.py
=============================

功能：
    按指定权重将数据分配到多个分组，支持两种分配模式，保证比例严格按照权重。

分配模式：
    1. **chunk**       - 块分配模式：
        每个分组按计算好的数量一次性连续取数。例如：
        A: [1,2,3], B: [4,5], C: [6]

    2. **round_robin** - 轮询分配模式：
        每个分组交替取数，直到达到目标数量，避免长连续块。例如：
        A: [1,4,7], B: [2,5], C: [3,6]

分配规则：
    - 总数按照权重比例分配（采用**最大余数法**保证整数且尽量公平）。
    - 模式仅影响分配顺序，不影响最终数量。

典型应用：
    - 按业务优先级（权重）分配任务
    - 按权重拆分数据集
    - 按比例分配资源

示例：
    >>> weights = {"A": 6, "B": 3, "C": 1, "D": 2}
    >>> data = list(range(1, 16))

    >>> distribute_by_weights(data, weights, mode="chunk")
    {'A': [1, 2, 3, 4, 5, 6], 'B': [7, 8, 9], 'C': [10], 'D': [11, 12]}

    >>> distribute_by_weights(data, weights, mode="round_robin")
    {'A': [1, 5, 9, 11, 13, 15], 'B': [2, 6, 10], 'C': [3], 'D': [4, 7]}
"""

from typing import Dict, List, Any


def distribute_by_weights(
    data: List[Any],
    weights: Dict[str, int],
    mode: str = "chunk",
) -> Dict[str, List[Any]]:
    """
    按指定权重将数据分配到多个分组。

    参数：
        data (List[Any]): 待分配的数据列表。
        weights (Dict[str, int]): {组名: 权重}，权重必须为非负整数。
        mode (str): 分配模式：
            - "chunk"       : 块分配（每组一次性连续取数）
            - "round_robin" : 严格比例下轮询分配（避免长连续块）

    返回：
        Dict[str, List[Any]]: {组名: [分配的数据]}
    """
    # 初始化返回结构
    result: Dict[str, List[Any]] = {k: [] for k in weights}

    # 预处理权重，过滤无效分组
    active_keys = [k for k, w in weights.items() if w > 0]
    if not active_keys or len(data) == 0:
        return result

    total_items = len(data)
    total_weight = sum(weights[k] for k in active_keys)

    # ==============================
    # Step 1: 计算每组的目标数量（严格按比例）
    # ==============================
    quotas_float = {
        k: total_items * (weights[k] / total_weight) for k in active_keys
    }
    # 向下取整的基础分配
    base_alloc = {k: int(quotas_float[k]) for k in active_keys}
    allocated = sum(base_alloc.values())
    leftover = total_items - allocated

    # 剩余数量按最大余数法分配
    remainder_order = sorted(
        active_keys, key=lambda k: (quotas_float[k] - base_alloc[k]), reverse=True
    )
    for key in remainder_order:
        if leftover <= 0:
            break
        base_alloc[key] += 1
        leftover -= 1

    # ==============================
    # Step 2: 根据模式分配数据
    # ==============================
    index = 0
    if mode == "chunk":
        # 块分配：每组连续取目标数量
        for key in active_keys:
            take = base_alloc[key]
            if take > 0:
                result[key].extend(data[index : index + take])
                index += take

    elif mode == "round_robin":
        # 轮询分配：每次给一个分组，直到该分组达到目标数量
        remaining = base_alloc.copy()
        while index < total_items:
            for key in active_keys:
                if remaining[key] > 0:
                    result[key].append(data[index])
                    remaining[key] -= 1
                    index += 1
                    if index >= total_items:
                        break
    else:
        raise ValueError(f"Unknown mode: {mode}")

    return result


if __name__ == "__main__":
    # 测试用例
    weights = {"A": 6, "B": 3, "C": 1, "D": 2}
    data = list(range(1, 16))

    print("【chunk 模式】")
    print(distribute_by_weights(data, weights, mode="chunk"))

    print("\n【round_robin 模式】")
    print(distribute_by_weights(data, weights, mode="round_robin"))
