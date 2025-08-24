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

from typing import Dict, List, Any


def distribute_by_weights(
        data: List[Any],
        weights: Dict[str, int],
        partial_strategy: str = "proportional"
) -> Dict[str, List[Any]]:
    """
    按权重将 data 分配给多个分组，支持不同尾巴处理策略。

    参数：
        data: 待分配的数据列表（顺序保持）
        weights: {组名: 权重}，权重为正整数，决定比例
        partial_strategy: 尾巴处理策略，可选：
            - "proportional": 按比例分配（最大余数法，尽量接近比例）
            - "round_robin": 轮询分配（一个一个交替分配，最公平）
            - "sequential_fill": 顺序填满（从第一个 key 开始优先吃满）

    返回：
        {组名: 对应的数据列表}
    """
    group_keys = list(weights.keys())
    int_weights = {k: max(0, int(weights[k])) for k in group_keys}

    # 初始化结果
    result = {k: [] for k in group_keys}

    total_items = len(data)
    total_weight = sum(int_weights.values())

    if total_items == 0 or total_weight == 0:
        return result

    # 过滤掉权重为 0 的 key
    active_keys = [k for k in group_keys if int_weights[k] > 0]
    if not active_keys:
        return result

    # 计算完整轮数和剩余数量
    full_rounds = total_items // total_weight
    remaining_items = total_items - full_rounds * total_weight

    current_index = 0

    # Step 1: 完整轮分配
    for _ in range(full_rounds):
        for key in active_keys:
            count = int_weights[key]
            result[key].extend(data[current_index: current_index + count])
            current_index += count

    # Step 2: 尾巴处理
    if remaining_items <= 0:
        return result

    if partial_strategy == "proportional":
        # 按比例（最大余数法）
        quotas = {
            key: remaining_items * (int_weights[key] / total_weight)
            for key in active_keys
        }
        base_allocation = {key: int(quotas[key]) for key in active_keys}
        left_to_assign = remaining_items - sum(base_allocation.values())

        # 按小数部分排序补齐剩余
        frac_order = sorted(
            active_keys,
            key=lambda k: (quotas[k] - base_allocation[k]),
            reverse=True
        )

        final_allocation = base_allocation.copy()
        for key in frac_order:
            if left_to_assign <= 0:
                break
            final_allocation[key] += 1
            left_to_assign -= 1

    elif partial_strategy == "round_robin":
        # 公平轮询，一个一个交替分配，直到尾巴用完
        final_allocation = {k: 0 for k in active_keys}
        idx = 0
        left = remaining_items
        while left > 0:
            key = active_keys[idx]
            if final_allocation[key] < int_weights[key]:
                final_allocation[key] += 1
                left -= 1
            idx = (idx + 1) % len(active_keys)

    elif partial_strategy == "sequential_fill":
        # 顺序优先填满：优先把当前 key 填到本轮配额
        final_allocation = {}
        left = remaining_items
        for key in active_keys:
            if left <= 0:
                final_allocation[key] = 0
                continue
            take = min(int_weights[key], left)
            final_allocation[key] = take
            left -= take

    else:
        raise ValueError(f"Unknown partial_strategy: {partial_strategy}")

    # 按 final_allocation 分配尾巴
    for key in active_keys:
        count = final_allocation.get(key, 0)
        if count > 0:
            result[key].extend(data[current_index: current_index + count])
            current_index += count

    return result
