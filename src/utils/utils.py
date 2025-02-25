def parse_market_cap(value_str: str) -> float:
    """解析市值字符串为数值
    
    Args:
        value_str: 市值字符串，如"100K", "1.5M", "$2B"等
        
    Returns:
        float: 解析后的数值，若解析失败则返回0
    """
    try:
        # 处理数值类型的输入
        if isinstance(value_str, (int, float)):
            return float(value_str)
        
        # 如果输入为None或空字符串，直接返回0
        if value_str is None or str(value_str).strip() == '':
            return 0
        
        # 清理字符串
        clean_str = str(value_str).replace('💰', '').replace('市值：', '').replace('市值:', '')
        clean_str = clean_str.replace('**', '').replace('$', '').replace(',', '').strip()
        
        # 查找并应用倍数
        multiplier = 1
        if 'K' in clean_str.upper():
            multiplier = 1000
            clean_str = clean_str.upper().replace('K', '').strip()
        elif 'M' in clean_str.upper():
            multiplier = 1000000
            clean_str = clean_str.upper().replace('M', '').strip()
        elif 'B' in clean_str.upper():
            multiplier = 1000000000
            clean_str = clean_str.upper().replace('B', '').strip()
        
        # 如果处理后的字符串为空，返回0
        if not clean_str:
            return 0
            
        return float(clean_str) * multiplier
        
    except Exception as e:
        # 记录错误但不抛出异常
        print(f"解析市值出错: {value_str}, 错误: {str(e)}")
        return 0


def format_market_cap(value):
    """格式化市值显示
    
    Args:
        value: 市值数值或字符串
        
    Returns:
        str: 格式化后的市值字符串
    """
    try:
        # 处理None或无效输入
        if value is None:
            return "0.00"
            
        # 如果是字符串，尝试解析
        if isinstance(value, str):
            value = parse_market_cap(value)
            
        # 格式化显示
        if value >= 100000000:  # 亿
            return f"{value/100000000:.2f}亿"
        elif value >= 10000:    # 万
            return f"{value/10000:.2f}万"
        return f"{value:.2f}"
    except Exception as e:
        # 记录错误但返回默认值
        print(f"市值格式化错误: {value}, 错误: {str(e)}")
        return "0.00" 