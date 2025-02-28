import re
import os
import json
import logging
import jieba
import numpy as np
from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

# 建立日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 加载情感分析词典
def load_sentiment_dict():
    """加载情感分析词典"""
    try:
        # 定义可能的词典路径
        dict_paths = [
            # 默认路径（相对于分析器）
            os.path.join(os.path.dirname(__file__), 'dicts'),
            # 项目根目录下的data/sentiment
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'sentiment')
        ]
        
        # 积极词汇
        positive_words = []
        for base_path in dict_paths:
            positive_path = os.path.join(base_path, 'positive_words.txt')
            if os.path.exists(positive_path):
                logger.info(f"从 {positive_path} 加载积极词汇")
                with open(positive_path, 'r', encoding='utf-8') as f:
                    positive_words = [line.strip() for line in f if line.strip()]
                break
                
        if not positive_words:
            # 默认词汇
            logger.warning("未找到积极词汇文件，使用默认词汇")
            positive_words = [
                '看涨', '上涨', '利好', '突破', '强烈推荐', '牛市', '利润', '增长', '上升', '发展',
                '机会', '优质', '成功', '突破', '领先', '大涨', '潜力', '值得', '赚', '优势',
                'bullish', 'moon', 'gem', 'pump', 'profit', 'gain', 'up', 'grow', 'increase',
                'opportunity', 'success', 'breakthrough', 'potential', 'recommend', 'promising',
                '🚀', '🔥', '💪', '💰', '📈', '✅'
            ]
            
        # 消极词汇
        negative_words = []
        for base_path in dict_paths:
            negative_path = os.path.join(base_path, 'negative_words.txt')
            if os.path.exists(negative_path):
                logger.info(f"从 {negative_path} 加载消极词汇")
                with open(negative_path, 'r', encoding='utf-8') as f:
                    negative_words = [line.strip() for line in f if line.strip()]
                break
                
        if not negative_words:
            # 默认词汇
            logger.warning("未找到消极词汇文件，使用默认词汇")
            negative_words = [
                '看跌', '下跌', '利空', '跌破', '风险', '熊市', '损失', '下降', '亏损', '崩盘',
                '泡沫', '资金盘', '骗局', '跑路', '失败', '危机', '警惕', '谨慎', '诈骗', '黑幕',
                'bearish', 'dump', 'scam', 'rug', 'loss', 'crash', 'down', 'decrease', 'fail',
                'ponzi', 'scheme', 'suspect', 'risk', 'warning', 'careful', 'suspect',
                '📉', '⚠️', '❌'
            ]
        
        # 炒作词汇
        hype_words = []
        for base_path in dict_paths:
            hype_path = os.path.join(base_path, 'hype_words.txt')
            if os.path.exists(hype_path):
                logger.info(f"从 {hype_path} 加载炒作词汇")
                with open(hype_path, 'r', encoding='utf-8') as f:
                    hype_words = [line.strip() for line in f if line.strip()]
                break
                
        if not hype_words:
            # 默认词汇
            logger.warning("未找到炒作词汇文件，使用默认词汇")
            hype_words = [
                '暴涨', '百倍', '千倍', '万倍', '秒杀', '起飞', '一夜暴富', '财富自由', '不容错过', 
                '机不可失', '稀缺', '抄底', '最后机会', '快上车', '错过必后悔', '爆发', '神秘', 
                '100x', '1000x', 'moonshot', 'to the moon', 'huge', 'massive', 'incredible',
                'don\'t miss', 'last chance', 'hidden gem', 'next big thing', 'explode'
            ]
            
        # 记录词汇数量
        logger.info(f"加载了 {len(positive_words)} 个积极词汇, {len(negative_words)} 个消极词汇, {len(hype_words)} 个炒作词汇")
        
        return {
            'positive': positive_words,
            'negative': negative_words,
            'hype': hype_words
        }
    except Exception as e:
        logger.error(f"加载情感词典出错: {str(e)}")
        return {'positive': [], 'negative': [], 'hype': []}

# 全局情感词典
SENTIMENT_DICT = load_sentiment_dict()

class TokenAnalyzer:
    """代币信息分析器，提供情感分析和价格趋势分析功能"""
    
    def __init__(self):
        """初始化分析器"""
        self.sentiment_dict = SENTIMENT_DICT
        
    def analyze_text(self, text: str) -> Dict[str, Any]:
        """分析文本，提取情感信息和关键词"""
        if not text:
            return {
                'sentiment_score': 0,
                'positive_words': [],
                'negative_words': [],
                'hype_score': 0,
                'risk_level': 'unknown'
            }
            
        # 分词（支持中英文混合）
        words = self._tokenize(text)
        
        # 匹配情感词
        positive_matches = [word for word in words if word.lower() in [w.lower() for w in self.sentiment_dict['positive']]]
        negative_matches = [word for word in words if word.lower() in [w.lower() for w in self.sentiment_dict['negative']]]
        hype_matches = [word for word in words if word.lower() in [w.lower() for w in self.sentiment_dict['hype']]]
        
        # 匹配表情符号
        emoji_score = self._analyze_emojis(text)
        
        # 计算情感得分
        positive_count = len(positive_matches) + emoji_score['positive']
        negative_count = len(negative_matches) + emoji_score['negative']
        total_count = positive_count + negative_count
        
        # 归一化情感得分到[-1, 1]区间
        sentiment_score = 0
        if total_count > 0:
            sentiment_score = (positive_count - negative_count) / total_count
            
        # 计算炒作得分
        hype_score = (len(hype_matches) / max(len(words), 1)) * 5  # 归一化到0-5范围
        
        # 确定风险等级
        risk_level = self._determine_risk_level(sentiment_score, hype_score, text)
        
        return {
            'sentiment_score': sentiment_score,
            'positive_words': positive_matches,
            'negative_words': negative_matches,
            'hype_score': hype_score,
            'risk_level': risk_level
        }
        
    def _tokenize(self, text: str) -> List[str]:
        """分词处理，支持中英文混合"""
        # 对中文进行分词
        words = []
        if any('\u4e00' <= ch <= '\u9fff' for ch in text):  # 包含中文字符
            words = list(jieba.cut(text))
        else:
            # 英文分词
            words = re.findall(r'\b\w+\b', text.lower())
            
        # 过滤空白词
        words = [w for w in words if w.strip()]
        return words
        
    def _analyze_emojis(self, text: str) -> Dict[str, int]:
        """分析文本中的表情符号"""
        positive_emojis = ['🚀', '🔥', '💪', '💰', '📈', '✅', '👍', '😊', '🙌', '💎']
        negative_emojis = ['📉', '⚠️', '❌', '👎', '😱', '😢', '😭', '🙄', '🤔', '😡']
        
        positive_count = sum(text.count(emoji) for emoji in positive_emojis)
        negative_count = sum(text.count(emoji) for emoji in negative_emojis)
        
        return {
            'positive': positive_count,
            'negative': negative_count
        }
        
    def _determine_risk_level(self, sentiment_score: float, hype_score: float, text: str) -> str:
        """基于情感分析和炒作得分确定风险等级"""
        # 检测风险关键词
        risk_keywords = ['scam', '骗局', 'rug', '跑路', 'ponzi', '资金盘', 'warning', '风险', 'honeypot']
        has_risk_keyword = any(keyword in text.lower() for keyword in risk_keywords)
        
        # 检测异常高的炒作
        excessive_hype = hype_score > 3.5
        
        # 综合评估风险
        if has_risk_keyword:
            return 'high'  # 统一使用英文
        elif excessive_hype and sentiment_score > 0.7:
            return 'medium-high'  # 统一使用英文
        elif excessive_hype:
            return 'medium'  # 统一使用英文
        elif sentiment_score < -0.5:
            return 'medium'  # 统一使用英文
        elif sentiment_score > 0.7:
            return 'low-medium'  # 统一使用英文
        else:
            return 'low'  # 统一使用英文
            
    def analyze_price_trend(self, current_price: float, price_history: List[Dict]) -> Dict[str, Any]:
        """分析价格趋势"""
        if not price_history or current_price is None:
            return {
                'price_change_24h': 0,
                'price_change_7d': 0,
                'volatility': 0,
                'trend': 'neutral'
            }
            
        # 按时间排序
        sorted_history = sorted(price_history, key=lambda x: x.get('timestamp', 0))
        
        # 提取24小时前和7天前的价格
        now = datetime.now()
        day_ago = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)
        
        price_24h_ago = None
        price_7d_ago = None
        
        for entry in sorted_history:
            timestamp = entry.get('timestamp')
            if not timestamp:
                continue
                
            entry_time = datetime.fromtimestamp(timestamp)
            if price_24h_ago is None and entry_time <= day_ago:
                price_24h_ago = entry.get('price')
            if price_7d_ago is None and entry_time <= week_ago:
                price_7d_ago = entry.get('price')
                
        # 计算价格变化
        price_change_24h = 0
        if price_24h_ago and price_24h_ago > 0:
            price_change_24h = (current_price - price_24h_ago) / price_24h_ago * 100
            
        price_change_7d = 0
        if price_7d_ago and price_7d_ago > 0:
            price_change_7d = (current_price - price_7d_ago) / price_7d_ago * 100
            
        # 计算波动率
        prices = [entry.get('price', 0) for entry in sorted_history if entry.get('price')]
        volatility = 0
        if len(prices) > 1:
            volatility = np.std(prices) / np.mean(prices) * 100 if np.mean(prices) > 0 else 0
            
        # 确定趋势
        trend = 'neutral'
        if price_change_24h > 10:
            trend = 'strong_bullish'
        elif price_change_24h > 5:
            trend = 'bullish'
        elif price_change_24h < -10:
            trend = 'strong_bearish'
        elif price_change_24h < -5:
            trend = 'bearish'
            
        return {
            'price_change_24h': price_change_24h,
            'price_change_7d': price_change_7d,
            'volatility': volatility,
            'trend': trend
        }
        
    def analyze_token(self, text: str, price: Optional[float] = None, price_history: List[Dict] = None) -> Dict[str, Any]:
        """综合分析代币信息"""
        # 情感分析
        sentiment_result = self.analyze_text(text)
        
        # 价格趋势分析
        price_trend = {}
        if price is not None and price_history:
            price_trend = self.analyze_price_trend(price, price_history)
            
        # 组合结果
        result = {
            # 情感分析结果
            'sentiment_score': sentiment_result.get('sentiment_score', 0),
            'positive_words': sentiment_result.get('positive_words', []),
            'negative_words': sentiment_result.get('negative_words', []),
            'hype_score': sentiment_result.get('hype_score', 0),
            'risk_level': sentiment_result.get('risk_level', 'unknown'),
            
            # 价格趋势结果
            'price': price,
            'price_change_24h': price_trend.get('price_change_24h', 0),
            'price_change_7d': price_trend.get('price_change_7d', 0),
            'volatility': price_trend.get('volatility', 0),
            'trend': price_trend.get('trend', 'neutral'),
            
            # 生成摘要
            'summary': self._generate_summary({**sentiment_result, **price_trend, 'price': price})
        }
        
        return result
        
    def _generate_summary(self, analysis_result: Dict) -> str:
        """生成分析摘要"""
        
        # 情感分析部分
        sentiment = analysis_result.get('sentiment_score', 0)
        sentiment_desc = "中性"
        if sentiment > 0.5:
            sentiment_desc = "非常积极"
        elif sentiment > 0.1:
            sentiment_desc = "积极"
        elif sentiment < -0.5:
            sentiment_desc = "非常消极"
        elif sentiment < -0.1:
            sentiment_desc = "消极"
            
        # 炒作评分描述
        hype_score = analysis_result.get('hype_score', 0)
        hype_desc = "无明显炒作"
        if hype_score > 4:
            hype_desc = "非常高的炒作"
        elif hype_score > 3:
            hype_desc = "高炒作"
        elif hype_score > 2:
            hype_desc = "中等炒作"
            
        # 风险等级描述
        risk_map = {
            'high': "高风险",
            'medium-high': "中高风险",
            'medium': "中风险", 
            'low-medium': "低中风险",
            'low': "低风险",
            'unknown': "未知风险"
        }
        risk_desc = risk_map.get(analysis_result.get('risk_level', 'unknown'), "未知风险")
        
        # 价格趋势部分
        trend_part = ""
        if 'price_change_24h' in analysis_result:
            change_24h = analysis_result.get('price_change_24h', 0)
            trend = analysis_result.get('trend', 'neutral')
            
            trend_map = {
                'strong_bullish': "强烈上涨",
                'bullish': "上涨",
                'neutral': "稳定",
                'bearish': "下跌",
                'strong_bearish': "强烈下跌"
            }
            
            trend_desc = trend_map.get(trend, "稳定")
            trend_part = f"价格24小时变化: {change_24h:.2f}%, 趋势: {trend_desc}. "
            
        # 组合摘要
        summary = f"情感分析: {sentiment_desc}, 炒作评分: {hype_desc}, 风险评级: {risk_desc}. {trend_part}"
        
        # 添加提示语
        if analysis_result.get('risk_level') in ['high', 'medium-high']:
            summary += "请注意风险，谨慎投资!"
        
        return summary
        
        
# 单例模式
_analyzer = None

def get_analyzer() -> TokenAnalyzer:
    """获取TokenAnalyzer单例"""
    global _analyzer
    if _analyzer is None:
        _analyzer = TokenAnalyzer()
    return _analyzer


# 测试函数
if __name__ == "__main__":
    # 测试文本
    test_text = """
    🚀 最新代币: MEMEX
    
    这个代币即将暴涨100倍！🔥
    市值只有: 500K
    
    Telegram: https://t.me/memex_coin
    网站: https://memex.finance
    
    不容错过的千倍机会！💰
    """
    
    analyzer = TokenAnalyzer()
    result = analyzer.analyze_token(test_text)
    
    print("分析结果:")
    print(f"情感得分: {result['sentiment_score']}")
    print(f"积极词汇: {result['positive_words']}")
    print(f"消极词汇: {result['negative_words']}")
    print(f"炒作得分: {result['hype_score']}")
    print(f"风险等级: {result['risk_level']}")
    print(f"摘要: {result['summary']}") 