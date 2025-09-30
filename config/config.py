import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """配置类"""
    
    # Unsplash API 配置
    UNSPLASH_ACCESS_KEY = os.getenv('UNSPLASH_ACCESS_KEY', '')
    
    # 下载配置
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
    REQUEST_INTERVAL = int(os.getenv('REQUEST_INTERVAL', '60'))
    DOWNLOAD_INTERVAL = int(os.getenv('DOWNLOAD_INTERVAL', '2'))
    
    # 路径配置
    BASE_DOWNLOAD_DIR = os.getenv('BASE_DOWNLOAD_DIR', '/app/data')
    DB_FILE = os.getenv('DB_FILE', '/app/data/unsplash.db')
    LOG_FILE = os.getenv('LOG_FILE', '/app/logs/downloader.log')
    
    # 日志级别
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # 下载链接跟踪配置
    ENABLE_URL_LOGGING = os.getenv('ENABLE_URL_LOGGING', 'true').lower() == 'true'
    
    # Unsplash 官方分类
    UNSPLASH_CATEGORIES = {
        'backgrounds': '背景',
        'fashion': '时尚',
        'nature': '自然',
        'science': '科学',
        'education': '教育',
        'feelings': '情感',
        'health': '健康',
        'people': '人物',
        'religion': '宗教',
        'places': '地点',
        'animals': '动物',
        'industry': '工业',
        'computer': '计算机',
        'food': '食物',
        'sports': '运动',
        'transportation': '交通',
        'travel': '旅行',
        'buildings': '建筑',
        'business': '商业',
        'music': '音乐'
    }
    
    @classmethod
    def get_category_name(cls, category_slug):
        """获取分类的中文名称"""
        return cls.UNSPLASH_CATEGORIES.get(category_slug, category_slug)
    
    @classmethod
    def validate(cls):
        """验证配置"""
        if not cls.UNSPLASH_ACCESS_KEY:
            raise ValueError("UNSPLASH_ACCESS_KEY 环境变量必须设置")
        
        if cls.BATCH_SIZE <= 0 or cls.BATCH_SIZE > 30:
            raise ValueError("BATCH_SIZE 必须在 1-30 之间")
        
        return True

