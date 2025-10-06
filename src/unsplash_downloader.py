import requests
import os
import time
import json
from datetime import datetime
import logging
from pathlib import Path
import sqlite3
from typing import List, Dict, Optional
import random
import hashlib

from config.config import Config

class UnsplashDownloader:
    def __init__(self):
        # 使用配置
        self.access_key = Config.UNSPLASH_ACCESS_KEY
        self.base_download_dir = Path(Config.BASE_DOWNLOAD_DIR)
        
        # 设置日志
        logging.basicConfig(
            level=getattr(logging, Config.LOG_LEVEL),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(Config.LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # 数据库配置
        self.db_file = Path(Config.DB_FILE)
        self.init_database()
        
        # API 配置
        self.base_url = "https://api.unsplash.com"
        self.headers = {
            'Authorization': f'Client-ID {self.access_key}',
            'Accept-Version': 'v1'
        }
        
        # 配置
        self.batch_size = Config.BATCH_SIZE
        self.request_interval = Config.REQUEST_INTERVAL
        self.download_interval = Config.DOWNLOAD_INTERVAL
        self.enable_url_logging = Config.ENABLE_URL_LOGGING
        
        # 新增：重复图片控制
        self.max_consecutive_duplicates = 5
        self.consecutive_duplicates = 0
        
        # 新增：API调用策略
        self.api_strategies = ['category', 'search', 'collections', 'random']
        self.current_strategy_index = 0
        
        # 新增：搜索关键词池
        self.search_keywords = [
            'landscape', 'nature', 'city', 'architecture', 'travel',
            'mountain', 'beach', 'forest', 'sky', 'water',
            'flower', 'animal', 'bird', 'cat', 'dog',
            'food', 'coffee', 'technology', 'computer', 'book',
            'art', 'music', 'sports', 'fitness', 'health',
            'business', 'office', 'car', 'bike', 'road',
            'winter', 'summer', 'autumn', 'spring', 'sunset',
            'night', 'morning', 'evening', 'dark', 'light'
        ]
        self.used_keywords = set()
        
        # 新增：图片质量筛选
        self.min_width = 1920
        self.min_height = 1080
        self.min_likes = 10
        
        # 创建基础目录结构
        self.create_category_directories()

    def create_category_directories(self):
        """创建分类目录"""
        try:
            # 创建基础目录
            self.base_download_dir.mkdir(exist_ok=True, parents=True)
            
            # 为每个 Unsplash 官方分类创建目录
            for category_slug in Config.UNSPLASH_CATEGORIES.keys():
                category_name = Config.get_category_name(category_slug)
                category_dir = self.base_download_dir / 'unsplash_images' / category_name
                category_dir.mkdir(exist_ok=True, parents=True)
                self.logger.debug(f"创建/确认分类目录: {category_dir}")
            
            self.logger.info("分类目录结构创建完成")
            
        except Exception as e:
            self.logger.error(f"创建分类目录失败: {e}")
            raise

    def init_database(self):
        """初始化数据库 - 包含自动迁移功能"""
        try:
            # 确保数据库文件目录存在
            self.db_file.parent.mkdir(exist_ok=True, parents=True)
            
            # 检查目录权限
            if not os.access(self.db_file.parent, os.W_OK):
                self.logger.error(f"目录 {self.db_file.parent} 没有写权限")
                try:
                    os.chmod(self.db_file.parent, 0o755)
                    self.logger.info(f"已修复目录权限: {self.db_file.parent}")
                except Exception as e:
                    self.logger.error(f"修复目录权限失败: {e}")
            
            # 测试数据库连接
            self.logger.info(f"尝试连接数据库: {self.db_file}")
            
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # 检查表是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='images'")
            table_exists = cursor.fetchone() is not None
            
            if table_exists:
                # 检查表结构，添加缺失的列
                self.migrate_database(cursor)
            else:
                # 创建新表
                self.create_tables(cursor)
            
            conn.commit()
            conn.close()
            self.logger.info("数据库初始化完成")
            
        except sqlite3.Error as e:
            self.logger.error(f"数据库初始化失败: {e}")
            raise

    def migrate_database(self, cursor):
        """迁移数据库表结构"""
        self.logger.info("检测到现有数据库，检查表结构...")
        
        # 检查images表结构
        cursor.execute("PRAGMA table_info(images)")
        columns = [column[1] for column in cursor.fetchall()]
        
        # 需要添加的列
        missing_columns = []
        
        if 'determined_category' not in columns:
            missing_columns.append('determined_category TEXT')
            self.logger.info("添加缺失列: determined_category")
            
        if 'confidence_score' not in columns:
            missing_columns.append('confidence_score REAL')
            self.logger.info("添加缺失列: confidence_score")
            
        if 'api_strategy' not in columns:
            missing_columns.append('api_strategy TEXT')
            self.logger.info("添加缺失列: api_strategy")
            
        if 'search_keyword' not in columns:
            missing_columns.append('search_keyword TEXT')
            self.logger.info("添加缺失列: search_keyword")
        
        # 添加缺失的列
        for column_def in missing_columns:
            try:
                cursor.execute(f"ALTER TABLE images ADD COLUMN {column_def}")
                self.logger.info(f"成功添加列: {column_def}")
            except sqlite3.Error as e:
                self.logger.error(f"添加列失败 {column_def}: {e}")
        
        # 创建其他可能缺失的表
        self.create_missing_tables(cursor)
        
        self.logger.info(f"数据库迁移完成，添加了 {len(missing_columns)} 个列")

    def create_tables(self, cursor):
        """创建所有表"""
        self.logger.info("创建新数据库表...")
        
        # 创建图片信息表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS images (
                id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                description TEXT,
                alt_description TEXT,
                user_name TEXT,
                user_username TEXT,
                user_id TEXT,
                image_url_raw TEXT,
                image_url_full TEXT,
                image_url_regular TEXT,
                image_url_small TEXT,
                image_url_thumb TEXT,
                download_time TEXT,
                width INTEGER,
                height INTEGER,
                color TEXT,
                likes INTEGER,
                tags TEXT,
                category TEXT,
                category_slug TEXT,
                created_at TEXT,
                updated_at TEXT,
                exif_data TEXT,
                location_data TEXT,
                download_status TEXT DEFAULT 'success',
                error_message TEXT,
                file_size INTEGER,
                file_hash TEXT,
                api_request_id TEXT,
                unsplash_link TEXT,
                api_strategy TEXT,
                search_keyword TEXT,
                determined_category TEXT,
                confidence_score REAL
            )
        ''')
        
        # 创建其他表
        self.create_missing_tables(cursor)

    def create_missing_tables(self, cursor):
        """创建其他可能缺失的表"""
        tables = [
            ('download_stats', '''
                CREATE TABLE IF NOT EXISTS download_stats (
                    date TEXT PRIMARY KEY,
                    total_downloaded INTEGER DEFAULT 0,
                    failed_downloads INTEGER DEFAULT 0,
                    total_file_size INTEGER DEFAULT 0
                )
            '''),
            ('image_tags', '''
                CREATE TABLE IF NOT EXISTS image_tags(
                    image_id TEXT,
                    tag TEXT
                )
            '''),
            ('category_stats', '''
                CREATE TABLE IF NOT EXISTS category_stats (
                    category TEXT PRIMARY KEY,
                    category_slug TEXT,
                    count INTEGER DEFAULT 0,
                    last_updated TEXT
                )
            '''),
            ('download_urls', '''
                CREATE TABLE IF NOT EXISTS download_urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id TEXT,
                    url_type TEXT,
                    url TEXT,
                    accessed_time TEXT,
                    status_code INTEGER,
                    response_time REAL,
                    FOREIGN KEY (image_id) REFERENCES images (id)
                )
            '''),
            ('error_logs', '''
                CREATE TABLE IF NOT EXISTS error_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id TEXT,
                    error_type TEXT,
                    error_message TEXT,
                    error_time TEXT,
                    url TEXT,
                    stack_trace TEXT
                )
            '''),
            ('api_strategy_stats', '''
                CREATE TABLE IF NOT EXISTS api_strategy_stats (
                    strategy TEXT PRIMARY KEY,
                    total_requests INTEGER DEFAULT 0,
                    successful_requests INTEGER DEFAULT 0,
                    total_images INTEGER DEFAULT 0,
                    new_images INTEGER DEFAULT 0,
                    last_used TEXT
                )
            ''')
        ]
        
        for table_name, create_sql in tables:
            try:
                cursor.execute(create_sql)
                self.logger.debug(f"创建/确认表: {table_name}")
            except sqlite3.Error as e:
                self.logger.error(f"创建表失败 {table_name}: {e}")

    def get_random_search_keyword(self):
        """获取随机搜索关键词"""
        if not self.search_keywords:
            return "nature"
        
        if len(self.used_keywords) >= len(self.search_keywords):
            self.used_keywords.clear()
            self.logger.info("重置搜索关键词池")
        
        available_keywords = [k for k in self.search_keywords if k not in self.used_keywords]
        if not available_keywords:
            available_keywords = self.search_keywords
        
        keyword = random.choice(available_keywords)
        self.used_keywords.add(keyword)
        return keyword

    def get_next_api_strategy(self):
        """获取下一个API调用策略"""
        strategy = self.api_strategies[self.current_strategy_index]
        self.current_strategy_index = (self.current_strategy_index + 1) % len(self.api_strategies)
        return strategy

    def record_api_strategy_usage(self, strategy: str, success: bool, total_images: int, new_images: int):
        """记录API策略使用情况"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO api_strategy_stats (strategy, total_requests, successful_requests, total_images, new_images, last_used)
                VALUES (?, 1, ?, ?, ?, ?)
                ON CONFLICT(strategy) DO UPDATE SET 
                    total_requests = total_requests + 1,
                    successful_requests = successful_requests + ?,
                    total_images = total_images + ?,
                    new_images = new_images + ?,
                    last_used = ?
            ''', (
                strategy,
                1 if success else 0,
                total_images,
                new_images,
                datetime.now().isoformat(),
                1 if success else 0,
                total_images,
                new_images,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except sqlite3.Error as e:
            self.logger.error(f"记录API策略统计失败: {e}")

    def get_photos_by_strategy(self, strategy: str, count: int = 10):
        """根据策略获取图片"""
        try:
            url = f"{self.base_url}/photos/random"
            params = {
                'count': count,
                'orientation': self.get_random_orientation()
            }
            
            # 根据策略添加不同参数
            if strategy == 'category':
                category = self.get_random_category()
                params['query'] = category
                extra_info = f"分类: {Config.get_category_name(category)}"
            elif strategy == 'search':
                keyword = self.get_random_search_keyword()
                params['query'] = keyword
                extra_info = f"关键词: {keyword}"
            elif strategy == 'collections':
                collections = ['317099', '1065976', '8637881', '8933527', '1675481']
                params['collections'] = ','.join(random.sample(collections, 2))
                extra_info = f"集合: {params['collections']}"
            else:  # random
                extra_info = "纯随机"
            
            self.logger.info(f"使用策略 '{strategy}' - {extra_info}")
            
            start_time = time.time()
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                photos = response.json()
                
                # 为每张图片添加策略信息
                for photo in photos:
                    photo['api_strategy'] = strategy
                    if strategy == 'search':
                        photo['search_keyword'] = params.get('query')
                    photo['api_request_time'] = response_time
                    photo['api_request_id'] = hashlib.md5(f"{strategy}_{datetime.now().timestamp()}".encode()).hexdigest()[:8]
                
                return photos
            else:
                self.logger.error(f"API 请求失败 ({strategy}): {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"网络错误 ({strategy}): {e}")
            return None

    def determine_image_category(self, photo_data: Dict) -> tuple:
        """
        智能确定图片分类
        返回: (category_slug, category_name, confidence_score)
        """
        # 方法1: 使用API请求的分类（最高优先级）
        if photo_data.get('api_strategy') == 'category' and 'search_keyword' in photo_data:
            requested_category = photo_data['search_keyword']
            if requested_category in Config.UNSPLASH_CATEGORIES:
                return requested_category, Config.get_category_name(requested_category), 1.0
        
        # 方法2: 使用图片的标签分析
        tags = []
        if 'tags' in photo_data:
            tags = [tag['title'].lower() for tag in photo_data['tags'] if 'title' in tag]
        
        # 方法3: 使用描述和标题分析
        description = (photo_data.get('description') or photo_data.get('alt_description') or '').lower()
        
        # 分类关键词映射 - 使用中文关键词
        category_keywords = {
            'nature': ['自然', '风景', '山', '森林', '树', '花', '植物', '叶子', '绿色', '户外', '野外', '公园', '花园'],
            'people': ['人', '人物', '人类', '男人', '女人', '孩子', '婴儿', '肖像', '脸', '人群', '团体', '家庭'],
            'animals': ['动物', '狗', '猫', '鸟', '宠物', '野生动物', '哺乳动物', '爬行动物', '昆虫', '鱼'],
            'architecture': ['建筑', '建筑物', '房子', '城市', '都市', '摩天大楼', '结构', '设计', '现代', '古代'],
            'travel': ['旅行', '旅游', '目的地', '假期', '冒险', '探索', '旅程', '旅行'],
            'food': ['食物', '餐', '菜肴', '烹饪', '食谱', '餐厅', '美味', '好吃', '美食'],
            'technology': ['技术', '电脑', '设备', '电子', '数字', '科技', '小工具', '创新'],
            'art': ['艺术', '绘画', '画画', '创意', '设计', '抽象', '多彩', '艺术性'],
            'sports': ['运动', '游戏', '运动员', '比赛', '健身', '锻炼', '训练', '比赛'],
            'business': ['商业', '办公室', '工作', '公司', '专业', '会议', '商业']
        }
        
        # 计算每个分类的匹配分数
        category_scores = {}
        for category_slug, keywords in category_keywords.items():
            score = 0
            
            # 标签匹配
            for tag in tags:
                if any(keyword in tag for keyword in keywords):
                    score += 2  # 标签匹配权重更高
            
            # 描述匹配
            for keyword in keywords:
                if keyword in description:
                    score += 1
            
            category_scores[category_slug] = score
        
        # 找到最高分的分类
        if category_scores:
            best_category = max(category_scores.items(), key=lambda x: x[1])
            best_slug, best_score = best_category
            
            # 计算置信度
            total_possible_score = len(tags) * 2 + len(description.split()) // 10
            confidence = best_score / max(total_possible_score, 1) if total_possible_score > 0 else 0
            
            # 如果置信度太低，使用随机分类
            if confidence < 0.3:
                random_slug = self.get_random_category()
                return random_slug, Config.get_category_name(random_slug), 0.1
            
            return best_slug, Config.get_category_name(best_slug), confidence
        
        # 如果没有匹配到任何分类，使用随机分类
        random_slug = self.get_random_category()
        return random_slug, Config.get_category_name(random_slug), 0.1

    def filter_low_quality_images(self, photos: List[Dict]) -> List[Dict]:
        """过滤低质量图片"""
        if not photos:
            return []
        
        filtered_photos = []
        for photo in photos:
            width = photo.get('width', 0)
            height = photo.get('height', 0)
            likes = photo.get('likes', 0)
            
            if (width < self.min_width or height < self.min_height or 
                likes < self.min_likes):
                self.logger.debug(f"跳过低质量图片 {photo['id']}: {width}x{height}, {likes} likes")
                continue
            
            filtered_photos.append(photo)
        
        self.logger.info(f"质量过滤: {len(photos)} -> {len(filtered_photos)} 张图片")
        return filtered_photos

    def get_random_category(self):
        """随机选择一个分类"""
        categories = list(Config.UNSPLASH_CATEGORIES.keys())
        return random.choice(categories)

    def get_random_orientation(self):
        """随机选择图片方向"""
        orientations = ['landscape', 'portrait', 'squarish', None]
        return random.choice(orientations)

    def is_image_downloaded(self, image_id: str) -> bool:
        """检查图片是否已下载"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM images WHERE id = ?", (image_id,))
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except sqlite3.Error as e:
            self.logger.error(f"查询图片状态失败: {e}")
            return False

    def calculate_file_hash(self, filepath: Path) -> str:
        """计算文件哈希值"""
        try:
            hasher = hashlib.md5()
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            self.logger.warning(f"计算文件哈希失败 {filepath}: {e}")
            return ""

    def record_download_url(self, image_id: str, url_type: str, url: str, status_code: int = None, response_time: float = None):
        """记录下载链接访问信息"""
        if not self.enable_url_logging:
            return
            
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO download_urls (image_id, url_type, url, accessed_time, status_code, response_time)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                image_id,
                url_type,
                url,
                datetime.now().isoformat(),
                status_code,
                response_time
            ))
            
            conn.commit()
            conn.close()
            
        except sqlite3.Error as e:
            self.logger.error(f"记录下载链接失败: {e}")

    def log_error(self, image_id: str, error_type: str, error_message: str, url: str = None, stack_trace: str = None):
        """记录错误信息"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO error_logs (image_id, error_type, error_message, error_time, url, stack_trace)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                image_id,
                error_type,
                error_message,
                datetime.now().isoformat(),
                url,
                stack_trace
            ))
            
            conn.commit()
            conn.close()
            
        except sqlite3.Error as e:
            self.logger.error(f"记录错误日志失败: {e}")

    def save_image_info(self, photo_data: Dict, filename: str, category_slug: str, category_name: str, 
                       file_size: int = 0, file_hash: str = "", determined_category: str = "", confidence_score: float = 0) -> bool:
        """保存图片信息到数据库 - 兼容新旧表结构"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # 处理标签
            tags = []
            if 'tags' in photo_data:
                tags = [tag['title'] for tag in photo_data['tags'] if 'title' in tag]
            
            # 处理 EXIF 数据
            exif_data = {}
            if 'exif' in photo_data:
                exif_data = photo_data['exif']
            
            # 处理位置数据
            location_data = {}
            if 'location' in photo_data:
                location_data = photo_data['location']
            
            # 检查表结构，动态构建SQL
            cursor.execute("PRAGMA table_info(images)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # 构建基础列名和值
            base_columns = [
                'id', 'filename', 'description', 'alt_description', 'user_name', 'user_username', 'user_id',
                'image_url_raw', 'image_url_full', 'image_url_regular', 'image_url_small', 'image_url_thumb',
                'download_time', 'width', 'height', 'color', 'likes', 'tags', 'category', 'category_slug',
                'created_at', 'updated_at', 'exif_data', 'location_data', 'file_size', 'file_hash'
            ]
            base_values = [
                photo_data['id'],
                filename,
                photo_data.get('description', ''),
                photo_data.get('alt_description', ''),
                photo_data['user'].get('name', ''),
                photo_data['user'].get('username', ''),
                photo_data['user'].get('id', ''),
                photo_data['urls'].get('raw', ''),
                photo_data['urls'].get('full', ''),
                photo_data['urls'].get('regular', ''),
                photo_data['urls'].get('small', ''),
                photo_data['urls'].get('thumb', ''),
                datetime.now().isoformat(),
                photo_data.get('width', 0),
                photo_data.get('height', 0),
                photo_data.get('color', ''),
                photo_data.get('likes', 0),
                json.dumps(tags),
                category_name,
                category_slug,
                photo_data.get('created_at', ''),
                photo_data.get('updated_at', ''),
                json.dumps(exif_data),
                json.dumps(location_data),
                file_size,
                file_hash
            ]
            
            # 添加可选列
            optional_columns = [
                ('api_request_id', photo_data.get('api_request_id', '')),
                ('unsplash_link', photo_data.get('links', {}).get('html', '')),
                ('api_strategy', photo_data.get('api_strategy', 'unknown')),
                ('search_keyword', photo_data.get('search_keyword', '')),
                ('determined_category', determined_category),
                ('confidence_score', confidence_score)
            ]
            
            # 只添加存在的列
            for col_name, col_value in optional_columns:
                if col_name in columns:
                    base_columns.append(col_name)
                    base_values.append(col_value)
            
            # 构建SQL
            placeholders = ', '.join(['?' for _ in base_columns])
            column_names = ', '.join(base_columns)
            
            cursor.execute(f'''
                INSERT OR REPLACE INTO images ({column_names})
                VALUES ({placeholders})
            ''', base_values)
            
            # 插入标签到单独的表
            for tag in tags:
                cursor.execute(
                    "INSERT OR IGNORE INTO image_tags (image_id, tag) VALUES (?, ?)",
                    (photo_data['id'], tag)
                )
            
            # 更新分类统计
            cursor.execute('''
                INSERT INTO category_stats (category, category_slug, count, last_updated)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(category) DO UPDATE SET 
                    count = count + 1,
                    last_updated = ?
            ''', (category_name, category_slug, datetime.now().isoformat(), datetime.now().isoformat()))
            
            # 更新下载统计
            today = datetime.now().strftime("%Y-%m-%d")
            cursor.execute('''
                INSERT INTO download_stats (date, total_downloaded, failed_downloads, total_file_size)
                VALUES (?, 1, 0, ?)
                ON CONFLICT(date) DO UPDATE SET 
                    total_downloaded = total_downloaded + 1,
                    total_file_size = total_file_size + ?
            ''', (today, file_size, file_size))
            
            conn.commit()
            conn.close()
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"保存图片信息失败: {e}")
            return False

    def get_category_directory(self, category_name: str) -> Path:
        """获取分类对应的目录路径"""
        return self.base_download_dir / 'unsplash_images' / category_name

    def download_image(self, photo_data):
        """下载单张图片到分类目录"""
        photo_id = photo_data['id']
        
        if self.is_image_downloaded(photo_id):
            self.logger.info(f"图片 {photo_id} 已下载，跳过")
            self.consecutive_duplicates += 1
            return False

        try:
            # 智能确定图片分类
            category_slug, category_name, confidence = self.determine_image_category(photo_data)
            
            self.logger.info(f"确定图片分类: {category_name} (置信度: {confidence:.2f})")
            
            # 如果置信度低，记录警告
            if confidence < 0.5:
                self.logger.warning(f"图片 {photo_id} 分类置信度较低: {confidence:.2f}")
            
            category_dir = self.get_category_directory(category_name)
            
            # 确保目录存在
            category_dir.mkdir(exist_ok=True, parents=True)
            
            # 选择最高质量的图片URL
            image_url = photo_data['urls']['raw']
            
            self.logger.info(f"开始下载图片: {photo_id} -> 分类: {category_name}")
            
            # 下载图片
            start_time = time.time()
            response = requests.get(image_url, stream=True, timeout=60)
            response_time = time.time() - start_time
            response.raise_for_status()
            
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_{photo_id}.jpg"
            filepath = category_dir / filename
            
            # 保存图片
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # 获取文件信息
            file_size = filepath.stat().st_size
            file_hash = self.calculate_file_hash(filepath)
            
            # 保存图片信息到数据库
            if self.save_image_info(photo_data, filename, category_slug, category_name, file_size, file_hash, category_slug, confidence):
                self.logger.info(f"成功下载并保存信息: {filename} -> {category_name} ({file_size} bytes)")
                self.consecutive_duplicates = 0
                return True
            else:
                self.logger.error(f"下载成功但保存信息失败: {filename}")
                if filepath.exists():
                    filepath.unlink()
                return False
            
        except Exception as e:
            self.logger.error(f"下载失败 {photo_id}: {e}")
            
            import traceback
            self.log_error(
                photo_id,
                'download_error',
                str(e),
                url=photo_data['urls'].get('raw', ''),
                stack_trace=traceback.format_exc()
            )
            
            self.record_failed_download()
            return False

    def record_failed_download(self):
        """记录失败的下载"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            today = datetime.now().strftime("%Y-%m-%d")
            cursor.execute('''
                INSERT INTO download_stats (date, total_downloaded, failed_downloads, total_file_size)
                VALUES (?, 0, 1, 0)
                ON CONFLICT(date) DO UPDATE SET failed_downloads = failed_downloads + 1
            ''', (today,))
            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            self.logger.error(f"记录失败下载失败: {e}")

    # 其他方法保持不变...

    def run_enhanced_download(self):
        """增强版下载循环 - 解决重复数据和分类问题"""
        self.logger.info(f"开始增强版下载，批次大小: {self.batch_size}")
        self.logger.info("使用智能分类算法提高分类准确性")
        self.logger.info(f"图片质量要求: {self.min_width}x{self.min_height}+, {self.min_likes}+ likes")
        
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while True:
            try:
                # 选择API策略
                strategy = self.get_next_api_strategy()
                
                self.logger.info(f"使用策略 '{strategy}' 请求 {self.batch_size} 张图片...")
                
                # 根据策略获取图片
                photos = self.get_photos_by_strategy(strategy, self.batch_size)
                
                if photos and isinstance(photos, list):
                    # 过滤低质量图片
                    filtered_photos = self.filter_low_quality_images(photos)
                    
                    if not filtered_photos:
                        self.logger.warning(f"策略 '{strategy}' 返回的图片全部被质量过滤")
                        continue
                    
                    downloaded_count = 0
                    total_images = len(filtered_photos)
                    
                    # 下载每张图片
                    for photo in filtered_photos:
                        if isinstance(photo, dict) and 'id' in photo:
                            if self.download_image(photo):
                                downloaded_count += 1
                                time.sleep(self.download_interval)
                    
                    self.logger.info(f"策略 '{strategy}' 完成: {downloaded_count}/{total_images} 张新图片")
                    
                    # 检查连续重复情况
                    if downloaded_count == 0:
                        self.consecutive_duplicates += 1
                        self.logger.warning(f"连续 {self.consecutive_duplicates} 批次没有新图片")
                        
                        if self.consecutive_duplicates >= self.max_consecutive_duplicates:
                            self.logger.warning("连续重复过多，强制切换策略并增加等待时间")
                            self.current_strategy_index = (self.current_strategy_index + 1) % len(self.api_strategies)
                            self.consecutive_duplicates = 0
                            extra_wait = 300
                            self.logger.info(f"额外等待 {extra_wait} 秒")
                            time.sleep(extra_wait)
                    else:
                        self.consecutive_duplicates = 0
                    
                    consecutive_errors = 0
                else:
                    consecutive_errors += 1
                    self.logger.warning(f"获取图片失败，连续错误次数: {consecutive_errors}")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        self.logger.error("连续错误过多，暂停 10 分钟")
                        time.sleep(600)
                        consecutive_errors = 0
                
                # 等待下一批次请求
                self.logger.info(f"等待 {self.request_interval} 秒后进行下一批次请求")
                time.sleep(self.request_interval)
                
            except KeyboardInterrupt:
                self.logger.info(f"用户中断下载")
                break
            except Exception as e:
                self.logger.error(f"未预期的错误: {e}")
                consecutive_errors += 1
                time.sleep(300)

def main():
    """主函数"""
    try:
        # 验证配置
        Config.validate()
        
        # 创建下载器实例
        downloader = UnsplashDownloader()
        
        # 开始下载
        downloader.run_enhanced_download()
        
    except ValueError as e:
        print(f"配置错误: {e}")
        print("请设置 UNSPLASH_ACCESS_KEY 环境变量")
        exit(1)
    except KeyboardInterrupt:
        print("下载结束")
    except Exception as e:
        print(f"未预期的错误: {e}")
        exit(1)

if __name__ == "__main__":
    main()
