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
            
            # 创建 other 目录
            other_dir = self.base_download_dir / 'unsplash_images' / '其他'
            other_dir.mkdir(exist_ok=True, parents=True)
            
            self.logger.info("分类目录结构创建完成")
            
        except Exception as e:
            self.logger.error(f"创建分类目录失败: {e}")
            raise

    def init_database(self):
        """初始化数据库"""
        try:
            # 确保数据库文件目录存在
            self.db_file.parent.mkdir(exist_ok=True, parents=True)
            
            # 检查目录权限
            if not os.access(self.db_file.parent, os.W_OK):
                self.logger.error(f"目录 {self.db_file.parent} 没有写权限")
                # 尝试修复权限
                try:
                    os.chmod(self.db_file.parent, 0o755)
                    self.logger.info(f"已修复目录权限: {self.db_file.parent}")
                except Exception as e:
                    self.logger.error(f"修复目录权限失败: {e}")
            
            # 测试数据库连接
            self.logger.info(f"尝试连接数据库: {self.db_file}")
            
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # 创建图片信息表（增强版）
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
                    unsplash_link TEXT
                )
            ''')
            
            # 创建下载统计表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS download_stats (
                    date TEXT PRIMARY KEY,
                    total_downloaded INTEGER DEFAULT 0,
                    failed_downloads INTEGER DEFAULT 0,
                    total_file_size INTEGER DEFAULT 0
                )
            ''')

            # 创建下载统计表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS image_tags(
                    image_id TEXT,
		    tag TEXT
                )
            ''')
            
            # 创建分类统计表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS category_stats (
                    category TEXT PRIMARY KEY,
                    category_slug TEXT,
                    count INTEGER DEFAULT 0,
                    last_updated TEXT
                )
            ''')
            
            # 创建下载链接跟踪表
            cursor.execute('''
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
            ''')
            
            # 创建错误日志表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS error_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id TEXT,
                    error_type TEXT,
                    error_message TEXT,
                    error_time TEXT,
                    url TEXT,
                    stack_trace TEXT
                )
            ''')
            
            conn.commit()
            conn.close()
            self.logger.info("数据库初始化完成")
            
        except sqlite3.Error as e:
            self.logger.error(f"数据库初始化失败: {e}")
            self.logger.error(f"数据库文件路径: {self.db_file}")
            self.logger.error(f"当前工作目录: {os.getcwd()}")
            raise

    def get_photos_by_category(self, category_slug: str, count: int = 10):
        """按分类获取图片"""
        try:
            url = f"{self.base_url}/photos/random"
            params = {
                'count': count,
                'query': category_slug,
                'orientation': self.get_random_orientation()
            }
            
            start_time = time.time()
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                photos = response.json()
                # 为每张图片添加分类信息
                for photo in photos:
                    photo['category_slug'] = category_slug
                    photo['category_name'] = Config.get_category_name(category_slug)
                    # 记录API请求信息
                    photo['api_request_time'] = response_time
                    photo['api_request_id'] = hashlib.md5(f"{category_slug}_{datetime.now().timestamp()}".encode()).hexdigest()[:8]
                return photos
            else:
                self.logger.error(f"API 请求失败: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"网络错误: {e}")
            return None

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

    def save_image_info(self, photo_data: Dict, filename: str, category_slug: str, category_name: str, file_size: int = 0, file_hash: str = "") -> bool:
        """保存图片信息到数据库"""
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
            
            # 插入图片信息
            cursor.execute('''
                INSERT OR REPLACE INTO images (
                    id, filename, description, alt_description, user_name, user_username, user_id,
                    image_url_raw, image_url_full, image_url_regular, image_url_small, image_url_thumb,
                    download_time, width, height, color, likes, tags, category, category_slug,
                    created_at, updated_at, exif_data, location_data, file_size, file_hash, api_request_id, unsplash_link
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
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
                file_hash,
                photo_data.get('api_request_id', ''),
                photo_data.get('links', {}).get('html', '')
            ))
            
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
            return False

        try:
            # 获取分类信息
            category_slug = photo_data.get('category_slug', 'other')
            category_name = photo_data.get('category_name', '其他')
            
            # 如果分类不在官方分类中，使用"其他"
            if category_slug not in Config.UNSPLASH_CATEGORIES:
                category_name = '其他'
            
            category_dir = self.get_category_directory(category_name)
            
            # 选择最高质量的图片URL
            image_url = photo_data['urls']['raw']
            
            # 记录下载链接访问
            self.record_download_url(
                photo_id, 
                'raw_download', 
                image_url,
                status_code=200,
                response_time=photo_data.get('api_request_time', 0)
            )
            
            self.logger.info(f"开始下载图片: {photo_id} -> 分类: {category_name}")
            self.logger.debug(f"下载链接: {image_url}")
            
            # 下载图片
            start_time = time.time()
            response = requests.get(image_url, stream=True, timeout=60)
            response_time = time.time() - start_time
            response.raise_for_status()
            
            # 记录响应信息
            self.record_download_url(
                photo_id,
                'image_response',
                image_url,
                status_code=response.status_code,
                response_time=response_time
            )
            
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
            if self.save_image_info(photo_data, filename, category_slug, category_name, file_size, file_hash):
                self.logger.info(f"成功下载并保存信息: {filename} -> {category_name} ({file_size} bytes)")
                return True
            else:
                self.logger.error(f"下载成功但保存信息失败: {filename}")
                # 如果保存信息失败，删除已下载的图片文件
                if filepath.exists():
                    filepath.unlink()
                return False
            
        except Exception as e:
            self.logger.error(f"下载失败 {photo_id}: {e}")
            
            # 记录错误信息
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

    def get_download_stats(self) -> Dict:
        """获取下载统计"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # 总下载数
            cursor.execute("SELECT COUNT(*) FROM images")
            total_images = cursor.fetchone()[0]
            
            # 总文件大小
            cursor.execute("SELECT SUM(file_size) FROM images WHERE file_size > 0")
            total_file_size = cursor.fetchone()[0] or 0
            
            # 今日下载数
            today = datetime.now().strftime("%Y-%m-%d")
            cursor.execute(
                "SELECT total_downloaded, failed_downloads, total_file_size FROM download_stats WHERE date = ?",
                (today,)
            )
            today_stats = cursor.fetchone()
            today_downloaded = today_stats[0] if today_stats else 0
            today_failed = today_stats[1] if today_stats else 0
            today_file_size = today_stats[2] if today_stats else 0
            
            # 分类统计
            cursor.execute("SELECT category, count FROM category_stats ORDER BY count DESC")
            category_stats = cursor.fetchall()
            
            # 标签统计
            cursor.execute("SELECT tag, COUNT(*) FROM image_tags GROUP BY tag ORDER BY COUNT(*) DESC LIMIT 10")
            top_tags = cursor.fetchall()
            
            # 错误统计
            cursor.execute("SELECT COUNT(*) FROM error_logs")
            total_errors = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                "total_images": total_images,
                "total_file_size": total_file_size,
                "today_downloaded": today_downloaded,
                "today_failed": today_failed,
                "today_file_size": today_file_size,
                "category_stats": category_stats,
                "top_tags": top_tags,
                "total_errors": total_errors
            }
            
        except sqlite3.Error as e:
            self.logger.error(f"获取统计失败: {e}")
            return {}

    def print_category_summary(self):
        """打印分类摘要"""
        try:
            # 统计每个分类的实际文件数量
            category_counts = {}
            total_files = 0
            total_size = 0
            
            for category_slug in Config.UNSPLASH_CATEGORIES.keys():
                category_name = Config.get_category_name(category_slug)
                category_dir = self.get_category_directory(category_name)
                if category_dir.exists():
                    files = list(category_dir.glob("*.jpg"))
                    category_size = sum(f.stat().st_size for f in files)
                    category_counts[category_name] = {
                        'count': len(files),
                        'size': category_size
                    }
                    total_files += len(files)
                    total_size += category_size
            
            # 统计"其他"分类
            other_dir = self.get_category_directory('其他')
            if other_dir.exists():
                files = list(other_dir.glob("*.jpg"))
                other_size = sum(f.stat().st_size for f in files)
                category_counts['其他'] = {
                    'count': len(files),
                    'size': other_size
                }
                total_files += len(files)
                total_size += other_size
            
            self.logger.info("=== 分类统计摘要 ===")
            for category, info in sorted(category_counts.items(), key=lambda x: x[1]['count'], reverse=True):
                count = info['count']
                size_mb = info['size'] / (1024 * 1024)
                if count > 0:
                    percentage = (count / total_files) * 100 if total_files > 0 else 0
                    self.logger.info(f"{category:10}: {count:4} 张 ({percentage:5.1f}%) - {size_mb:6.1f} MB")
            
            total_size_mb = total_size / (1024 * 1024)
            self.logger.info(f"总计: {total_files} 张图片 - {total_size_mb:.1f} MB")
            
        except Exception as e:
            self.logger.error(f"生成分类摘要失败: {e}")

    def run_category_based_download(self):
        """基于分类的下载循环"""
        self.logger.info(f"开始基于分类的下载，批次大小: {self.batch_size}")
        self.logger.info("使用 Unsplash 官方分类系统")
        self.logger.info(f"URL 跟踪: {'启用' if self.enable_url_logging else '禁用'}")
        
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        # 初始分类摘要
        self.print_category_summary()
        
        while True:
            try:
                # 随机选择一个分类
                selected_category = self.get_random_category()
                category_name = Config.get_category_name(selected_category)
                
                self.logger.info(f"从分类 '{category_name}' 请求 {self.batch_size} 张图片...")
                
                # 按分类获取图片
                photos = self.get_photos_by_category(selected_category, self.batch_size)
                
                if photos and isinstance(photos, list):
                    downloaded_count = 0
                    
                    # 下载每张图片
                    for photo in photos:
                        if isinstance(photo, dict) and 'id' in photo:
                            if self.download_image(photo):
                                downloaded_count += 1
                                # 每张图片下载后等待一下
                                time.sleep(self.download_interval)
                    
                    stats = self.get_download_stats()
                    self.logger.info(stats)
                    self.logger.info(f"分类 '{category_name}' 下载完成: {downloaded_count}/{len(photos)} 张新图片")
                    
                    # 显示分类分布
                    if downloaded_count > 0:
                        self.logger.info("当前分类分布:")
                        for category, count in stats['category_stats']:
                            self.logger.info(f"  {category}: {count} 张")
                    
                    self.logger.info(f"累计下载: {stats['total_images']} 张图片 - {stats['total_file_size'] / (1024*1024):.1f} MB")
                    
                    consecutive_errors = 0
                else:
                    consecutive_errors += 1
                    self.logger.warning(f"获取图片失败，连续错误次数: {consecutive_errors}")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        self.logger.error("连续错误过多，暂停 10 分钟")
                        time.sleep(600)
                        consecutive_errors = 0
                
                # 每下载3批次后打印一次完整分类摘要
                if downloaded_count > 0:
                    self.print_category_summary()
                
                # 等待下一批次请求
                self.logger.info(f"等待 {self.request_interval} 秒后进行下一批次请求")
                time.sleep(self.request_interval)
                
            except KeyboardInterrupt:
                stats = self.get_download_stats()
                self.logger.info(f"用户中断下载")
                self.print_category_summary()
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
        downloader.run_category_based_download()
        
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

