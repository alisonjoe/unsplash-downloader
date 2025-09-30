import sqlite3
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path
import json

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.config import Config

class UnsplashDBManager:
    def __init__(self):
        self.db_file = Config.DB_FILE
        self.init_database()
    
    def init_database(self):
        """初始化数据库表结构"""
        try:
            # 确保数据库文件目录存在
            Path(self.db_file).parent.mkdir(exist_ok=True, parents=True)
            
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # 检查表是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='images'")
            table_exists = cursor.fetchone()
            
            if not table_exists:
                print("检测到数据库表不存在，开始创建表结构...")
                
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
                        unsplash_link TEXT
                    )
                ''')
                print("✓ 创建 images 表完成")
                
                # 创建下载统计表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS download_stats (
                        date TEXT PRIMARY KEY,
                        total_downloaded INTEGER DEFAULT 0,
                        failed_downloads INTEGER DEFAULT 0,
                        total_file_size INTEGER DEFAULT 0
                    )
                ''')
                print("✓ 创建 download_stats 表完成")
                
                # 创建分类统计表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS category_stats (
                        category TEXT PRIMARY KEY,
                        category_slug TEXT,
                        count INTEGER DEFAULT 0,
                        last_updated TEXT
                    )
                ''')
                print("✓ 创建 category_stats 表完成")
                
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
                print("✓ 创建 download_urls 表完成")
                
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
                print("✓ 创建 error_logs 表完成")
                
                # 创建图片标签表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS image_tags (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        image_id TEXT,
                        tag TEXT,
                        FOREIGN KEY (image_id) REFERENCES images (id)
                    )
                ''')
                print("✓ 创建 image_tags 表完成")
                
                # 插入初始分类数据
                for category_slug, category_name in Config.UNSPLASH_CATEGORIES.items():
                    cursor.execute('''
                        INSERT OR IGNORE INTO category_stats (category, category_slug, count, last_updated)
                        VALUES (?, ?, 0, ?)
                    ''', (category_name, category_slug, datetime.now().isoformat()))
                
                # 插入"其他"分类
                cursor.execute('''
                    INSERT OR IGNORE INTO category_stats (category, category_slug, count, last_updated)
                    VALUES (?, ?, 0, ?)
                ''', ('其他', 'other', datetime.now().isoformat()))
                
                print("✓ 数据库表结构初始化完成")
            else:
                print("✓ 数据库表已存在")
            
            conn.commit()
            conn.close()
            
        except sqlite3.Error as e:
            print(f"❌ 数据库初始化失败: {e}")
            return False
        return True
    
    def repair_database(self):
        """修复数据库"""
        try:
            print("开始修复数据库...")
            
            # 备份原数据库
            backup_file = f"{self.db_file}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            if Path(self.db_file).exists():
                import shutil
                shutil.copy2(self.db_file, backup_file)
                print(f"✓ 已备份原数据库到: {backup_file}")
            
            # 删除原数据库文件
            if Path(self.db_file).exists():
                Path(self.db_file).unlink()
                print("✓ 已删除损坏的数据库文件")
            
            # 重新初始化
            success = self.init_database()
            if success:
                print("✓ 数据库修复完成")
            else:
                print("❌ 数据库修复失败")
            
            return success
            
        except Exception as e:
            print(f"❌ 数据库修复失败: {e}")
            return False
    
    def check_database_health(self):
        """检查数据库健康状态"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # 检查所有表
            tables = ['images', 'download_stats', 'category_stats', 'download_urls', 'error_logs', 'image_tags']
            missing_tables = []
            
            for table in tables:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
                if not cursor.fetchone():
                    missing_tables.append(table)
            
            # 检查 images 表结构
            if 'images' not in missing_tables:
                cursor.execute("PRAGMA table_info(images)")
                columns = [row[1] for row in cursor.fetchall()]
                required_columns = ['id', 'filename', 'category', 'download_time']
                missing_columns = [col for col in required_columns if col not in columns]
            else:
                missing_columns = required_columns
            
            conn.close()
            
            if not missing_tables and not missing_columns:
                print("✓ 数据库健康状态: 良好")
                return True
            else:
                print("❌ 数据库健康状态: 有问题")
                if missing_tables:
                    print(f"   缺失的表: {', '.join(missing_tables)}")
                if missing_columns:
                    print(f"   缺失的列: {', '.join(missing_columns)}")
                return False
                
        except sqlite3.Error as e:
            print(f"❌ 检查数据库健康状态失败: {e}")
            return False

    def show_stats(self):
        """显示统计信息"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # 总图片数
            cursor.execute("SELECT COUNT(*) FROM images")
            total_images = cursor.fetchone()[0]
            
            # 总文件大小
            cursor.execute("SELECT SUM(file_size) FROM images WHERE file_size IS NOT NULL")
            total_size_bytes = cursor.fetchone()[0] or 0
            total_size_mb = total_size_bytes / (1024 * 1024)
            
            # 总错误数
            cursor.execute("SELECT COUNT(*) FROM error_logs")
            total_errors = cursor.fetchone()[0]
            
            print(f"总图片数: {total_images}")
            print(f"总文件大小: {total_size_mb:.2f} MB")
            print(f"总错误数: {total_errors}")
            
            # 分类统计
            cursor.execute('''
                SELECT category, COUNT(*) as count, SUM(file_size) as size
                FROM images 
                WHERE category IS NOT NULL 
                GROUP BY category 
                ORDER BY count DESC
            ''')
            
            print("\n分类统计:")
            for row in cursor.fetchall():
                category, count, size_bytes = row
                size_mb = (size_bytes or 0) / (1024 * 1024)
                percentage = (count / total_images * 100) if total_images > 0 else 0
                print(f"  {category:12}: {count:4} 张 ({percentage:5.1f}%) - {size_mb:6.1f} MB")
            
            conn.close()
            
        except sqlite3.Error as e:
            print(f"显示统计信息失败: {e}")

    def show_tables(self):
        """显示所有表"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = cursor.fetchall()
            
            if tables:
                print("数据库中的表:")
                for table in tables:
                    # 获取表的行数
                    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                    count = cursor.fetchone()[0]
                    print(f"  {table[0]}: {count} 行")
            else:
                print("数据库中没有表")
            
            conn.close()
            
        except sqlite3.Error as e:
            print(f"显示表失败: {e}")

    def list_categories(self):
        """显示所有分类"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT category, category_slug, count, last_updated 
                FROM category_stats 
                ORDER BY count DESC
            ''')
            
            print("分类统计:")
            for row in cursor.fetchall():
                category, slug, count, updated = row
                print(f"  {category:12} ({slug:10}): {count:4} 张 - 最后更新: {updated[:19]}")
            
            conn.close()
            
        except sqlite3.Error as e:
            print(f"显示分类失败: {e}")

    def search_images(self, keyword: str):
        """搜索图片"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            search_pattern = f"%{keyword}%"
            cursor.execute('''
                SELECT id, filename, description, category, download_time 
                FROM images 
                WHERE filename LIKE ? OR description LIKE ? OR category LIKE ?
                ORDER BY download_time DESC
                LIMIT 50
            ''', (search_pattern, search_pattern, search_pattern))
            
            print(f"搜索 '{keyword}' 的结果:")
            for row in cursor.fetchall():
                image_id, filename, description, category, download_time = row
                desc_preview = description[:50] + "..." if description and len(description) > 50 else description
                print(f"  {image_id} - {category} - {download_time[:19]}")
                print(f"    文件名: {filename}")
                if desc_preview:
                    print(f"    描述: {desc_preview}")
                print()
            
            conn.close()
            
        except sqlite3.Error as e:
            print(f"搜索失败: {e}")

    def show_image_detail(self, image_id: str):
        """显示图片详情"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM images WHERE id = ?
            ''', (image_id,))
            
            row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                print(f"图片详情 - ID: {image_id}")
                for i, column in enumerate(columns):
                    value = row[i]
                    if value is not None:
                        print(f"  {column}: {value}")
            else:
                print(f"未找到图片: {image_id}")
            
            conn.close()
            
        except sqlite3.Error as e:
            print(f"显示图片详情失败: {e}")

    def show_download_urls(self, image_id: str = None):
        """显示下载链接"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            if image_id:
                cursor.execute('''
                    SELECT * FROM download_urls WHERE image_id = ? ORDER BY accessed_time DESC
                ''', (image_id,))
            else:
                cursor.execute('''
                    SELECT * FROM download_urls ORDER BY accessed_time DESC LIMIT 20
                ''')
            
            rows = cursor.fetchall()
            if rows:
                columns = [description[0] for description in cursor.description]
                print("下载链接:")
                for row in rows:
                    print("  " + "-" * 50)
                    for i, column in enumerate(columns):
                        value = row[i]
                        if value is not None:
                            print(f"    {column}: {value}")
            else:
                print("没有下载链接记录")
            
            conn.close()
            
        except sqlite3.Error as e:
            print(f"显示下载链接失败: {e}")

    def show_errors(self):
        """显示错误日志"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM error_logs ORDER BY error_time DESC LIMIT 20
            ''')
            
            rows = cursor.fetchall()
            if rows:
                columns = [description[0] for description in cursor.description]
                print("错误日志:")
                for row in rows:
                    print("  " + "-" * 50)
                    for i, column in enumerate(columns):
                        value = row[i]
                        if value is not None:
                            print(f"    {column}: {value}")
            else:
                print("没有错误记录")
            
            conn.close()
            
        except sqlite3.Error as e:
            print(f"显示错误日志失败: {e}")

def main():
    """数据库管理工具主函数"""
    manager = UnsplashDBManager()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "stats":
            manager.show_stats()
        elif sys.argv[1] == "search" and len(sys.argv) > 2:
            manager.search_images(sys.argv[2])
        elif sys.argv[1] == "category" and len(sys.argv) > 2:
            manager.show_category_images(sys.argv[2])
        elif sys.argv[1] == "categories":
            manager.list_categories()
        elif sys.argv[1] == "detail" and len(sys.argv) > 2:
            manager.show_image_detail(sys.argv[2])
        elif sys.argv[1] == "urls":
            if len(sys.argv) > 2:
                manager.show_download_urls(sys.argv[2])
            else:
                manager.show_download_urls()
        elif sys.argv[1] == "errors":
            manager.show_errors()
        elif sys.argv[1] == "tables":
            manager.show_tables()
        elif sys.argv[1] == "health":
            manager.check_database_health()
        elif sys.argv[1] == "repair":
            manager.repair_database()
        elif sys.argv[1] == "init":
            success = manager.init_database()
            if success:
                print("数据库初始化成功")
            else:
                print("数据库初始化失败")
        else:
            print("用法:")
            print("  python -m src.db_manager stats                # 显示统计")
            print("  python -m src.db_manager search <关键词>      # 搜索图片")
            print("  python -m src.db_manager category <分类名>    # 显示分类图片")
            print("  python -m src.db_manager categories           # 显示所有分类")
            print("  python -m src.db_manager detail <图片ID>      # 显示图片详情")
            print("  python -m src.db_manager urls [图片ID]        # 显示下载链接")
            print("  python -m src.db_manager errors               # 显示错误日志")
            print("  python -m src.db_manager tables               # 显示所有表")
            print("  python -m src.db_manager health               # 检查数据库健康状态")
            print("  python -m src.db_manager repair               # 修复数据库")
            print("  python -m src.db_manager init                 # 初始化数据库")
    else:
        # 默认检查数据库健康状态
        if manager.check_database_health():
            manager.show_stats()
        else:
            print("数据库有问题，请运行 'python -m src.db_manager repair' 来修复")

if __name__ == "__main__":
    main()

