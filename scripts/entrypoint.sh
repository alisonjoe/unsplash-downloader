#!/bin/bash

# 设置环境
set -e

echo "Unsplash Downloader 启动中..."

# 检查必要的环境变量
if [ -z "$UNSPLASH_ACCESS_KEY" ]; then
    echo "错误: UNSPLASH_ACCESS_KEY 环境变量未设置"
    exit 1
fi

# 根据命令执行不同的操作
case "$1" in
    "download")
        echo "启动下载器..."
        exec python -m src.unsplash_downloader
        ;;
    "stats")
        echo "显示统计信息..."
        exec python -m src.db_manager stats
        ;;
    "search")
        if [ -z "$2" ]; then
            echo "用法: search <关键词>"
            exit 1
        fi
        echo "搜索图片: $2"
        exec python -m src.db_manager search "$2"
        ;;
    "category")
        if [ -z "$2" ]; then
            echo "用法: category <分类名>"
            exit 1
        fi
        echo "显示分类图片: $2"
        exec python -m src.db_manager category "$2"
        ;;
    "tables")
        echo "显示所有表"
        exec python -m src.db_manager tables
        ;;
    "health")
        echo "检查数据库健康状态"
        exec python -m src.db_manager health
        ;;
    "repair")
        echo "修复数据库"
        exec python -m src.db_manager repair
        ;;
    "init")
        echo "初始化数据库"
        exec python -m src.db_manager init
        ;;
    "categories")
        echo "显示所有分类"
        exec python -m src.db_manager categories
        ;;
    "detail")
        if [ -z "$2" ]; then
            echo "用法: detail <图片ID>"
            exit 1
        fi
        echo "显示图片详情: $2"
        exec python -m src.db_manager detail "$2"
        ;;
    "urls")
        if [ -z "$2" ]; then
            echo "用法: urls <图片ID>"
            exit 1
        fi
        echo "显示下载链接: $2"
        exec python -m src.db_manager urls "$2"
        ;;
    "shell")
        echo "启动交互式 Shell..."
        exec /bin/bash
        ;;
    *)
        echo "用法: $0 {download|stats|search|category|tables|health|repair|init|categories|detail|urls|shell}"
        echo ""
        echo "命令说明:"
        echo "  download   - 启动图片下载器"
        echo "  stats      - 显示下载统计"
        echo "  search     - 搜索图片"
        echo "  category   - 显示分类图片"
        echo "  tables     - 显示所有表"
        echo "  health     - 检查数据库健康状态"
        echo "  repair     - 修复数据库"
        echo "  init       - 初始化数据库"
        echo "  categories - 显示所有分类"
        echo "  detail     - 显示图片详情"
        echo "  urls       - 显示下载链接"
        echo "  shell      - 启动交互式 Shell"
        exit 1
        ;;
esac
