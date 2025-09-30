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
    "shell")
        echo "启动交互式 Shell..."
        exec /bin/bash
        ;;
    *)
        echo "用法: $0 {download|stats|search|shell}"
        echo ""
        echo "命令说明:"
        echo "  download - 启动图片下载器"
        echo "  stats    - 显示下载统计"
        echo "  search   - 搜索图片"
        echo "  shell    - 启动交互式 Shell"
        exit 1
        ;;
esac

