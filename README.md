# unsplash-downloader
通过unsplash开发者API进行图片下载


# 申请 unsplash 开发者账号

https://unsplash.com/developers



# 修改 .env

copy .env_example  .env

vim .env 修改 UNSPLASH_ACCESS_KEY

<img width="1493" height="552" alt="image" src="https://github.com/user-attachments/assets/ed499c0d-351e-4e1d-91ea-4d8815f616ff" />  



# 运行部署

docker-compose build  

docker-compose up -d

docker-compose logs-f



# db_manager

docker-compose --profile tools run --rm db-manager stats 显示统计

docker-compose --profile tools run --rm db-manager search <关键词> 搜索图片

docker-compose --profile tools run --rm db-manager category <分类名> 显示分类图片

docker-compose --profile tools run --rm db-manager categories 显示所有分类          

docker-compose --profile tools run --rm db-manager detail <图片ID> 显示图片详情

docker-compose --profile tools run --rm db-manager urls <图片ID> 显示下载连接

docker-compose --profile tools run --rm db-manager errors 显示错误日志 // TODO

docker-compose --profile tools run --rm db-manager tables 显示所有表 

docker-compose --profile tools run --rm db-manager health 检查数据库健康状态

docker-compose --profile tools run --rm db-manager repair 修复数据库 //TODO  

docker-compose --profile tools run --rm db-manager init 初始化数据库 



