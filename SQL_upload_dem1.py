import sys
from pathlib import Path
import pymysql                          # 连接，语句操作
from dbutils.pooled_db import PooledDB  # 引入连接池，防止断链
import time
import random
from datetime import datetime, timedelta
import logging


# 初始化日志配置 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# 数据库配置
DB_CONFIG = {
    'host': '192.238.222.34',       # 服务器地址
    'user': 'dem0_test',            # 数据库账号
    'password': 'dem0_password',    # 数据库密码
    'db': 'dem0_test',              # 数据库名称
    'port': 3306,                   # 数据库端口
    'charset': 'utf8mb4',           # 编码格式
    'connect_timeout': 5,           # 连接超时5秒 
    'read_timeout': 10,             # 读取超时10秒
    'write_timeout': 10             # 写入超时10秒
}

# 创建全局连接池 
CONNECTION_POOL = PooledDB(
    creator=pymysql,
    mincached=2,        # 初始空闲连接
    maxcached=5,        # 最大空闲连接
    maxconnections=20,  # 总连接上限
    blocking=True,      # 连接耗尽时阻塞等待
    **DB_CONFIG
)

# PCB故障类型及其概率分布
FAULT_TYPES = [
    "裂纹",         # 25.1%
    "氧化发色",      # 22.3%
    "残膜",         # 22.3%
    "露铜",         # 15.3%
    "凹陷点",        # 9.6%
    "异物点",        # 2.5%
    "板边异物"       # 2.5%
]

# 故障类型权重（根据要求的比例）
FAULT_WEIGHTS = [25.1, 22.3, 22.3, 15.3, 9.6, 2.5, 2.5]

# PCB故障总概率
OVERALL_FAULT_PROBABILITY = 0.0742  # 7.42%

class Database:
    def __init__(self, pool):
        self.pool = pool
        
    def __enter__(self):
        try:
            self.connection = self.pool.connection()  # 从连接池获取连接
            return self.connection.cursor()
        except pymysql.Error as e:
            logging.error(f"数据库连接失败: {e}")
            raise
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'connection') and self.connection:
            try:
                if exc_type is None:
                    self.connection.commit()
                else:
                    logging.error("发生错误，回滚事务")
                    self.connection.rollback()
            finally:
                self.connection.close()  # 实际是归还到连接池

def generate_fault_data():
    """生成PCB检测数据"""
    current_time = datetime.now()
    
    # 决定是否有故障
    has_fault = random.random() < OVERALL_FAULT_PROBABILITY
    
    fault_type = None
    if has_fault:
        # 根据权重随机选择故障类型
        fault_type = random.choices(FAULT_TYPES, weights=FAULT_WEIGHTS, k=1)[0]
        logging.info(f"检测到故障: {fault_type}")
    else:
        logging.info("检测正常")
    
    return (current_time, has_fault, fault_type)

def create_table_if_needed():
    """创建表结构（如果不存在）"""
    try:
        with Database(CONNECTION_POOL) as cursor:
            cursor.execute("SHOW TABLES LIKE 'pcb_fault_data'")
            if not cursor.fetchone():
                create_table_sql = """
                CREATE TABLE pcb_fault_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    detection_time DATETIME NOT NULL,
                    has_fault BOOLEAN NOT NULL,
                    fault_type VARCHAR(20)
                )
                """
                cursor.execute(create_table_sql)
                logging.info("创建数据表成功")
    except pymysql.Error as e:
        logging.error(f"创建表时出错: {e}")

def insert_data(data):
    """插入数据到数据库（带重试机制）"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with Database(CONNECTION_POOL) as cursor:
                sql = """
                INSERT INTO pcb_fault_data (detection_time, has_fault, fault_type)
                VALUES (%s, %s, %s)
                """
                cursor.execute(sql, data)
            logging.info("数据插入成功")
            return True
        except pymysql.OperationalError as e:
            wait_time = 2 ** attempt  # 指数退避策略
            logging.warning(f"数据库错误({e})，等待{wait_time}秒后重试({attempt+1}/{max_retries})")
            time.sleep(wait_time)
    logging.error("插入数据失败，已达最大重试次数")
    return False

def main():
    logging.info("PCB故障检测数据生成器已启动...")
    
    # 创建表（如果不存在）
    create_table_if_needed()
    
    # 初始时间戳（每10秒一个PCB检测）
    last_detection_time = datetime.now()
    last_heartbeat = datetime.now()  # 心跳计时
    
    try:
        while True:
            current_time = datetime.now()
            
            # 每10分钟发送心跳包保持连接活性
            if (current_time - last_heartbeat) > timedelta(minutes=10):
                try:
                    with Database(CONNECTION_POOL) as cursor:
                        cursor.execute("SELECT 1")
                    last_heartbeat = current_time
                    logging.info("数据库心跳检测成功")
                except Exception as e:
                    logging.warning(f"心跳检测失败: {e}")
            
            # 如果距离上次检测超过10秒
            if (current_time - last_detection_time) > timedelta(seconds=10):
                # 生成检测数据
                data = generate_fault_data()
                
                # 插入数据库
                success = insert_data(data)
                if success:
                    last_detection_time = current_time
                else:
                    # 如果插入失败，等待30秒后继续
                    time.sleep(30)
            
            # 避免CPU高占用
            time.sleep(1)
                
    except KeyboardInterrupt:
        logging.info("\n程序已终止")
    except Exception as e:
        logging.critical(f"程序意外终止: {e}")

if __name__ == "__main__":
    main()