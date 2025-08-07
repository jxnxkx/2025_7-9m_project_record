import pymysql
import time
import random
from datetime import datetime
import logging
import json
import os

# 0. 数据库连接测试 

# conn = pymysql.connect(host='192.238.222.34',user='dem0_test',password='dem0_password',db='dem0_test',port=3306,charset='utf8')

# if conn:
#     print("连接成功!")
# else:
#     print(f"连接失败")

# cur = conn.cursor()
# cur2 = conn.cursor()

# cur.execute('SELECT * FROM dem0_test')
# cur2.execute('desc dem0_test')

# result = cur.fetchall()
# result2 = cur2.fetchall()

# print(result)
# print(result2)


# 1. 初始化日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# 2. 数据库配置（建议从环境变量或配置文件中读取敏感信息）
DB_CONFIG = {
    'host': '192.238.222.34',
    'user': 'dem0_test',
    'password': 'dem0_password',
    'db': 'dem0_test',
    'port': 3306,
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# 3. 北京所有行政区
DISTRICTS = [
    "东城区", "西城区", "朝阳区", "丰台区", "石景山区",
    "海淀区", "顺义区", "通州区", "大兴区", "房山区",
    "门头沟区", "昌平区", "平谷区", "密云区", "怀柔区", "延庆区"
]

# 4. 数据库连接类（封装PyMySQL操作）[1,2](@ref)
class Database:
    def __init__(self, config):
        self.config = config
        self.connection = None
        
    def __enter__(self):
        try:
            self.connection = pymysql.connect(**self.config)
            logging.info("数据库连接成功")
            return self.connection.cursor()
        except pymysql.Error as e:
            logging.error(f"数据库连接失败: {e}")
            raise
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            if exc_type is None:
                self.connection.commit()
            else:
                self.connection.rollback()
            self.connection.close()
            logging.info("数据库连接已关闭")

# 5. PM2.5生成逻辑（随时间波动）
def generate_pm25():
    current_hour = datetime.now().hour
    
    # 根据时间段设置基准值 (微克/立方米)
    if 5 <= current_hour < 9:    # 早高峰
        base = 85.0
    elif 9 <= current_hour < 12: # 上午
        base = 75.0
    elif 12 <= current_hour < 14: # 中午最低
        base = 55.0
    elif 14 <= current_hour < 18: # 下午
        base = 65.0
    elif 18 <= current_hour < 21: # 晚高峰
        base = 90.0
    else:                        # 夜间
        base = 80.0
    
    # 添加随机波动 (±15)
    return round(base + random.uniform(-15, 15), 1)

# 6. 生成监测站点，使用种子随机
def generate_stations():
    # 创建独立的随机数生成器
    local_rng = random.Random(42)  # 局部固定种子
    stations = {}
    for district in DISTRICTS:
        station_count = local_rng.randint(3, 5)  # 使用局部生成器
        stations[district] = [f"{district}监测站{i+1}" for i in range(station_count)]
    return stations

# 7. 数据库操作[6,8](@ref)
def insert_data():
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    stations = generate_stations()
    total_records = sum(len(s) for s in stations.values())
    
    try:
        with Database(DB_CONFIG) as cursor:
            # 为每个区的每个站点生成数据
            batch_data = []
            for district, station_list in stations.items():
                for station in station_list:
                    pm25 = generate_pm25()
                    batch_data.append((current_time, district, station, pm25))
            
            # 批量插入数据（提高性能）[6,8](@ref)
            sql = """
            INSERT INTO dem0 (data, district, station, PM)
            VALUES (%s, %s, %s, %s)
            """
            cursor.executemany(sql, batch_data)
            
        logging.info(f"{current_time} 插入成功，共插入 {total_records} 条记录")
        return True
        
    except pymysql.Error as e:
        logging.error(f"数据库操作失败: {e}")
        return False

# 8. 主程序[5](@ref)
def main():
    logging.info("PM2.5监测数据生成器已启动...")
    
    try:
        while True:
            success = insert_data()
            if not success:
                logging.warning("插入失败，将在30秒后重试...")
                time.sleep(30)
            else:
                time.sleep(60)  # 正常等待60秒
                
    except KeyboardInterrupt:
        logging.info("\n程序已终止")
    except Exception as e:
        logging.critical(f"程序意外终止: {e}")

if __name__ == "__main__":
    main()