import pymysql
from pymysql import Error
from datetime import datetime, timedelta
from config import db_host, db_port, db_database, db_user, db_pass

class UserManager:
    """管理代币数据库操作的类"""

    def __init__(self, host=db_host, database=db_database, db_port=db_port,
                 user=db_user, password=db_pass):
        """初始化数据库连接参数"""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = db_port
        self.connection = None

    def connect(self):
        """连接到MySQL数据库"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            return True
        except Error as e:
            print(f"连接MySQL时发生错误: {e}")
            return False

    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
            print("数据库连接已关闭")

    def create_table(self):
        """创建数据表（如果不存在）"""
        if not self.connection:
            if not self.connect():
                return False
        try:
            with self.connection.cursor() as cursor:
                # 创建表的SQL语句
                create_table_query = """
                CREATE TABLE IF NOT EXISTS user (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    webhook_url VARCHAR(255),
                    phone VARCHAR(100),
                    email VARCHAR(50),
                    subscrible_by DATETIME COMMENT '订阅过期时间',
                    trail_by  DATETIME COMMENT '试用过期时间',
                    created_time DATETIME,
                    created_by VARCHAR(100),
                    updated_time DATETIME,
                    updated_by VARCHAR(100)
                )
                """
                cursor.execute(create_table_query)
            self.connection.commit()
            print("数据表创建成功或已存在")
            return True
        except Error as e:
            print(f"创建表时发生错误: {e}")
            return False

    def insert_user(self, user_data):
        """
        插入单个代币数据到数据库
        参数:
            token_data (dict): 包含代币信息的字典
        返回:
            bool: 操作是否成功
            int: 如果成功，返回新插入记录的ID；如果失败，返回None
        """
        try:
            # 建立数据库连接
            if not self.connection:
                self.connect()
            # 插入数据
            with self.connection.cursor() as cursor:
                current_time = datetime.now()

                # 准备插入数据的SQL语句
                insert_query = """
                INSERT INTO user 
                (name, webhook_url, phone, email, subscrible_by, trail_by
                 created_time, created_by, updated_time, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # 准备数据
                data_tuple = (
                    user_data.get('name', ''),
                    user_data.get('webhook_url', ''),
                    user_data.get('phone', ''),
                    user_data.get('email', 0.0),
                    current_time + timedelta(days=7),
                    current_time + timedelta(days=7),
                    current_time,
                    'system',
                    current_time,
                    'system',
                )

                # 执行SQL语句
                cursor.execute(insert_query, data_tuple)
                self.connection.commit()

                # 获取新插入记录的ID
                new_id = cursor.lastrowid
                print(f"成功插入用户记录，ID: {new_id}")

                return True, new_id
        except Error as e:
            print(f"插入数据时发生错误: {e}")
            return False, None


    def query_users(self, name=None, email=None, wecom_userid=None, phone=None, is_deleted=0):
        """查询数据库中的用户数据"""
        if not self.connection:
            if not self.connect():
                return []

        try:
            with self.connection.cursor() as cursor:
                # 基础查询
                query = f"SELECT * FROM user WHERE is_deleted={is_deleted}"
                params = []

                # 添加条件
                if name:
                    query += " AND name = %s"
                    params.append(name)

                if phone:
                    query += " AND phone = %s"
                    params.append(phone)

                if email:
                    query += " AND email = %s"
                    params.append(email)

                if wecom_userid:
                    query += " AND wecom_userid = %s"
                    params.append(wecom_userid)

                # 执行查询
                cursor.execute(query, params)
                results = cursor.fetchall()

                return results

        except Error as e:
            print(f"查询数据时发生错误: {e}")
            return []

    def get_user_by_id(self, user_id):
        """根据ID查询特定的用户记录"""
        if not self.connection:
            if not self.connect():
                return None

        try:
            with self.connection.cursor() as cursor:
                query = "SELECT * FROM user WHERE id = %s"
                cursor.execute(query, (user_id,))
                result = cursor.fetchone()
                return result

        except Error as e:
            print(f"根据ID查询数据时发生错误: {e}")
            return None

    def update_user(self, user_id, data, updated_by="system"):
        """更新用户数据"""
        if not self.connection:
            if not self.connect():
                return False

        try:
            with self.connection.cursor() as cursor:
                # 准备基础更新查询
                update_query = "UPDATE user SET "
                update_parts = []
                params = []

                # 添加要更新的字段
                for key, value in data.items():
                    if key in ['name', 'phone', 'email', 'webhook_url', 'is_deleted']:
                        update_parts.append(f"{key} = %s")
                        params.append(value)

                # 添加更新时间和更新人
                update_parts.append("updated_time = %s")
                params.append(datetime.now())
                update_parts.append("updated_by = %s")
                params.append(updated_by)

                # 完成查询字符串
                update_query += ", ".join(update_parts)
                update_query += " WHERE id = %s"
                params.append(user_id)

                # 执行更新
                cursor.execute(update_query, params)

            self.connection.commit()
            print(f"成功更新ID为 {user_id} 的记录")
            return True

        except Error as e:
            print(f"更新数据时发生错误: {e}")
            return False

    def delete_user(self, user_id):
        """删除用户数据"""
        if not self.connection:
            if not self.connect():
                return False

        try:
            # with self.connection.cursor() as cursor:
            #     delete_query = "DELETE FROM user WHERE id = %s"
            #     cursor.execute(delete_query, (user_id,))
            #
            # self.connection.commit()
            # print(f"成功删除ID为 {user_id} 的记录")
            self.update_user(user_id, {"is_deleted": 1})
            return True

        except Error as e:
            print(f"删除数据时发生错误: {e}")
            return False


# 使用示例
def main():
    # 创建数据库管理器实例
    db_manager = UserManager()
    # purchased_tokens = [
    #     {'spot_exchange': 'Bybit', 'future_exchange': 'Bybit', 'token': 'AVL', 'totalAmount': 720.0,
    #      'user': 'zxl',
    #      'webhook_url': 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=38fd27ea-8569-4de2-9dee-4c4a4ffb77ed'},
    #     {'spot_exchange': 'GateIO', 'future_exchange': 'Bybit', 'token': 'B3', 'totalAmount': 75050.0,
    #      'user': 'zxl',
    #      'webhook_url': 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=38fd27ea-8569-4de2-9dee-4c4a4ffb77ed'},
    # ]

    try:
        # 连接到数据库
        if db_manager.connect():
            print("成功连接到数据库")

            # # 创建表
            # db_manager.create_table()
            #
            # 插入数据
            # for token in purchased_tokens:
            #     db_manager.insert_token(token)

            # 查询数据
            print("\n查询Bybit交易所的用户数据:")
            # results = db_manager.query_tokens(spot_exchange='Bybit', token='AVL')
            results = db_manager.query_users(wecom_userid='raymon-ZhuXiuLong')
            for row in results:
                print(row)

                # 如果找到了记录，尝试更新它
                # if row:
                #     token_id = row['id']
                #     # 更新记录
                #     update_data = {'totalAmount': 750.0}
                #     db_manager.update_token(token_id, update_data, "admin")
                #
                #     # 查看更新后的记录
                #     updated_token = db_manager.get_token_by_id(token_id)
                #     print("\n更新后的记录:")
                #     print(updated_token)

    finally:
        # 关闭数据库连接
        db_manager.disconnect()


if __name__ == "__main__":
    main()
