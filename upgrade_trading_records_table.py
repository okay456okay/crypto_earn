#!/usr/bin/env python3
"""
交易记录表升级脚本
为现有的trading_records表添加平仓相关字段：
- close_price: 平仓价格
- close_order_id: 平仓订单ID
- close_order_status: 平仓订单状态
- close_time: 平仓时间
"""

import pymysql
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from config import mysql_config
except ImportError:
    # 如果没有config.py，使用默认配置
    mysql_config = {
        'host': 'localhost',
        'user': 'crypt_earn',
        'password': 'XkKz^t$jGm',
        'database': 'crypt_earn',
        'charset': 'utf8mb4',
        'port': 3306
    }

def check_column_exists(cursor, table_name, column_name):
    """检查表中是否存在指定列"""
    cursor.execute(f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = %s 
        AND COLUMN_NAME = %s
    """, (mysql_config['database'], table_name, column_name))
    
    result = cursor.fetchone()
    return result[0] > 0

def upgrade_trading_records_table():
    """升级交易记录表，添加平仓相关字段"""
    try:
        print("🔧 开始升级trading_records表...")
        
        # 连接数据库
        conn = pymysql.connect(**mysql_config)
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = %s 
            AND TABLE_NAME = 'trading_records'
        """, (mysql_config['database'],))
        
        if cursor.fetchone()[0] == 0:
            print("❌ trading_records表不存在，请先创建表")
            return False
        
        print("✅ 找到trading_records表")
        
        # 需要添加的字段列表
        columns_to_add = [
            {
                'name': 'close_price',
                'definition': 'DECIMAL(20,8) DEFAULT NULL COMMENT "平仓价格"'
            },
            {
                'name': 'close_order_id',
                'definition': 'VARCHAR(100) DEFAULT NULL COMMENT "平仓订单ID"'
            },
            {
                'name': 'close_order_status',
                'definition': 'VARCHAR(20) DEFAULT "OPEN" COMMENT "平仓订单状态(OPEN/FILLED/CANCELLED/CLOSED)"'
            },
            {
                'name': 'close_time',
                'definition': 'TIMESTAMP NULL COMMENT "平仓时间"'
            }
        ]
        
        # 逐个添加字段
        for column in columns_to_add:
            if not check_column_exists(cursor, 'trading_records', column['name']):
                print(f"🔨 添加字段: {column['name']}")
                alter_sql = f"ALTER TABLE trading_records ADD COLUMN {column['name']} {column['definition']}"
                cursor.execute(alter_sql)
                print(f"✅ 成功添加字段: {column['name']}")
            else:
                print(f"⏭️ 字段已存在，跳过: {column['name']}")
        
        # 添加索引
        try:
            cursor.execute("ALTER TABLE trading_records ADD INDEX idx_close_status (close_order_status)")
            print("✅ 成功添加索引: idx_close_status")
        except pymysql.Error as e:
            if "Duplicate key name" in str(e):
                print("⏭️ 索引已存在，跳过: idx_close_status")
            else:
                print(f"⚠️ 添加索引失败: {e}")
        
        # 提交更改
        conn.commit()
        
        # 显示表结构
        print("\n📋 升级后的表结构:")
        cursor.execute("DESCRIBE trading_records")
        columns = cursor.fetchall()
        
        for column in columns:
            print(f"   {column[0]} | {column[1]} | {column[2]} | {column[3]} | {column[4]} | {column[5]}")
        
        # 统计现有数据
        cursor.execute("SELECT COUNT(*) FROM trading_records")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM trading_records WHERE close_order_status = 'OPEN'")
        open_records = cursor.fetchone()[0]
        
        print(f"\n📊 数据统计:")
        print(f"   总交易记录数: {total_records}")
        print(f"   未平仓记录数: {open_records}")
        print(f"   已平仓记录数: {total_records - open_records}")
        
        conn.close()
        print("\n🎉 trading_records表升级完成！")
        return True
        
    except Exception as e:
        print(f"❌ 升级失败: {e}")
        return False

def main():
    """主函数"""
    print("=" * 60)
    print("🚀 交易记录表升级脚本")
    print("=" * 60)
    
    try:
        success = upgrade_trading_records_table()
        if success:
            print("\n✅ 升级成功！现在可以使用新的平仓功能")
            print("\n💡 升级后的功能:")
            print("   • 记录平仓价格和订单信息")
            print("   • 自动检查和更新平仓订单状态")
            print("   • 统计时自动排除已平仓记录")
            print("   • 支持平仓状态管理")
        else:
            print("\n❌ 升级失败，请检查错误信息")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n❌ 用户中断升级")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 升级过程中发生未知错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 