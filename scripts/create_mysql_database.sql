-- MySQL数据库初始化脚本
-- Binance价格高点扫描器数据库 (混合K线版本)

-- 创建数据库
CREATE DATABASE IF NOT EXISTS crypt_earn 
  DEFAULT CHARACTER SET utf8mb4 
  DEFAULT COLLATE utf8mb4_unicode_ci;

-- 使用数据库
USE crypt_earn;

-- 创建用户（如果不存在）
-- CREATE USER IF NOT EXISTS 'crypt_earn'@'localhost' IDENTIFIED BY 'XkKz^t$jGm';
-- GRANT ALL PRIVILEGES ON crypt_earn.* TO 'crypt_earn'@'localhost';
-- FLUSH PRIVILEGES;

-- 创建交易记录表
CREATE TABLE IF NOT EXISTS trading_records (
    id INT PRIMARY KEY AUTO_INCREMENT,
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    order_time TIMESTAMP NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    quantity DECIMAL(20,8) NOT NULL,
    leverage INT NOT NULL,
    direction VARCHAR(10) NOT NULL,
    order_id VARCHAR(100),
    margin_amount DECIMAL(20,8) NOT NULL,
    current_price DECIMAL(20,8) DEFAULT 0.0,
    price_change_percent DECIMAL(10,4) DEFAULT 0.0,
    pnl_amount DECIMAL(20,8) DEFAULT 0.0,
    price_update_time TIMESTAMP NULL,
    close_price DECIMAL(20,8) DEFAULT NULL,
    close_order_id VARCHAR(100) DEFAULT NULL,
    close_order_status VARCHAR(20) DEFAULT 'OPEN',
    close_time TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_trade (exchange, symbol, order_time),
    INDEX idx_symbol (symbol),
    INDEX idx_order_time (order_time),
    INDEX idx_close_status (close_order_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建1分钟K线数据表（当天数据）
CREATE TABLE IF NOT EXISTS kline_data_1min (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    symbol VARCHAR(50) NOT NULL,
    open_time BIGINT NOT NULL,
    close_time BIGINT NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    volume DECIMAL(20,8) NOT NULL,
    quote_volume DECIMAL(20,8) NOT NULL,
    trades_count INT NOT NULL,
    taker_buy_base_volume DECIMAL(20,8) NOT NULL,
    taker_buy_quote_volume DECIMAL(20,8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_kline_1min (symbol, open_time),
    INDEX idx_open_time_1min (open_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建30分钟K线数据表（历史数据）
CREATE TABLE IF NOT EXISTS kline_data_30min (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    symbol VARCHAR(50) NOT NULL,
    open_time BIGINT NOT NULL,
    close_time BIGINT NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    volume DECIMAL(20,8) NOT NULL,
    quote_volume DECIMAL(20,8) NOT NULL,
    trades_count INT NOT NULL,
    taker_buy_base_volume DECIMAL(20,8) NOT NULL,
    taker_buy_quote_volume DECIMAL(20,8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_kline_30min (symbol, open_time),
    INDEX idx_open_time_30min (open_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建系统状态表（记录跨天处理状态）
CREATE TABLE IF NOT EXISTS system_status (
    id INT PRIMARY KEY AUTO_INCREMENT,
    status_key VARCHAR(50) NOT NULL UNIQUE,
    status_value TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status_key (status_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 兼容性处理：检查并重命名旧的kline_data表
-- 如果存在旧的kline_data表，将其重命名为备份表
-- DROP TABLE IF EXISTS kline_data_backup;
-- RENAME TABLE kline_data TO kline_data_backup;

-- 显示创建的表
SHOW TABLES;

-- 显示表结构
DESCRIBE trading_records;
DESCRIBE kline_data_1min;
DESCRIBE kline_data_30min;
DESCRIBE system_status; 