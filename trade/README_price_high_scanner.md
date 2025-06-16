# Binance价格高点扫描器

## 功能说明

该脚本用于扫描Binance所有合约交易对，监控价格突破指定天数高点的情况，并通过企业微信群机器人发送通知。

## 主要特性

### 1. 数据监控
- 获取所有USDT永续合约交易对
- 可配置历史K线分析天数（默认30天）
- 获取指定天数的30分钟K线数据
- 实时检测价格是否突破历史高点

### 2. 信息收集
脚本会收集以下信息并在通知中展示：

**实时变化数据（每次获取）：**
- 当前价格
- 资金费率、资金费率年化收益率
- 资金费率结算周期

**静态数据（缓存机制，1天过期）：**
- 历史最高价、历史最低价
- 市值
- 市值排名
- 市场占用率
- Twitter账号
- Twitter最后更新时间
- Github地址
- 仓库最后更新时间
- 官网地址
- 发行日期
- 合约描述
- 合约标签

### 3. 智能缓存机制
为了提高效率和减少API调用，脚本实现了带过期时间的本地缓存：
- 代币详细信息（`token_info_cache.pkl`）
- 合约描述信息（`symbol_description_cache.pkl`）
- 产品标签信息（`products_cache.pkl`）

**缓存特性：**
- 缓存过期时间：24小时
- 自动检测过期并重新获取数据
- 兼容旧格式缓存并自动升级
- 缓存文件存储在 `trade/cache/` 目录下

### 4. 企业微信通知
当检测到价格突破历史高点时，会自动发送格式化的Markdown消息到指定的企业微信群，包含：
- 合约基本信息（当前价格、历史高低点）
- 资金费率详细信息
- 代币详细信息（市值、排名、社交媒体等）
- 合约描述和标签
- 时间戳

### 5. 命令行参数支持
所有脚本都支持 `--days` 参数来指定历史K线分析天数：
```bash
# 使用默认30天数据
python binance_price_high_scanner.py

# 使用7天数据进行分析
python binance_price_high_scanner.py --days 7

# 使用60天数据进行分析
python binance_price_high_scanner.py --days 60
```

## 文件结构

```
trade/
├── binance_price_high_scanner.py    # 主扫描器脚本
├── test_price_high_scanner.py       # 测试脚本
├── quick_test_scanner.py            # 快速测试脚本（前10个合约）
├── scheduler_price_high_scanner.py  # 定时调度器
├── run_price_scanner.sh             # Shell启动脚本
├── README_price_high_scanner.md     # 说明文档
└── cache/                           # 缓存目录
    ├── token_info_cache.pkl         # 代币信息缓存
    ├── symbol_description_cache.pkl # 合约描述缓存
    └── products_cache.pkl           # 产品标签缓存
```

## 使用方法

### 1. 单次运行
```bash
# 使用默认30天数据
python binance_price_high_scanner.py

# 使用指定天数
python binance_price_high_scanner.py --days 7
```

### 2. 快速测试（只扫描前10个合约）
```bash
# 使用默认7天数据
python quick_test_scanner.py

# 使用指定天数
python quick_test_scanner.py --days 5
```

### 3. 定时调度（每30分钟运行一次）
```bash
# 使用默认30天数据
python scheduler_price_high_scanner.py

# 使用指定天数
python scheduler_price_high_scanner.py --days 14
```

### 4. Shell脚本启动（带菜单）
```bash
chmod +x run_price_scanner.sh
./run_price_scanner.sh
```

### 5. 功能测试
```bash
python test_price_high_scanner.py
```

## 配置说明

### 企业微信机器人配置
在脚本中修改以下URL：
```python
self.webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
```

### 缓存配置
缓存相关配置在脚本初始化中：
```python
# 缓存过期时间（小时）
self.cache_expire_hours = 24

# 缓存目录
self.cache_dir = os.path.join(project_root, 'trade/cache')
```

## API数据源

1. **K线数据**: Binance Futures API
2. **资金费率**: Binance Futures API  
3. **代币信息**: `https://www.binance.com/bapi/apex/v1/friendly/apex/marketing/web/token-info`
4. **合约描述**: `https://bin.bnbstatic.com/api/i18n/-/web/cms/en/symbol-description`
5. **产品标签**: `https://www.binance.com/bapi/asset/v2/public/asset-service/product/get-products`

## 性能优化

1. **智能缓存**: 静态数据缓存24小时，减少API调用
2. **批量获取**: 一次性获取所有符号描述和产品标签
3. **请求限制**: 每次请求间隔0.1秒，避免API限制
4. **错误恢复**: 自动处理单个交易对的错误，不影响整体扫描

## 日志和监控

- 详细的日志记录每个步骤
- 实时显示扫描进度
- 缓存状态监控
- 通知发送状态跟踪

## 注意事项

1. 确保Binance API密钥配置正确
2. 企业微信机器人Key需要有效
3. 网络代理配置（如需要）
4. 缓存目录需要有写权限
5. 建议在服务器上长期运行时使用调度器模式

## 更新日志

**v2.0 (2024-12-30)**
- ✅ 添加命令行参数支持（--days）
- ✅ 实现智能缓存机制（24小时过期）
- ✅ 新增更多代币信息字段（市值排名、市场占用率、社交媒体更新时间等）
- ✅ 优化通知消息格式
- ✅ 改进错误处理和日志记录
- ✅ 所有脚本支持可配置历史天数

**v1.0 (2024-12-30)**
- ✅ 基础价格突破检测功能
- ✅ 企业微信群通知
- ✅ 基础缓存功能
- ✅ 定时调度支持 