
CREATE TABLE user_behavior (
    user_id VARCHAR COMMENT '用户ID',
    item_id VARCHAR COMMENT '商品ID',
    category_id VARCHAR COMMENT '类别ID',
    behavior VARCHAR COMMENT '操作行为',
    ts TIMESTAMP
)
WITH (
    'connector.type' = 'kafka', -- 使用 kafka connector
    'connector.version' = '0.10',  -- kafka 版本，universal 支持 0.11 以上的版本
    'connector.topic' = 'user_behavior',  -- kafka topic
    'connector.startup-mode' = 'earliest-offset', -- 从起始 offset 开始读取
    'connector.properties.0.key' = 'zookeeper.connect',  -- 连接信息
    'connector.properties.0.value' = 'localhost:2181',
    'connector.properties.1.key' = 'bootstrap.servers',
    'connector.properties.1.value' = 'localhost:9092',
    'update-mode' = 'append',
    'format.type' = 'json',  -- 数据源格式为 json
    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
)



CREATE TABLE pvuv (
    dt VARCHAR COMMENT '用户ID',
    pv BIGINT COMMENT 'pv',
    uv BIGINT COMMENT 'uv'
)
WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/flink',
    'connector.username' = 'root',
    'connector.password' = '123456',
    'connector.table' = 'pvuv',
    'connector.write.flush.max-rows' = '1' -- 默认5000条
)