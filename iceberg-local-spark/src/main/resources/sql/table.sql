-- 1. 先按 dt（日期）分区，再在每个分区内按 user_id 分 5 桶
CREATE TABLE user_behavior_part_bucket
(
    user_id   INT,
    item_id   BIGINT,
    behavior  STRING,
    timestamp BIGINT
)
-- 一级分区：dt（日期）
    PARTITIONED BY (dt STRING)
-- 分区内分桶：按 user_id 分 32 桶，桶内按 timestamp 排序
CLUSTERED BY (user_id)
SORTED BY (user_id, timestamp)
INTO 5 BUCKETS
STORED AS parquet;
