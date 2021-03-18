from pyflink.table import StreamTableEnvironment,EnvironmentSettings
from pyflink.datastream import StreamExecutionEnvironment
env=StreamExecutionEnvironment.get_execution_environment()
env_setting=EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
st_env=StreamTableEnvironment.create(env,env_setting)

'''
# 依赖关系
kafka程序要依赖的包：
flink-connector-kafka
kafka-clients
flink-connector-kafka-base

# 主键设置
数据要写入到mysql，需要给flink的table和mysql的table指定主键，便于更新。
flink table的主键要设置但是不能启用，状态要设置为 NOT ENFORCED
'''
source_sql="""
CREATE TEMPORARY TABLE kafka_source (
  id String,
  create_time TIMESTAMP(3),
  proc_time as PROCTIME()
) WITH (
 'connector' = 'kafka',
 'topic' = 'example',
 'properties.bootstrap.servers' = '192.168.251.98:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'canal-json'  
)
"""


target_sql="""
CREATE  TEMPORARY TABLE result_count(
cnt BIGINT,
ind STRING,
PRIMARY KEY(ind) NOT ENFORCED
)
WITH
(
'connector'='jdbc',
'url'='jdbc:mysql://192.168.251.94:3306/hand_in',
'table-name' = 'result_count',
'username'='root',
'password'='hl.Data2018'
)
"""

st_env.execute_sql(source_sql)
st_env.execute_sql(target_sql)
st_env.execute_sql("INSERT INTO result_count SELECT COUNT(1) as cnt,'1' as ind FROM kafka_source")