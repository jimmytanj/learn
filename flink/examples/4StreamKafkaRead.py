from pyflink.table import StreamTableEnvironment,EnvironmentSettings
from pyflink.datastream import StreamExecutionEnvironment
env=StreamExecutionEnvironment.get_execution_environment()
env_setting=EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
st_env=StreamTableEnvironment.create(env,None,env_setting)

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
  app String,
  busi String,
  ip  String
) WITH (
 'connector' = 'kafka',
 'topic' = 'ULS-BUSI-LOG-dev',
 'properties.bootstrap.servers' = '10.100.1.16:9192',
 'properties.group.id' = 'testGroup',
 'scan.startup.mode' = 'earliest-offset',
 'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true',
 'format' = 'json'  
)
"""


target_sql="""
CREATE  TEMPORARY TABLE result_count(
cnt BIGINT,
ind STRING
)
WITH
(
'connector'='print'
)
"""

st_env.execute_sql(source_sql)
st_env.execute_sql(target_sql)
st_env.execute_sql("SELECT COUNT(1) as cnt,app as ind FROM kafka_source group by app").print()