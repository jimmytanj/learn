from pyflink.table import  StreamTableEnvironment,EnvironmentSettings
from pyflink.datastream import StreamExecutionEnvironment


env=StreamExecutionEnvironment.get_execution_environment()
env_setting=EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
st_env=StreamTableEnvironment.create(env,env_setting)

source_sql="""
CREATE TEMPORARY TABLE kafka_source (
  app String,
  busi String,
  `date` BIGINT,
  ip String
) WITH (
 'connector' = 'kafka',
 'topic' = 'ULS-BUSI-LOG-dev',
 'properties.bootstrap.servers' = '10.100.1.16:9192',
 'properties.group.id' = 'testGroup',
 'scan.startup.mode' = 'earliest-offset',
 'format' = 'json'  
)
"""
target_sql="""
CREATE  TEMPORARY TABLE result_count(
cnt BIGINT,
url STRING,
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

query_sql="""
SELECT ip,COUNT(1) AS CNT
FROM kafka_source
GROUP BY ip
"""

st_env.execute_sql(source_sql)
st_env.execute_sql(query_sql)