from pyflink.table import EnvironmentSettings, BatchTableEnvironment
'''
批处理模式，
从mysql读，输出到mysql
需要flink-jdbc-connector
如果要连接mysql，还需要mysql-connector-java
拷贝到flink/lib
是否可以通过add_jar的方式还未确认
'''


env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = BatchTableEnvironment.create(environment_settings=env_settings)

source_sql="""
CREATE TABLE CLC
(
clc_id BIGINT,
clc_code VARCHAR(100),
clc_name VARCHAR(256),
parent_code VARCHAR(100),
clc_level INT,
clc_path VARCHAR(1024),
create_time TIMESTAMP(3)
)
with 
(
'connector'='jdbc',
'url'='jdbc:mysql://192.168.251.94:3306/hand_in',
'table-name' = 'clc_new',
'username'='root',
'password'='hl.Data2018'
)
"""

sink_sql="""
CREATE TABLE CLC_RESULT(
clc_code VARCHAR(100),
cnt BIGINT
)
with 
(
'connector'='jdbc',
'url'='jdbc:mysql://192.168.251.94:3306/hand_in',
'table-name' = 'clc_result',
'username'='root',
'password'='hl.Data2018'
)
"""
table_env.execute_sql(source_sql)
table_env.execute_sql(sink_sql)
query_sql="insert into CLC_RESULT select substring(clc_code,1,1),count(1) from CLC group by substring(clc_code,1,1)"
table_env.execute_sql(query_sql)