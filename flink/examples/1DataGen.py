
from pyflink.table import StreamTableEnvironment,EnvironmentSettings
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()

st_env = StreamTableEnvironment.create(environment_settings=env_settings)
# 源表
st_env.execute_sql('''
CREATE TEMPORARY TABLE wcsource (
  `word` VARCHAR(2),
  ts AS localtimestamp,
  WATERMARK FOR ts AS ts
) WITH (
  'connector' = 'datagen',
  'rows-per-second'='1'
)
''')

# 目标表
st_env.execute_sql("""
CREATE TEMPORARY TABLE wcsink(
word STRING,
cnt BIGINT
)
with
(
'connector'='print'
)
""")
# 在1.12版本中可以直接在execute_sql后面调print，且execute_sql会直接提交到flink执行，不需要再execute。
# 在1.11版本中还需要有ExecutionEnviroment.execute()
st_env.execute_sql("insert into wcsink select word,count(1) as cnt from wcsource group by word").print()
