from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import JsonRowDeserializationSchema,SimpleStringEncoder
from pyflink.datastream.connectors import FlinkKafkaConsumer,StreamingFileSink

s_env = StreamExecutionEnvironment.get_execution_environment()
s_env.set_parallelism(1)
src=s_env.add_source(FlinkKafkaConsumer(topics="ULS-BUSI-LOG-dev",deserialization_schema=JsonRowDeserializationSchema(),properties={"bootstrap.servers":"10.100.1.16:9192","group.id":"cqph",}))
src.add_sink(StreamingFileSink
    .for_row_format('/tmp/output', SimpleStringEncoder())
    .build())

s_env.execute("test")