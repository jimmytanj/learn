import json

from pyflink.common.serialization import SimpleStringSchema, SimpleStringEncoder
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, StreamingFileSink

s_env = StreamExecutionEnvironment.get_execution_environment()
s_env.set_parallelism(1)
fkc = FlinkKafkaConsumer(topics="ULS-BUSI-LOG-dev", deserialization_schema=SimpleStringSchema(),
                         properties={"bootstrap.servers": "10.100.1.16:9192", "group.id": "cqph"})
fkc.set_start_from_earliest()
src = s_env.add_source(fkc)
app = src.map(lambda x: json.loads(x))
app.add_sink(StreamingFileSink
             .for_row_format('/tmp/output', SimpleStringEncoder())
             .build())

s_env.execute("test")
