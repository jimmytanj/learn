import json

from pyflink.common.serialization import SimpleStringSchema, SimpleStringEncoder, JsonRowDeserializationSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, StreamingFileSink
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction





s_env = StreamExecutionEnvironment.get_execution_environment()
s_env.set_parallelism(1)
ti = Types.ROW_NAMED(["app", 'busi', 'date', 'ip'], [Types.STRING(), Types.STRING(),
                                                     Types.BIG_INT(), Types.STRING()])
builder = JsonRowDeserializationSchema.builder()
builder.type_info(ti)
jrds = builder.ignore_parse_errors().build()
fkc = FlinkKafkaConsumer(topics="ULS-BUSI-LOG-dev", deserialization_schema=jrds,
                         properties={"bootstrap.servers": "10.100.1.16:9192", "group.id": "123",
                                     "auto.offset.reset": "earliest"})
fkc.set_start_from_earliest()
src = s_env.add_source(fkc).map(lambda x:x.get("values"))
src.add_sink(StreamingFileSink
             .for_row_format('C:\\tmp\\pyoutput', SimpleStringEncoder())
             .build())
##
s_env.execute("123")
