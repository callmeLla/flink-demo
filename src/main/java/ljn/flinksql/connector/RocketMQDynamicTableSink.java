package ljn.flinksql.connector;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.rocketmq.client.AccessChannel;

import java.util.Properties;

public class RocketMQDynamicTableSink implements DynamicTableSink {

    private EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private DataType producedDataType;
    private String host;

    private Integer port;

    private String topic;

    private String group;
    private String tag;

    public RocketMQDynamicTableSink(EncodingFormat<SerializationSchema<RowData>> encodingFormat
            , DataType producedDataType, String host, Integer port, String topic, String group, String tag) {
        this.encodingFormat = encodingFormat;
        this.producedDataType = producedDataType;
        this.host = host;
        this.port = port;
        this.topic = topic;
        this.group = group;
        this.tag = tag;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT).build();
//                .addContainedKind(RowKind.UPDATE_BEFORE)
//                .addContainedKind(RowKind.UPDATE_AFTER)
//                .addContainedKind(RowKind.DELETE).build();//支持的sink类型，append、upsert、retract
//        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> serializer = encodingFormat.createRuntimeEncoder(
                context,
                producedDataType);

        Properties producerProps = getProducerProps();
        SinkFunction sinkFunction = new RocketMQSink(producerProps, serializer, topic, tag);
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new RocketMQDynamicTableSink(encodingFormat, producedDataType, host, port, topic, group, tag);
    }

    @Override
    public String asSummaryString() {
        return "rocketmq table sink";
    }

    private Properties getProducerProps() {
        Properties producerProps = new Properties();
//        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR,
//                "http://${instanceId}.${region}.mq-internal.aliyuncs.com:8080");
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR,
                host+":"+port);

        producerProps.setProperty(RocketMQConfig.PRODUCER_GROUP, group);


//        producerProps.setProperty(RocketMQConfig.ACCESS_KEY, "${AccessKey}");
//        producerProps.setProperty(RocketMQConfig.SECRET_KEY, "${SecretKey}");
//        producerProps.setProperty(RocketMQConfig.ACCESS_CHANNEL, AccessChannel.CLOUD.name());
        return producerProps;
    }
}
