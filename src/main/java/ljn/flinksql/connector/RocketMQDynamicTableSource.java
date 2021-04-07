package ljn.flinksql.connector;

import ljn.flinksql.connector.common.serialization.KeyValueDeserializationSchema;
import ljn.flinksql.connector.common.serialization.SimpleKeyValueDeserializationSchema;
import ljn.flinksql.connector.common.serialization.SimpleTupleDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.rocketmq.client.AccessChannel;

import java.util.Properties;

import static ljn.flinksql.connector.RocketMQConfig.CONSUMER_OFFSET_LATEST;
import static ljn.flinksql.connector.RocketMQConfig.DEFAULT_CONSUMER_TAG;

public class RocketMQDynamicTableSource implements ScanTableSource {
    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private DataType producedDataType;

    private String host;
    private Integer port;
    private String topic;
    private String group;
    private String tag;


    public RocketMQDynamicTableSource(DecodingFormat<DeserializationSchema<RowData>> decodingFormat
            , DataType producedDataType, String host, Integer port, String topic, String group, String tag) {
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
        this.host = host;
        this.port = port;
        this.topic = topic;
        this.group = group;
        this.tag = tag;
    }


    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {

        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                scanContext,
                producedDataType);
        Properties consumerProps = getConsumerProps();
        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(true);
        SourceFunction sourceFunction = new RocketMQSource<>(schema, consumerProps, deserializer);
        return SourceFunctionProvider.of(sourceFunction,false);
    }


    @Override
    public DynamicTableSource copy() {
        return new RocketMQDynamicTableSource(decodingFormat, producedDataType, host, port, topic, group, tag);
    }

    @Override
    public String asSummaryString() {
        return "RocketMQ Table Source";
    }

    private Properties getConsumerProps() {
        Properties consumerProps = new Properties();
//        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR,
//                "http://${instanceId}.${region}.mq-internal.aliyuncs.com:8080");
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR,
                host+":"+port);

        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, group);

        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, topic);
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TAG, tag == null?DEFAULT_CONSUMER_TAG:tag);
        consumerProps.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);
//        consumerProps.setProperty(RocketMQConfig.ACCESS_KEY, "${AccessKey}");
//        consumerProps.setProperty(RocketMQConfig.SECRET_KEY, "${SecretKey}");
//        consumerProps.setProperty(RocketMQConfig.ACCESS_CHANNEL, AccessChannel.CLOUD.name());
        return consumerProps;
    }

}
