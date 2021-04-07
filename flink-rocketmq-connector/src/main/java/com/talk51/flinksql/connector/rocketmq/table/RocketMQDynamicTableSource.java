package com.talk51.flinksql.connector.rocketmq.table;


import com.talk51.flinksql.connector.rocketmq.config.RocketMQConfig;
import com.talk51.flinksql.connector.rocketmq.RocketMQSource;
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


import java.util.Properties;

/**
 * @ClassName: RocketMQDynamicTableSource
 * @Description: 计划期间使用RocketMQDynamicTableSource
 * @Author: liujianing
 * @Date: 2021/4/7 15:18
 * @Version: v1.0 文件初始创建
 */
public class RocketMQDynamicTableSource implements ScanTableSource {
    /** 解码格式 */
    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    /** 数据类型 */
    private DataType producedDataType;
    /** rocketmq地址 */
    private String nameserver;
    /** 主题 */
    private String topic;
    /** 分组 */
    private String group;
    /** tag */
    private String tag;


    public RocketMQDynamicTableSource(DecodingFormat<DeserializationSchema<RowData>> decodingFormat
            , DataType producedDataType, String nameserver, String topic, String group, String tag) {
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
        this.nameserver = nameserver;
        this.topic = topic;
        this.group = group;
        this.tag = tag;
    }


    /**
     * @Description: 支持的ChangelogMode类型
     * @Date: 2021/4/7 15:22
     * @Author: liujianing
     * @Return
     * @Throws
     */
    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    /**
     * @Description: 运行时主要逻辑
     * @Date: 2021/4/7 15:23
     * @Author: liujianing
     * @Return
     * @Throws
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {

        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                scanContext,
                producedDataType);
        /** 获取客户端参数 */
        Properties consumerProps = getConsumerProps();
        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(true);
        SourceFunction sourceFunction = new RocketMQSource<>(schema, consumerProps, deserializer);
        return SourceFunctionProvider.of(sourceFunction,false);
    }


    @Override
    public DynamicTableSource copy() {
        return new RocketMQDynamicTableSource(decodingFormat, producedDataType, nameserver, topic, group, tag);
    }

    @Override
    public String asSummaryString() {
        return "RocketMQ Table Source";
    }


    /**
     * @Description: 获取rocketmq客户端参数
     * @Date: 2021/4/7 15:25
     * @Author: liujianing
     * @Return
     * @Throws
     */
    private Properties getConsumerProps() {
        Properties consumerProps = new Properties();
//        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR,
//                "http://${instanceId}.${region}.mq-internal.aliyuncs.com:8080");
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR,
                nameserver);

        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, group);

        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, topic);
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TAG, tag == null? RocketMQConfig.DEFAULT_CONSUMER_TAG:tag);
        consumerProps.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, RocketMQConfig.CONSUMER_OFFSET_LATEST);
//        consumerProps.setProperty(RocketMQConfig.ACCESS_KEY, "${AccessKey}");
//        consumerProps.setProperty(RocketMQConfig.SECRET_KEY, "${SecretKey}");
//        consumerProps.setProperty(RocketMQConfig.ACCESS_CHANNEL, AccessChannel.CLOUD.name());
        return consumerProps;
    }

}
