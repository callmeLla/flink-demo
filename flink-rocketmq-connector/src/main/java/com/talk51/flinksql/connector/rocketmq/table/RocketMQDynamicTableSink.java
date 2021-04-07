package com.talk51.flinksql.connector.rocketmq.table;

import com.talk51.flinksql.connector.rocketmq.config.RocketMQConfig;
import com.talk51.flinksql.connector.rocketmq.RocketMQSink;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Properties;

/**
 * @ClassName: RocketMQDynamicTableSink
 * @Description: 计划期间使用RocketMQDynamicTableSink
 * @Author: liujianing
 * @Date: 2021/4/7 15:25
 * @Version: v1.0 文件初始创建
 */
public class RocketMQDynamicTableSink implements DynamicTableSink {

    /** 编码格式 */
    private EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    /** 数据类型 */
    private DataType producedDataType;
    /** 连接地址 */
    private String nameserver;
    /** 主题 */
    private String topic;
    /** 分组 */
    private String group;
    /** tag */
    private String tag;

    public RocketMQDynamicTableSink(EncodingFormat<SerializationSchema<RowData>> encodingFormat
            , DataType producedDataType, String nameserver, String topic, String group, String tag) {
        this.encodingFormat = encodingFormat;
        this.producedDataType = producedDataType;
        this.nameserver = nameserver;
        this.topic = topic;
        this.group = group;
        this.tag = tag;
    }

    /**
     * @Description: 支持的sink类型
     * @Date: 2021/4/7 15:27
     * @Author: liujianing
     * @Return
     * @Throws
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT).build();
//                .addContainedKind(RowKind.UPDATE_BEFORE)
//                .addContainedKind(RowKind.UPDATE_AFTER)
//                .addContainedKind(RowKind.DELETE).build();//支持的sink类型，append、upsert、retract
//        return changelogMode;
    }

    /**
     * @Description: 运行时主要逻辑
     * @Date: 2021/4/7 15:27
     * @Author: liujianing
     * @Return
     * @Throws
     */
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> serializer = encodingFormat.createRuntimeEncoder(
                context,
                producedDataType);

        /** 获取rocketmq参数 */
        Properties producerProps = getProducerProps();
        SinkFunction sinkFunction = new RocketMQSink(producerProps, serializer, topic, tag);
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new RocketMQDynamicTableSink(encodingFormat, producedDataType, nameserver, topic, group, tag);
    }

    @Override
    public String asSummaryString() {
        return "rocketmq table sink";
    }


    /**
     * @Description: 获取rocketmq参数
     * @Date: 2021/4/7 15:28
     * @Author: liujianing
     * @Return
     * @Throws
     */
    private Properties getProducerProps() {
        Properties producerProps = new Properties();
//        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR,
//                "http://${instanceId}.${region}.mq-internal.aliyuncs.com:8080");
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR,
                nameserver);

        producerProps.setProperty(RocketMQConfig.PRODUCER_GROUP, group);


//        producerProps.setProperty(RocketMQConfig.ACCESS_KEY, "${AccessKey}");
//        producerProps.setProperty(RocketMQConfig.SECRET_KEY, "${SecretKey}");
//        producerProps.setProperty(RocketMQConfig.ACCESS_CHANNEL, AccessChannel.CLOUD.name());
        return producerProps;
    }
}
