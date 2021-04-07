package ljn.flinksql.connector;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

public class RocketMQDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    /** 连接ip */
    public static final ConfigOption<String> host = ConfigOptions.key("host").stringType().noDefaultValue();
    /** 端口号 */
    public static final ConfigOption<Integer> port = ConfigOptions.key("port").intType().noDefaultValue();
    /** 主题 */
    public static final ConfigOption<String> topic = ConfigOptions.key("topic").stringType().noDefaultValue();
    /** 分组 */
    public static final ConfigOption<String> group = ConfigOptions.key("group").stringType().noDefaultValue();
    /** Tag */
    public static final ConfigOption<String> tag = ConfigOptions.key("tag").stringType().noDefaultValue();


    public static final String IDENTIFIER = "rocketmq";

    /**
     * @Description: 唯一标识符，用来让框架通过SPI来找到该connector
     * @Date: 2021/4/6 13:42
     * @Author: liujianing
     * @Return
     * @Throws
     */
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }


    /**
     * @Description: 设置必须项
     * @Date: 2021/4/6 13:41
     * @Author: liujianing
     * @Return
     * @Throws
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(host);
        options.add(port);
        options.add(topic);
        options.add(group);
        options.add(tag);
        return options;
    }

    /**
     * @Description: 设置可选项
     * @Date: 2021/4/6 13:41
     * @Author: liujianing
     * @Return
     * @Throws
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet();

        return options;
    }

    /**
     * @Description: 创建上下文sink
     * @param context 上下文对象
     * @Date: 2021/4/7 10:56
     * @Author: liujianing
     * @Return
     * @Throws
     */
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final EncodingFormat<SerializationSchema<RowData>> decodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);

        /** 获取生效的选项 */
        final ReadableConfig options = helper.getOptions();
        final String hostvalue = options.get(host);
        final String topicvalue = options.get(topic);
        final int portvalue = options.get(port);
        final String groupvalue = options.get(group);
        final String tagvalue = options.get(tag);

        // discover a suitable decoding format
//        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
//                DeserializationFormatFactory.class,
//                FactoryUtil.FORMAT);

        // validate all options
//        helper.validate();
        final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        return new RocketMQDynamicTableSink(decodingFormat, producedDataType, hostvalue, portvalue, topicvalue, groupvalue, tagvalue);
    }

    /**
     * @Description: 创建动态表Source
     * @param context 上下文对象
     * @Date: 2021/4/7 10:55
     * @Author: liujianing
     * @Return
     * @Throws
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);
        /** 获取生效的选项 */
        final ReadableConfig options = helper.getOptions();
        final String hostvalue = options.get(host);
        final String topicvalue = options.get(topic);
        final int portvalue = options.get(port);
        final String groupvalue = options.get(group);
        final String tagvalue = options.get(tag);
        // validate all options
//        helper.validate();
        final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        return new RocketMQDynamicTableSource(decodingFormat, producedDataType, hostvalue, portvalue, topicvalue, groupvalue, tagvalue);
    }


}
