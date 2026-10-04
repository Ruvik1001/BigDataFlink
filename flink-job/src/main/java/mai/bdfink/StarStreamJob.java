package mai.bdfink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

public class StarStreamJob {

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP");
        String topic = env("KAFKA_TOPIC");

        StreamExecutionEnvironment stream = StreamExecutionEnvironment.getExecutionEnvironment();
        stream.setParallelism(1);
        // offset коммитится вместе с checkpoint, повтор строки ловит unique в fact_sales
        stream.enableCheckpointing(10_000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("star-stream")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        stream.fromSource(source, WatermarkStrategy.noWatermarks(), "mock-sales")
                .addSink(new StarJdbcSink())
                .name("star-jdbc");

        stream.execute("csv-json-to-star");
    }

    private static String env(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("нет переменной " + key);
        }
        return value;
    }
}
