package kafkatoclickhouse.source;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import kafkatoclickhouse.model.UserEvent;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class KafkaUserEventSource {

    public static KafkaSource<UserEvent> get() {

        return KafkaSource.<UserEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("kafka-test")
                .setGroupId("kafka-to-clickhouse-group")
                // Kafka offset 초기화 로직 - consumer group이 마지막으로 처리한 지점부터 이어서
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new DeserializeUserEventSource())
                .build();

        /*
        코드 흐름은
            Kafka ConsumerRecord를 읽고 →
            value(byte[])만 뽑아서 →
            UserEventSource가 JSON 파싱 →
            UserEvent 객체를 만들고 →
            DataStream<UserEvent>로 Return

        Kafka 메시지의 value(byte[])를 String으로 변환
            .setValueOnlyDeserializer(new SimpleStringSchema())
         */
    }
}