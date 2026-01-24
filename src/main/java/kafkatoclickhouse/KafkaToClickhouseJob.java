package kafkatoclickhouse;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.datastream.DataStream;

import kafkatoclickhouse.model.UserEvent;
import kafkatoclickhouse.source.KafkaUserEventSource;
import kafkatoclickhouse.sink.ClickHouseHttpSink ;

public class KafkaToClickhouseJob {

    public static void main(String[] args) throws Exception {

//        final StreamExecutionEnvironment env = createOperateEnvironment(); // Operate 환경
         final StreamExecutionEnvironment env = createLocalEnvironment(); // Local 환경

        DataStream<UserEvent> source_kafka_user_event = env.fromSource(
            KafkaUserEventSource.get(),
            WatermarkStrategy.noWatermarks(),
            "KafkaUserEventSource"
        );

        DataStream<UserEvent> userStream = source_kafka_user_event
            // contentId = 1234 제외
            .filter(e -> e.getContentId() != 1234)
            .name("filter-contentId-1234")
            .keyBy(UserEvent::getContentId);

        userStream
            .addSink(new ClickHouseHttpSink())
            .name("ClickHouseHttpSink");

        env.execute("Kafka → ClickHouse Example");
    }

    // 운영 환경
    public static StreamExecutionEnvironment createOperateEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5 * 600_00); // 5분
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2 * 60_000L);   // 2분 - 최소 간격
        env.getCheckpointConfig().setCheckpointTimeout(15 * 60_000L);     // 15분 타임아웃
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);       // 최대 동시 체크포인트 수 제한
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }

    // Local 환경
    public static StreamExecutionEnvironment createLocalEnvironment(){
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8081);     // Web UI 포트
        configuration.setString(RestOptions.BIND_ADDRESS, "0.0.0.0"); // 외부 접근 허용
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING); //스트리밍 모드 (Kafka 필수)
        env.enableCheckpointing(10_000); // 1분
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1); // retry 1 번
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }
}
