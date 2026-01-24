package kafkatoclickhouse.sink;

import kafkatoclickhouse.model.UserEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Date;
import java.sql.PreparedStatement;

import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

// lifecycle RichSinkFunction에 위임
public class ClickHouseJdbcSink extends RichSinkFunction<UserEvent> {
    // transient : 직렬화하면 안되는 필요로 표시
    private transient SinkFunction<UserEvent> delegate;

    @Override
    public void open(Configuration parameters) throws Exception {
        /*
        DataSource 생성
        Connection Pool 준비
        PreparedStatement 템플릿 준비
        Batch buffer 초기화
         */
        delegate = JdbcSink.sink(
                "INSERT INTO vod_stat (event_date, content_id, view_cnt, gift_cnt) VALUES (?, ?, ?, ?)",
                (PreparedStatement ps, UserEvent user) -> {
                    ps.setDate(1, Date.valueOf(user.getEventDate()));
                    ps.setLong(2, user.getContentId());
                    ps.setLong(3, user.getViewCnt());
                    ps.setLong(4, user.getGiftCnt());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(5000)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://ck01:9000/default")
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .build()
        );

        if (delegate instanceof RichSinkFunction) {
            ((RichSinkFunction<?>) delegate).open(parameters);
        }
    }

    @Override
    // 레코드 1건을 Sink로 보낼 때마다 호출하는 메서드, 테이블이 insert
    public void invoke(UserEvent value, Context context) throws Exception {
        delegate.invoke(value, context);
        /* 아래 코드가 실행
        PreparedStatement.setDate(1, Date.valueOf(user.getEventDate()));
        PreparedStatement.setLong(2, user.getContentId());
        PreparedStatement.setLong(3, user.getViewCnt());
        PreparedStatement.setLong(4, user.getGiftCnt());
        */
    }

    @Override
    public void close() throws Exception {
        if (delegate instanceof RichSinkFunction) {
            ((RichSinkFunction<?>) delegate).close();
        }
    }

}
