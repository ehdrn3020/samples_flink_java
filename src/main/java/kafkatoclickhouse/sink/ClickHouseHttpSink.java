package kafkatoclickhouse.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import kafkatoclickhouse.model.UserEvent;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.util.ArrayList;
import java.util.List;

public class ClickHouseHttpSink extends RichSinkFunction<UserEvent> {

    private static final int BATCH_SIZE = 500;
    private static final String CLICKHOUSE_URL =
            "http://localhost:8123/?query=" +
                    "INSERT INTO vod_stat (event_date, content_id, view_cnt, gift_cnt) " +
                    "FORMAT JSONEachRow";

    private List<UserEvent> buffer;
    private transient CloseableHttpClient httpClient; // httpClient가 실제로 네트워크 전송 담당

    @Override
    public void open(Configuration parameters) {
        buffer = new ArrayList<>();
        httpClient = HttpClients.createDefault(); // open()에서 HTTP 클라이언트 1번 생성
    }

    @Override
    public void invoke(UserEvent value, Context ctx) throws Exception {
        buffer.add(value);

        if (buffer.size() >= 5000) {
            flush();
        }
    }
    // 모아둔 이벤트들을 한 번에 ClickHouse로 전송
    private void flush() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }

        StringBuilder body = new StringBuilder();
        for (UserEvent e : buffer) {
            body.append(String.format(
                    "{\"event_date\":\"%s\",\"content_id\":%d,\"view_cnt\":%d,\"gift_cnt\":%d}\n",
                    e.getEventDate(),
                    e.getContentId(),
                    e.getViewCnt(),
                    e.getGiftCnt()
            ));
        }

        HttpPost post = new HttpPost(CLICKHOUSE_URL);
        post.setEntity(new StringEntity(body.toString(), ContentType.APPLICATION_JSON));

        try (CloseableHttpResponse response = httpClient.execute(post)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                throw new RuntimeException("Failed to insert data into ClickHouse, status code: " + statusCode);
            }
        }
    }

    @Override
    public void close() throws Exception {
        flush();
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
