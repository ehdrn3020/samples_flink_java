package kafkatoclickhouse.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import kafkatoclickhouse.model.UserEvent;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseHttpSink extends RichSinkFunction<UserEvent> {

    private static final int BATCH_SIZE = 1;
    private static final String CLICKHOUSE_URL = "http://localhost:8123/";

    private List<UserEvent> buffer;
    private transient CloseableHttpClient httpClient; // httpClient가 실제로 네트워크 전송 담당

    @Override
    public void open(Configuration parameters) {
        // clickhouse 인증 설정
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
            AuthScope.ANY,
            new UsernamePasswordCredentials("myuser", "mypass")
        );

        httpClient = HttpClients.custom()
            .setDefaultCredentialsProvider(credsProvider)
            .build(); // open()에서 HTTP 클라이언트 1번 생성

        buffer = new ArrayList<>();
    }

    @Override
    public void invoke(UserEvent value, Context ctx) throws Exception {
        System.out.println("[Sink] received event: " + value);

        buffer.add(value);
        System.out.println("[Sink] buffer size = " + buffer.size());

        if (buffer.size() >= BATCH_SIZE) {
            System.out.println("[Sink] flushing");
            flush();
        }
    }
    // 모아둔 이벤트들을 한 번에 ClickHouse로 전송
    private void flush() throws Exception {
        if (buffer.isEmpty()) return;

        StringBuilder body = new StringBuilder(buffer.size() * 64);

        for (UserEvent e : buffer) { // tsv
            body.append(e.getEventDate()).append('\t')
                    .append(e.getContentId()).append('\t')
                    .append(e.getViewCnt()).append('\t')
                    .append(e.getGiftCnt()).append('\n');
        }

        String query = "INSERT INTO test_db.user_tbl FORMAT TSV";
        String url = CLICKHOUSE_URL + "?query=" +
                URLEncoder.encode(query, StandardCharsets.UTF_8);

        HttpPost post = new HttpPost(url);
        StringEntity entity = new StringEntity(
                body.toString(),
                ContentType.TEXT_PLAIN.withCharset(StandardCharsets.UTF_8)
        );
        entity.setChunked(false);

        post.setHeader("Expect", "");
        post.setEntity(entity);   // 전송

        try (CloseableHttpResponse response = httpClient.execute(post)) {
            int status = response.getStatusLine().getStatusCode();
            if (status != 200) {
                throw new RuntimeException("ClickHouse insert failed: " + status);
            }
        }

        buffer.clear();
    }

    @Override
    public void close() throws Exception {
        flush();
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
