package frauddetection.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import frauddetection.model.Transaction;

public class TransactionSource implements SourceFunction<Transaction> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {

        long accountId = 1L;

        // 1️⃣ 소액 → 2️⃣ 고액 패턴 한 번만 발생
        ctx.collect(new Transaction(accountId, 0.5, System.currentTimeMillis()));
        Thread.sleep(500);

        ctx.collect(new Transaction(accountId, 600.0, System.currentTimeMillis()));
        Thread.sleep(500);

        // 스트리밍처럼 보이게 idle 상태 유지
        while (running) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
