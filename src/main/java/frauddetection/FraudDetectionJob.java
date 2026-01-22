package frauddetection;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import frauddetection.model.Alert;
import frauddetection.model.Transaction;
import frauddetection.source.TransactionSource;
import frauddetection.sink.AlertSink;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");


        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts
                .addSink(new AlertSink())
                .name("alert-sink");

        env.execute("Fraud Detection Job");
    }
}
