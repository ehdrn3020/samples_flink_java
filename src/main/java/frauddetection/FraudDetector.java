package frauddetection;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import frauddetection.model.Alert;
import frauddetection.model.Transaction;

/*
1분 이내에
소액 거래(SMALL_AMOUNT 이하) 발생 후
고액 거래(LARGE_AMOUNT 이상) 발생
→ Fraud Alert
*/
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {
    private static final long serialVersionUID = 1L;
    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void processElement(
        Transaction transaction,
        Context context,
        Collector<Alert> collector
    ) throws Exception {

        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());

        collector.collect(alert);
    }
}
