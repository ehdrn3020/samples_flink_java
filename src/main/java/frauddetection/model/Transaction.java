package frauddetection.model;

import java.io.Serializable;

public class Transaction implements Serializable {

    private static final long serialVersionUID = 1L;

    private long accountId;
    private double amount;
    private long timestamp;

    // Flink POJO 요구사항: 기본 생성자
    public Transaction() {}

    public Transaction(long accountId, double amount, long timestamp) {
        this.accountId = accountId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    public long getAccountId() {
        return accountId;
    }

    public double getAmount() {
        return amount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "accountId=" + accountId +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
