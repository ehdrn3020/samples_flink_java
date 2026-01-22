package kafkatoclickhouse.model;

import java.io.Serializable;

public class Alert implements Serializable {

    private long accountId;

    public Alert() {}

    public Alert(long accountId) {
        this.accountId = accountId;
    }

    public long getAccountId() {
        return accountId;
    }

    @Override
    public String toString() {
        return "⚠️ FRAUD ALERT accountId=" + accountId;
    }
}
