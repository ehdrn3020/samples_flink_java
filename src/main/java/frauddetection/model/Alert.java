package frauddetection.model;

import java.io.Serializable;

public class Alert implements Serializable {

    private static final long serialVersionUID = 1L;

    private long id;

    public Alert() {}

    public Alert(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "⚠️ FRAUD ALERT for accountId=" + id;
    }
}
