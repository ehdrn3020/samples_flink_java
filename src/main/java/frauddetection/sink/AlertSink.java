package frauddetection.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import frauddetection.model.Alert;

public class AlertSink implements SinkFunction<Alert> {

    @Override
    public void invoke(Alert alert, Context context) {
        System.out.println(alert);
    }
}
