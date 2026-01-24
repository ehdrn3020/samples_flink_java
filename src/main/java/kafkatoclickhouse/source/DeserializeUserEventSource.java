package kafkatoclickhouse.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import kafkatoclickhouse.model.UserEvent;

import java.io.IOException;

public class DeserializeUserEventSource implements DeserializationSchema<UserEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public UserEvent deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, UserEvent.class);
    }

    @Override
    public boolean isEndOfStream(UserEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserEvent> getProducedType() {
        return TypeInformation.of(UserEvent.class);
    }
}
