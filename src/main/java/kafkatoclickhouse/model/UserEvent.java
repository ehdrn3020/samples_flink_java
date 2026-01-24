package kafkatoclickhouse.model;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UserEvent implements Serializable {

    @JsonProperty("event_date")
    private String eventDate;

    @JsonProperty("content_id")
    private long contentId;

    @JsonProperty("view_cnt")
    private long viewCnt;

    @JsonProperty("gift_cnt")
    private long giftCnt;

    public UserEvent() {}

    // getters
    public String getEventDate() { return eventDate; }
    public long getContentId() { return contentId; }
    public long getViewCnt() { return viewCnt; }
    public long getGiftCnt() { return giftCnt; }

    @Override
    public String toString() {
        return "Insert ROw:" + eventDate + "," + contentId + "," + viewCnt + "," + giftCnt;
    }
    
}
