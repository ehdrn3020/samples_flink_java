package kafkatoclickhouse.model;

import java.io.Serializable;

public class UserEvent implements Serializable {

    private String eventDate;
    private long contentId;
    private long viewCnt;
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
