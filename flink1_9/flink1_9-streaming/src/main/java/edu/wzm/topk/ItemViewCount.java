package edu.wzm.topk;

import lombok.Data;

@Data
public class ItemViewCount {
    private long itemId;
    private long windowEndTimeStamp;
    private long clickCount;

    public static ItemViewCount of(long itemId, long windowEndTimeStamp, long count){
        ItemViewCount viewCount = new ItemViewCount();
        viewCount.itemId = itemId;
        viewCount.windowEndTimeStamp = windowEndTimeStamp;
        viewCount.clickCount = count;

        return viewCount;
    }
}
