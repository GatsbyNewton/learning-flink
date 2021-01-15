package edu.wzm.topk;

import lombok.Data;

@Data
public class UserBehavior {
    private long userId;
    private long itemId;
    private long categoryId;
    private String behavior;    // 用户行为，如 pv、buy、cart、fav
    private long timestamp;
}
