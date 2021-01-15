package edu.wzm.external;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: Jimmy Wong
 * @Date: 2020/1/22
 * @version:
 * @Description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private String orderId;
    private String userId;
    private Long timestamp;

    public static Order of(String str) {
        String[] msg = str.trim().split("\\s+");
        return new Order(msg[0], msg[1], Long.valueOf(msg[2]));
    }
}
