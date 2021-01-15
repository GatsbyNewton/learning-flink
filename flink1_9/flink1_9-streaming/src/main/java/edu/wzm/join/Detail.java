package edu.wzm.join;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Detail {
    private String username;
    private String orderId;
    private Long timestamp;
}