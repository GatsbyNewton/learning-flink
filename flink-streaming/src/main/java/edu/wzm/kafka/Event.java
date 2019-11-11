package edu.wzm.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Event {
    private String word;
    private int frequency;
    private long timestamp;

    public Event(){}

    public static Event fromString(String str) {
        String[] msg = str.trim().split(",");
        return new Event(msg[0], Integer.parseInt(msg[1]), Long.parseLong(msg[2]));
    }
}
