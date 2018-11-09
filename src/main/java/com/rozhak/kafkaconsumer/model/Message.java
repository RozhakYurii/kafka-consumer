package com.rozhak.kafkaconsumer.model;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Message {

    private String text;
    private String tag;


}
