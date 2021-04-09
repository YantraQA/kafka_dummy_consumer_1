package com.yantraQA.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Notification {
   private Long id;
   private String topicName;
   private String content;
   private NotificationType type;
}
