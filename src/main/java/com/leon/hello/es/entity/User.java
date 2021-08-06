package com.leon.hello.es.entity;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @PROJECT_NAME: hello-es
 * @CLASS_NAME: User
 * @AUTHOR: OceanLeonAI
 * @CREATED_DATE: 2021/7/22 15:59
 * @Version 1.0
 * @DESCRIPTION:
 **/
@Data
@Component
@ConfigurationProperties(prefix = "user")
public class User {
    private String name;

    private int age;

    private boolean ok; // don't user is for prefix
}
