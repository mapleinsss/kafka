package org.maple.c_customizeSerialize;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @auther Mapleins
 * @date 2019-06-25 16:46
 * @Description 需要被序列化的类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Company implements Serializable {
    private String name;
    private String address;
}
