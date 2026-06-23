package com.jiayixin_2311650203.flume.interceptor;

import com.alibaba.fastjson.JSONObject;

public class JsonTest {

    public static void main(String[] args) {

        String jsonStr = "{\"name\":\"zhangsan\"}";
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        System.out.println(jsonObject.get("name"));


    }
}
