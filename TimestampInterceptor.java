package com.jiayixin_2311650203.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TimestampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    // 处理单个Event的核心方法
    // TODO 核心处理思路：获取event中的header和body 将body中时间戳替换到header中的 timestamp对应值
    @Override
    public Event intercept(Event event) {
        // 获取Header 和 Body
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String data = new String(body, StandardCharsets.UTF_8);
        try {
            // 将data转换成json对象
            JSONObject jsonObject = JSONObject.parseObject(data);
            String tsValue = jsonObject.getString("ts");
            headers.put("timestamp", tsValue);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return event;
    }

    // 批量处理event
    @Override
    public List<Event> intercept(List<Event> list) {

        Iterator<Event> eventIterator = list.iterator();
        while (eventIterator.hasNext()){
            Event nextEvent = eventIterator.next();
            Event eventResult = intercept(nextEvent);
            if(eventResult == null){
                eventIterator.remove();
            }
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
