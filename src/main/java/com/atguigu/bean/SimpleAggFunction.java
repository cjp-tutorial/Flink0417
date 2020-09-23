package com.atguigu.bean;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/23 16:17
 */
public class SimpleAggFunction<T> implements AggregateFunction<T, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(T value, Long accumulator) {
        return accumulator + 1L;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
