package com.atguigu.bean;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/19 10:18
 */
public class OrderEvent {

    private Long orderId;
    /**
     * 事件类型：下单、支付
     */
    private String eventType;
    /**
     * 交易码：支付成功生成的交易码
     */
    private String txId;
    private Long eventTime;

    public OrderEvent() {
    }

    public OrderEvent(Long orderId, String eventType, String txId, Long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.txId = txId;
        this.eventTime = eventTime;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                ", txId='" + txId + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
