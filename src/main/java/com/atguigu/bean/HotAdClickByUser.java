package com.atguigu.bean;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/23 16:46
 */
public class HotAdClickByUser {
    private Long userId;
    private Long adId;
    private Long clickCount;
    private Long windowEnd;

    public HotAdClickByUser() {
    }

    public HotAdClickByUser(Long userId, Long adId, Long clickCount, Long windowEnd) {
        this.userId = userId;
        this.adId = adId;
        this.clickCount = clickCount;
        this.windowEnd = windowEnd;
    }


    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }


    @Override
    public String toString() {
        return "HotAdClickByUser{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", clickCount=" + clickCount +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
