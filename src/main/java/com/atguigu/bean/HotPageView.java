package com.atguigu.bean;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/23 16:20
 */
public class HotPageView {
    private String url;
    private Long viewCount;
    private Long windowEnd;

    public HotPageView() {
    }

    public HotPageView(String url, Long viewCount, Long windowEnd) {
        this.url = url;
        this.viewCount = viewCount;
        this.windowEnd = windowEnd;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getViewCount() {
        return viewCount;
    }

    public void setViewCount(Long viewCount) {
        this.viewCount = viewCount;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "HotPageView{" +
                "url='" + url + '\'' +
                ", viewCount=" + viewCount +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
