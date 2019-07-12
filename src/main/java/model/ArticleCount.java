package model;


import java.io.Serializable;

public class ArticleCount implements Serializable {

    private static final long serialVersionUID = 1L;

    private long window;
    private String articleId;
    private long count = 0;

    public ArticleCount(long window, String articleId, long count) {
        this.window = window;
        this.articleId = articleId;
        this.count = count;
    }

    public ArticleCount() {
    }


    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getWindow() {
        return window;
    }

    public void setWindow(long window) {
        this.window = window;
    }

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    @Override
    public String toString() {
        return "articleID: "+this.articleId + "\tcount: "+this.count;
    }
}
