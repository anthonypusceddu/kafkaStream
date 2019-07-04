package model;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

public class ArticleCount implements Serializable {

    private String articleId;
    private long count;

    public ArticleCount(String articleId, long count) {
        this.articleId = articleId;
        this.count = count;
    }

    public ArticleCount() {
    }

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "articleID: "+this.articleId;
    }
}
