package model;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

public class ArticleCount implements Serializable {

    private String articleId;
    private int count;

    public ArticleCount(String articleId, int count) {
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

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }


}
