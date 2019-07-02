package model;

import java.io.Serializable;

public class Post implements Serializable {


    private long approveDate;
    private String articleId;
    private int articleWordCount;
    private int commentID;
    private String commentType;
    private long createDate;
    private int depth;
    private boolean editorsSelection;
    private int inReplyTo;
    private String parentUserDispalyName;
    private int recommendations;
    private String sectionName;
    private String userDisplayName;
    private int userID;
    private String userLocation;

    public Post() {
    }

    public long getApproveDate() {
        return approveDate;
    }

    public void setApproveDate(long approveDate) {
        this.approveDate = approveDate;
    }

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public int getArticleWordCount() {
        return articleWordCount;
    }

    public void setArticleWordCount(int articleWordCount) {
        this.articleWordCount = articleWordCount;
    }

    public int getCommentID() {
        return commentID;
    }

    public void setCommentID(int commentID) {
        this.commentID = commentID;
    }

    public String getCommentType() {
        return commentType;
    }

    public void setCommentType(String commentType) {
        this.commentType = commentType;
    }

    public long getCreateDate() {
        return createDate;
    }

    public void setCreateDate(long createDate) {
        this.createDate = createDate;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public boolean isEditorsSelection() {
        return editorsSelection;
    }

    public void setEditorsSelection(boolean editorsSelection) {
        this.editorsSelection = editorsSelection;
    }

    public int getInReplyTo() {
        return inReplyTo;
    }

    public void setInReplyTo(int inReplyTo) {
        this.inReplyTo = inReplyTo;
    }

    public String getParentUserDispalyName() {
        return parentUserDispalyName;
    }

    public void setParentUserDispalyName(String parentUserDispalyName) {
        this.parentUserDispalyName = parentUserDispalyName;
    }

    public int getRecommendations() {
        return recommendations;
    }

    public void setRecommendations(int recommendations) {
        this.recommendations = recommendations;
    }

    public String getSectionName() {
        return sectionName;
    }

    public void setSectionName(String sectionName) {
        this.sectionName = sectionName;
    }

    public String getUserDisplayName() {
        return userDisplayName;
    }

    public void setUserDisplayName(String userDisplayName) {
        this.userDisplayName = userDisplayName;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public String getUserLocation() {
        return userLocation;
    }

    public void setUserLocation(String userLocation) {
        this.userLocation = userLocation;
    }
}
