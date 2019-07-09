package model;

import utils.Config;

public class Score {

    private int like;
    private int indirect_comment_count;



    private double score = 0.0;

    public Score(int like, int indirect_comment_count) {
        this.like = like;
        this.indirect_comment_count = indirect_comment_count;
    }

    public Score() {
    }

    public int getLike() {
        return like;
    }

    public void setLike(int like) {
        this.like = like;
    }

    public int getIndirect_comment_count() {
        return indirect_comment_count;
    }

    public void setIndirect_comment_count(int indirect_comment_count) {
        this.indirect_comment_count = indirect_comment_count;
    }

    public void addCount(int i) {
        this.indirect_comment_count++;
    }
    public void addLike(int i) {
        this.like+=i;
    }


    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }


    public void clearScore(){
        this.like =0;
        this.indirect_comment_count = 0;
    }

    @Override
    public String toString() {
        return "Like: "+ this.like +"\t Count: "+ this.indirect_comment_count+"\t Score: "+ this.score;
    }

    public void calculateScore() {
        this.score = Config.wa * this.like + Config.wb * this.indirect_comment_count;
    }
}
