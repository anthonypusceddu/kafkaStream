package model;

import redis.RedisJava;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class State {
    //key: commentId_LvL2, value: CommentId_LvL1
    private HashMap<Integer,Integer> LvL2ToLvL1Map = new HashMap<>();
    //key: commentId_LvL1, value: UserID
    private HashMap<Integer,Integer> LvL1ToUsrIdMap = new HashMap<>();
    //key : User_Id, value: user_score( = wa*Like +wb*count)
    private HashMap<Integer,Score> hUserScore = new HashMap<>();

    private HashMap<Integer,Score> hUserScoreWindow2 = new HashMap<>();

    private HashMap<Integer,Score> hUserScoreWindow3 = new HashMap<>();

    private int day;
    private int month;


    // first timestamp in window
    private Long timestamp;
    //private boolean isFirstTimeInWindow= true;

    Jedis jedis ;



    public State() {
    }

    public State(Long timestamp) {
        this.timestamp = timestamp;
        this.day=0;
        this.month=0;
    }

    public Jedis getJedis() {
        return jedis;
    }

    public void setJedis() {
        if(this.jedis==null){
            this.jedis = RedisJava.connect();


            this.jedis.flushAll();
            this.hUserScore.clear();
            this.LvL2ToLvL1Map.clear();
            this.LvL1ToUsrIdMap.clear();
        }


        //Thread.sleep(1000);

    }

    public boolean searchUserInRedis(int usrID){
        Long nrem= this.jedis.lrem("hUserScore", 1,String.valueOf(usrID));
        this.jedis.lpush("hUserScore", String.valueOf(usrID));
        boolean exists;
        if(nrem>0)
            exists=true;
        else
            exists=false;
       return exists;
    }

    public boolean usrExist(Integer usrID) {
        //controllo se l'utente esiste nella lista locale
        //se non esiste controllo in redis
        //se in redis esiste allora ne faccio una copia nella lista locale
        boolean existsUsr = this.hUserScore.containsKey(usrID);
        if (existsUsr == false){
            boolean exists=searchUserInRedis(usrID);
            if(exists==true)
                hUserScore.put(usrID,new Score(0,0) );
            return exists;

        }
        return true;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public void updateLikeScore(Integer usrID, Integer like){
        //Deserialize
        /*String r= this.jedis.hget("hUserScore",String.valueOf(usrID));
        Score score=Deserializer.deserialize_score(r);
        score.addLike(like);
        score.calculateScore();
        //serializzo
        String score_serializer=Serializer.serialize_score(score);
        this.jedis.hset("hUserScore",String.valueOf(usrID),score_serializer)*/

        Score s = this.hUserScore.get(usrID);
        s.addLike(1);
        s.calculateScore();
        this.hUserScore.replace(usrID,s);
    }


    public void updateCountScore(Integer usrID){
        //Deserialize
        /*String r= this.jedis.hget("hUserScore",String.valueOf(usrID))
        Score score=Deserializer.deserialize_score(r);
        score.addCount(1);
        score.calculateScore();
        //serializzo
        String score_serializer=Serializer.serialize_score(score);
        this.jedis.hset("hUserScore",String.valueOf(usrID),score_serializer)*/

        Score s = this.hUserScore.get(usrID);

        s.addCount(1);
        s.calculateScore();
        this.hUserScore.replace(usrID,s);
    }

    public void addUser(int usrID,int like) {
        this.hUserScore.put(usrID,new Score(like,0));

        //String score_serialized= Serializer.serialize_score(new Score( like,0));
        //String str_usrid=String.valueOf(usrID);

        //this.jedis.hset("hUserScore",str_usrid,score_serialized)
        //this.jedis.lpush("hUserScore",str_usrid);
    }

    public void addCommentToUserReference(int commentId, int usrID) {
        LvL1ToUsrIdMap.put(commentId,usrID);

        String a=String.valueOf(commentId);
        String b= String.valueOf(usrID);
        this.jedis.hset("LvL1ToUsrIdMap",a,b);

    }

    public HashMap<Integer, Score> gethUserScoreWindow1() {
        return hUserScore;
    }

    public HashMap<Integer, Score> gethUserScoreWindow2() {
        return hUserScoreWindow2;
    }

    public HashMap<Integer, Score> gethUserScoreWindow3() {
        return hUserScoreWindow3;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public int retrieveUsrIdFromMap(int replyTo) {
        //cerco l'utente nella lista locale
        //se non c è lo cerco in redis e lo aggiungo nella lista locale
        int usrId=LvL1ToUsrIdMap.getOrDefault(replyTo, -1);
        if(usrId==-1){
            String str_usrid=this.jedis.hget("LvL1ToUsrIdMap",String.valueOf(replyTo));
            if(str_usrid != null){
                usrId=Integer.valueOf(str_usrid);

                //prendo l'utente da redis e lo aggiungo alla lista di utenti locale
                //String r= this.jedis.hget("hUserScore",String.valueOf(str_usrid));
                //Score score=Deserializer.deserialize_score(r);
                Score score= new Score(0,0);
                hUserScore.put(usrId,score);
            }
            return usrId;
        }
        return usrId;
    }

    public void addCommentToCommentReference(int commentId, int replyTo) {
        this.jedis.hset("LvL2ToLvL1Map",String.valueOf(commentId), String.valueOf(replyTo));

        LvL2ToLvL1Map.put(commentId,replyTo);
    }

    public int retrieveCommentIdfromMap(int replyTo) {
        //cerco il commento nella lista locale
        //se non c è lo cerco in redis
        int commentId=LvL2ToLvL1Map.getOrDefault(replyTo, -1);
        if(commentId==-1) {

            String str_commentId = this.jedis.hget("LvL2ToLvL1Map", String.valueOf(replyTo));
            if (str_commentId != null) {
                commentId = Integer.valueOf(str_commentId);
            }
            return commentId;
        }
        return commentId;
    }

    public void resetWindow1(long l) {
        this.timestamp = l;
        /*for ( String usr : this.jedis.hkeys("hUserScore")){
            String r= this.jedis.hget("hUserScore",String.valueOf(usr));

            //deserializer
            Score score_deserialized=Deserializer.deserialize_score(r);
            score_deserialized.clearScore();

            //serializzo
            String score_serialized=Serializer.serialize_score(score_deserialized);
            this.jedis.hset("hUserScore",usr,score_serialized);

        }*/

        //reset delle 3 hashmap di appoggio
        hUserScore.clear();
        LvL1ToUsrIdMap.clear();
        LvL2ToLvL1Map.clear();

    }

    public void resetWindow2() {
        this.day=0;
        hUserScoreWindow2.clear();
    }

    public void resetWindow3() {

        this.month=0;
        hUserScoreWindow3.clear();
    }

    public void joinHashmaph1h2(HashMap<Integer,Score> h1, HashMap<Integer,Score> h2) {
        Set list = h1.keySet();
        Iterator iter = list.iterator();

        while (iter.hasNext()) {
            Object key = iter.next();
            Score value = (Score) h1.get(key);

            if (h2.containsKey(key)) {
                //aggiornamento score
                Score scoreWindow2 = h2.get(key);
                Score newScore = new Score(scoreWindow2.getLike() + value.getLike(), scoreWindow2.getIndirect_comment_count() + value.getIndirect_comment_count());
                newScore.calculateScore();
                h2.replace((Integer) key, newScore);
            } else {
                h2.put((Integer) key, value);
            }
        }
    }

    public void joinHashmap () {
        //unisco hashmap user-score finestra 1-2 sommando gli score
        this.joinHashmaph1h2(this.hUserScore, this.hUserScoreWindow2);
        this.joinHashmaph1h2(this.hUserScoreWindow2, this.hUserScoreWindow3);

    }


}
