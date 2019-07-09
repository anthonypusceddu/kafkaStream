package model;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import scala.Tuple2;
import scala.Tuple5;
import scala.Tuple6;
import utils.Config;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class MyProcessFunction implements ProcessorSupplier {


    @Override
    public Processor get() {

        return new Processor() {
            private State valueState;

            private ProcessorContext context;
            private KeyValueStore<Long, Tuple6<Integer, Integer, Integer, Integer, Integer,Long>> kvStore;

            @Override
            public void init(ProcessorContext processorContext) {
                // keep the processor context locally because we need it in punctuate() and commit()
                this.context = processorContext;
                valueState = new State(Config.START);
                valueState.setJedis();

                System.out.println("addio");

                // retrieve the key-value store named "Counts"
                kvStore = (KeyValueStore) processorContext.getStateStore("myProcessorState");


                //this.state = processorContext.getStateStore("myProcessorState");
                // punctuate each second, can access this.state
                processorContext.schedule(Duration.ofMillis(1), PunctuationType.STREAM_TIME,(timestamp) -> {

                    KeyValueIterator<Long, Tuple6<Integer, Integer, Integer, Integer, Integer,Long>> iter = this.kvStore.all();
                    while (iter.hasNext()) {

                        KeyValue<Long,Tuple6<Integer, Integer, Integer, Integer, Integer,Long>> entry = iter.next();
                        processorContext.forward(entry.key, entry.value.toString());
                    }
                    iter.close();

                    // commit the current processing progress
                    processorContext.commit();
                });
            }

            @Override
            public void process(Object o, Object o2) {
                Tuple6<Integer, Integer, Integer, Integer, Integer,Long> app= (Tuple6<Integer, Integer, Integer, Integer, Integer,Long>)o2;
                //System.out.println(app._2());

                Long newTimestamp= app._6();
                int usrID= app._1();
                int commentId = app._5();
                int replyTo = app._4();


                switch (app._2()){
                    case 1:
                        int like = app._2();
                        if ( valueState.usrExist(usrID)) {

                            this.valueState.updateLikeScore(usrID,like);

                        }else {

                            this.valueState.addUser(usrID,like);

                        }

                        this.valueState.addCommentToUserReference(commentId,usrID);
                        break;
                    case 2:
                        usrID =  this.valueState.retrieveUsrIdFromMap(replyTo);
                        if ( usrID !=-1) {
                            this.valueState.updateCountScore(usrID);
                            this.valueState.addCommentToCommentReference(commentId, replyTo);
                        }
                        break;
                    case 3:
                        commentId =  this.valueState.retrieveCommentIdfromMap(replyTo);
                        usrID = this.valueState.retrieveUsrIdFromMap(commentId);
                        if ( commentId!= -1 & usrID !=-1) {
                            this.valueState.updateCountScore(usrID);
                        }
                        break;
                }
                //System.out.println("new time"+newTimestamp);
                //System.out.println("ref"+valueState.getTimestamp());
                if (newTimestamp-valueState.getTimestamp() >= Config.H24){
                    System.out.println("PRIMO IF");

                    List<HashMap<Integer,Score>> list=this.createRank(valueState.gethUserScoreWindow1());
                    System.out.println(list);

                    valueState.joinHashmap();
                    valueState.resetWindow1(valueState.getTimestamp()+Config.H24);
                    valueState.setDay(valueState.getDay()+1);

                }
                if (valueState.getDay()==7){
                    List<HashMap<Integer,Score>> list=this.createRank(valueState.gethUserScoreWindow2());

                    System.out.println("SECONDO IF");
                    valueState.resetWindow2();
                    valueState.setMonth(valueState.getMonth()+1);
                }
                if (valueState.getMonth()==4){
                    List<HashMap<Integer,Score>> list=this.createRank(valueState.gethUserScoreWindow3());
                    System.out.println("TERZO IF");

                    valueState.resetWindow3();
                }
            }




            public List<HashMap<Integer,Score>> createRank(HashMap<Integer,Score> h){
                List<HashMap<Integer, Score>> treeMap = new ArrayList<>();

                Set list = h.keySet();
                Iterator iter = list.iterator();

                while (iter.hasNext()) {
                    Object key = iter.next();
                    Score value = (Score) h.get(key);

                    HashMap<Integer,Score> app = new HashMap<>();
                    app.put((Integer)key,value);
                    treeMap.add(app);
                }


                Collections.sort(treeMap, new MyComparator());

                treeMap=treeMap.stream().limit(10).collect(Collectors.toList());
                return treeMap;


            }

            public void close() {
                // can access this.state
            }
        };
    }


}
