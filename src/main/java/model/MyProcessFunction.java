package model;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import scala.Tuple5;
import utils.Config;

import java.time.Duration;

public class MyProcessFunction implements ProcessorSupplier {


    @Override
    public Processor get() {
        return new Processor() {
            private State valueState;

            private ProcessorContext context;
            private KeyValueStore<Long, Tuple5<Integer, Integer, Integer, Integer, Integer>> kvStore;

            @Override
            public void init(ProcessorContext processorContext) {
                // keep the processor context locally because we need it in punctuate() and commit()
                this.context = processorContext;
                valueState = new State(Config.START);

                // retrieve the key-value store named "Counts"
                kvStore = (KeyValueStore) processorContext.getStateStore("myProcessorState");


                //this.state = processorContext.getStateStore("myProcessorState");
                // punctuate each second, can access this.state
                processorContext.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME,(timestamp) -> {

                    KeyValueIterator<Long,Tuple5<Integer, Integer, Integer, Integer, Integer>> iter = this.kvStore.all();
                    while (iter.hasNext()) {
                        KeyValue<Long,Tuple5<Integer, Integer, Integer, Integer, Integer>> entry = iter.next();
                        processorContext.forward(entry.key, entry.value.toString());
                    }
                    iter.close();

                    // commit the current processing progress
                    processorContext.commit();
                });
            }

            @Override
            public void process(Object o, Object o2) {
                Tuple5<Integer, Integer, Integer, Integer, Integer> app= (Tuple5<Integer, Integer, Integer, Integer, Integer>)o2;
                System.out.println(app._2());


                int usrID= app._1();
                int commentId = app._5();
                int replyTo = app._4();


                valueState.setJedis();



            }

            public void close() {
                // can access this.state
            }
        };
    }


}
