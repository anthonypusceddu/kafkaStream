package query1;

import model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import serDes.*;
import utils.Config;

import java.util.*;

public class Query1 {

   public static KTable<Long, String> executeQuery(KTable<Windowed<String>, Long> counts){

       Comparator<ArticleCount> byCount = Comparator.comparingLong(ArticleCount::getCount);
        /*
          group by window
          aggregate in priority queue already ordered
         */
       KTable<Long, PriorityQueue<ArticleCount>> aggregate = counts
               .groupBy((w, c) -> new KeyValue<>(w.window().start(), new ArticleCount(w.key(), c))
                       , Grouped.with(Serdes.Long(), Serdes.serdeFrom(new ArticleCountSerializer(), new ArticleCountDeserializer()))
               )
               .aggregate(
                       () -> new PriorityQueue<>(byCount),

                       (windowedString, articleCount, queue) -> {
                           queue.add(articleCount);
                           return queue;
                       },

                       (windowedString, articleCount, queue) -> {
                           queue.remove(articleCount);
                           return queue;
                       },
                       Materialized.with(Serdes.Long(), new PriorityQueueSerde<>(byCount, new ArticleCountSerializer(), new ArticleCountDeserializer())));

       /* aggregate.toStream().foreach(new ForeachAction<Long, PriorityQueue<ArticleCount>>() {
            @Override
            public void apply(Long key, PriorityQueue<ArticleCount> articleCounts) {
                System.out.println("key: "+ key+"\tlista: "+ articleCounts.toString());
            }
        });*/


       /*
         map values to take the first 10 articleIDs
        */
       KTable<Long, String> topViewCounts = aggregate
               .mapValues(queue -> {
                   final StringBuilder sb = new StringBuilder();
                   for (int i = 0; i < Config.topN; i++) {
                       ArticleCount record = queue.poll();
                       if (record == null) {
                           break;
                       }
                       sb.append(record.getArticleId());
                       sb.append("\n");
                   }
                   return sb.toString();
               });

       return topViewCounts;

   }


}
