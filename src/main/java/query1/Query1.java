package query1;

import model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import serDes.*;
import utils.Config;
import utils.SerDes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static java.time.Duration.ofMinutes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class Query1 {

   public static KStream<Long, ArticleCount[]> executeQuery(KStream<Long, Post> source, int timeInHour, long until){


       /*
         map to (articleID, 1)
         groupByKey
         tumbling window 1 hour
         reduce - sum articles occurrencies
         suppress to have just the last record in window
         map to (window start time, ArticleCount)
         groupByKey
         tumbling window 1 hour
         aggregate to compute the top three articles
         suppress
         map to (window, array of ArticleCount)
        */
       KStream<Long, ArticleCount[]> counts = source
               .map((k, v) -> KeyValue.pair(v.getArticleId(), 1L))
               .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
               .windowedBy(TimeWindows.of(Duration.ofHours(timeInHour)).until(until).grace(ofMinutes(1)))
               .reduce(Long::sum)
               .suppress(Suppressed.untilWindowCloses(unbounded()))
               .toStream()
               .map(new KeyValueMapper<Windowed<String>, Long, KeyValue<Long, byte[]>>() {
                   @Override
                   public KeyValue<Long, byte[]> apply(Windowed<String> stringWindowed, Long aLong) {
                       return KeyValue.pair(stringWindowed.window().start(), SerDes.serialize(new ArticleCount(stringWindowed.window().start(), stringWindowed.key(), aLong)));
                   }
               })
               .groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
               .windowedBy(TimeWindows.of(Duration.ofHours(timeInHour)).until(until))
               .aggregate(new Initializer<byte[]>() {
                   @Override
                   public byte[] apply() {
                       ArticleCount[] list = new ArticleCount[]{new ArticleCount(), new ArticleCount(), new ArticleCount()};
                       return SerDes.serialize(list);
                   }
               }, new Aggregator<Long, byte[], byte[]>() {
                   @Override
                   public byte[] apply(Long aLong, byte[] article, byte[] articleList) {
                       ArticleCount[] al = (ArticleCount[]) SerDes.deserialize(articleList);
                       ArticleCount a = (ArticleCount) SerDes.deserialize(article);
                       for (int i = 0; i < 3; i++) {
                           if (al[i] == null) {
                               al[i] = a;
                               break;
                           }
                       }
                       long newCount = a.getCount();
                       if (newCount >= al[0].getCount()) {
                           for (int j = 1; j < 3; j++) {
                               al[j] = al[j - 1];
                           }
                           al[0] = a;
                       } else if (newCount >= al[1].getCount()) {
                           al[2] = al[1];
                           al[1] = a;
                       } else if (newCount >= al[2].getCount()) {
                           al[2] = a;
                       }
                       return SerDes.serialize(al);
                   }
               })
               .suppress(Suppressed.untilWindowCloses(unbounded())).toStream()
               .map(new KeyValueMapper<Windowed<Long>, byte[], KeyValue<Long, ArticleCount[]>>() {
                   @Override
                   public KeyValue<Long, ArticleCount[]> apply(Windowed<Long> longWindowed, byte[] bytes) {
                       return new KeyValue<>(longWindowed.key(), (ArticleCount[]) SerDes.deserialize(bytes));
                   }
               });

       return counts;

   }


}
