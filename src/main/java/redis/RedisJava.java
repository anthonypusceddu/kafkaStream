package redis;

import redis.clients.jedis.Jedis;

public class RedisJava {
    public static Jedis connect() {

        //connessione verso il server Redis locale
        Jedis j = new Jedis("localhost");
        //jedis.auth("password");//password

        System.out.println("Connection to server sucessfully");
        //controlla se il server è in funzione
        System.out.println("Server is running: "+j.ping());
        j.flushAll();
        return j;
    }
}