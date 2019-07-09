package model;

import java.util.Comparator;
import java.util.HashMap;

public class MyComparator implements Comparator {


    @Override
    public int compare(Object o1, Object o2) {
        HashMap<Integer, Score> app = (HashMap<Integer, Score>) o1;
        HashMap<Integer, Score> app2= (HashMap<Integer, Score>) o2;

        Double a=0.0;
        Double b=0.0;
        for(Score t:app.values())
        {
            a=t.getScore();
        }
        for(Score t:app2.values())
        {
            b=t.getScore();
        }


        if(a<b){
            return 1;
        }
        if(a>b){
            return -1;
        }
        return 0;
    }
}