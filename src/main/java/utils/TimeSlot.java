package utils;

import model.Post;
import org.joda.time.DateTime;

public class TimeSlot {

    public static Integer getTimeSlot(Post post) {
        int timeSlot = 0;
        //DateTimeFormatter dtf = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");
        DateTime date = new DateTime(post.getCreateDate());
        int hour = date.getHourOfDay();
        if ( hour>=0 & hour<2){
            timeSlot = 0;
        }else if ( hour>=2 & hour<4){
            timeSlot = 2;
        } else if ( hour>=4 & hour<6){
            timeSlot=4;
        } else if ( hour>=6 & hour<8){
            timeSlot=6;
        } else if ( hour>=8 & hour<10){
            timeSlot=8;
        } else if ( hour>=10 & hour<12){
            timeSlot=10;
        }else if ( hour>=12 & hour<14){
            timeSlot=12;
        } else if ( hour>=14 & hour<16){
            timeSlot=14;
        } else if ( hour>=16 & hour<18){
            timeSlot=16;
        } else if ( hour>=18 & hour<20){
            timeSlot=18;
        } else if ( hour>=20 & hour<22){
            timeSlot=20;
        }else {
            timeSlot=22;
        }
        return timeSlot;
    }
}
