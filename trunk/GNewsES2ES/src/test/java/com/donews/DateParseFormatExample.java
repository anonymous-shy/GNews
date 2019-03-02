package com.donews;

/**
 * Created by Shy on 2017/10/23
 */

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateParseFormatExample {

    public static void main(String[] args) {

        //Format examples
        LocalDate date = LocalDate.now();
        //default format
        System.out.println("1.Default format of LocalDate = " + date);
        //specific format
        System.out.println(date.format(DateTimeFormatter.ofPattern("d::MMM::uuuu")));
        System.out.println(date.format(DateTimeFormatter.BASIC_ISO_DATE));

        LocalDateTime dateTime = LocalDateTime.now();
        //default format
        System.out.println("2.Default format of LocalDateTime = " + dateTime);
        //specific format
        System.out.println(dateTime.format(DateTimeFormatter.ofPattern("d::MMM::uuuu HH::mm::ss")));
        System.out.println(dateTime.format(DateTimeFormatter.BASIC_ISO_DATE));

        Instant timestamp = Instant.now();
        //default format
        System.out.println("3.Default format of Instant = " + timestamp);

        //Parse examples
        LocalDateTime dt = LocalDateTime.parse("27::Apr::2014 21::39::48",
                DateTimeFormatter.ofPattern("d::MMM::yyyy HH::mm::ss"));
        System.out.println("Default format after parsing = " + dt);
//        LocalDateTime dt = LocalDateTime.parse("Sun Oct 22 10:36:19 CST 2017",
//                DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy"));
//        System.out.println("4.Default format after parsing = " + dt);
    }

}
