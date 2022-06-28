package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.Timestamp;

public class BombingOperations extends Mapper<Object , Text, Text , Text> {

    protected void map(Object key , Text value , Context context) throws IOException, InterruptedException {
        String txt = value.toString();
        String[] tokens = txt.split(",");

        if(tokens[0].compareTo("THOR_DATA_VIET_ID") != 0){
            context.write(new Text(tokens[6].trim()) , new Text(tokens[1].trim()));
        }

        //        System.out.println(name.compareTo("city"));
//        if(name.compareTo("City") != 0){
//            context.write(new Text(id) , new Text(name));
//        }

    }}
