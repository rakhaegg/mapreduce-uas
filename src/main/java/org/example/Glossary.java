package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Glossary  extends Mapper<Object , Text, Text , Text> {

    public void map(Object key , Text value , Context context) throws IOException, InterruptedException{
        String txt = value.toString();
        String[] tokens = txt.split(",");
        if(tokens[0].compareTo("GLOSS_ID") != 0){
            context.write(new Text(tokens[0].trim()) , new Text(tokens[1].trim()));
        }
    }
}
