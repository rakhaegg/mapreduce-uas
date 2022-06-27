package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

public class ReducerBombingOperations extends Reducer<Text , Text , Text , IntWritable> {

    private final IntWritable result = new IntWritable();
    private Text cityName = new Text("Unknown");

    public void reduce(Text key , Iterable<Text> values , Context context) throws IOException , InterruptedException{
        System.out.println("-----------------------");
        System.out.println("Key : "  + key.toString());
        System.out.println("-----------------------");

        ArrayList<String> counteryFlyMissin = new ArrayList<String>();
        HashMap<String , Integer> mapMission = new HashMap<String, Integer>();

        Iterable<Text> xx = values;
        for (Text val : values){
            if(mapMission.size() == 0 && !(val.getLength() == 0)){
                //counteryFlyMissin.add(val.toString().trim());
                mapMission.put(val.toString() , 1);
            }else if(mapMission.containsKey(val.toString()) && !(val.getLength() == 0 ) ) {
                for(String i : mapMission.keySet()){
                    if(i.equals(val.toString())){
                        int temp = mapMission.get(i) + 1;
                        mapMission.put(val.toString() , temp);
                    }
                }
            }
        }
        System.out.println(mapMission);

    }
}
