package org.example;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
public class ReducerBombingOperations extends Reducer<Text , Text , Text , Text> {

    private final String url = "jdbc:postgresql://localhost:5432/mapreduce";


    private final String user = "developer";
    private final String password = "rakhaelangx1";

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
            if(!mapMission.containsKey(val.toString())||mapMission.size() == 0 ){
                //counteryFlyMissin.add(val.toString().trim());
                    if(val.toString().trim().length() >0 ){
                        mapMission.put(val.toString() , 1);

                    }else{
                        if(mapMission.containsKey("Kosong")){
                            int temp = mapMission.get("Kosong") +1;
                            mapMission.put("Kosong" , temp);
                        }else{
                            mapMission.put("Kosong" , 1);
                        }
                    }

            } else if(mapMission.containsKey(val.toString())) {
                for(String i : mapMission.keySet()){
                    if(i.equals(val.toString())){
                        int temp = mapMission.get(i) + 1;
                        mapMission.put(val.toString() , temp);
                        break;
                    }

                }
            }
        }

        System.out.println(mapMission.toString());
        context.write(new Text(key.toString()) , new Text(mapMission.toString()));
        //output.collect(new Text(key.toString()) , new Text(mapMission.toString()));


    }
}
