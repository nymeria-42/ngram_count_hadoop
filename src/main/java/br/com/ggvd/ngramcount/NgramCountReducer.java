/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.ggvd.ngramcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class NgramCountReducer extends
        Reducer<Text, Text, Text, Text> {
    Text result = new Text();
    
    @Override
    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Configuration c = context.getConfiguration();
        int minCount = c.getInt("minCount", 5);
        
        
        int sum = 0;
        List<String> fileList = new ArrayList<>();
        for (Text value : values) {
            String textvalue = value.toString();
            String[] parts = textvalue.split("\t");
            int countPart = Integer.parseInt(parts[0]);
            String filenamePart = parts[1];
            fileList.add(filenamePart);
            sum += countPart;
        }
        
        Set<String> fileSet = new HashSet<String>();
        fileSet.addAll(fileList);
        
        StringBuilder final_result = new StringBuilder();
        final_result.append(sum).append("\t"); 
        
        for (String item : fileSet) {
            final_result.append(item);
            final_result.append("\t");
        }
        
        result.set(final_result.toString());
        if (sum >= minCount) {
            context.write(text, result);
        }
    }
        
    }