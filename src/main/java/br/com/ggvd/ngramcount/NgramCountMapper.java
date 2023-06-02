/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.ggvd.ngramcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class NgramCountMapper extends
        Mapper<Object, Text, Text, Text> {

//    private final IntWritable ONE = new IntWritable(1);

    private final Text word = new Text();
    private final Text result = new Text();

    
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
        Configuration c = context.getConfiguration();
        int ngram = c.getInt("ngrams", 2);

        String line = value.toString();
//        String[] words = line.replaceAll("[^\\w\\s]", "").toLowerCase().split("\\s+");
        List<String> words = new ArrayList<String>(Arrays.asList(line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase().split("\\s+")));
        if (!words.isEmpty()&& words.get(0).trim().length() == 0){
            words.remove(0);
        }
        if (words.size() > ngram){
            for (int i = 0; i < words.size() - (ngram - 1); i++) {
                StringBuilder ngramBuilder = new StringBuilder();
                for (int j = 0; j < ngram; j++) {
                    if (j > 0) {
                        ngramBuilder.append(" ");
                    }
                    ngramBuilder.append(words.get(i + j).trim());
                }


                String ngramText = ngramBuilder.toString();
                String map_value = "1"  + '\t' + fileName;
                word.set(ngramText);
                result.set(map_value);

                context.write(word, result);
            }
    }}
}
