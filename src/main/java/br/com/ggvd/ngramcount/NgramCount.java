/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.ggvd.ngramcount;

import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class NgramCount {
 
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
 
        // Captura o parâmetros passados após o nome da Classe driver.
        int ngrams = Integer.parseInt(args[0]);
        int minCount = Integer.parseInt(args[1]);
        Path inputPath = new Path("file:///" + args[2]);
        Path outputPath = new Path("file:///" + args[3]);
        
 
        // Criar uma configuração
        Configuration conf = new Configuration(true);
        conf.setInt("ngrams", ngrams);
        conf.setInt("minCount", minCount);
 
        // Criar o job
        Job job = new Job(conf, "ContaPalavras");
        job.setJarByClass(NgramCount.class);
 
        // Definir classes para Map e Reduce
        job.setMapperClass(NgramCountMapper.class);
        job.setReducerClass(NgramCountReducer.class);
        job.setNumReduceTasks(1);
 
        // Definir as chaves e valor
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
 
        // Entradas
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
 
        // Saidas
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // EXcluir saida se existir
//        FileSystem hdfs = FileSystem.get(conf);;
//        if (hdfs.exists(outputDir))
//            hdfs.delete(outputDir, true);
//        FileSystem fs = FileSystem.get(conf);
        LocalFileSystem localFileSystem = FileSystem.getLocal(conf);
        if (localFileSystem.exists(outputPath))
            localFileSystem.delete(outputPath, true);
 
        // Executa job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
 
    }
}