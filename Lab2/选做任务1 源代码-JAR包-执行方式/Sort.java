import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

import com.jcraft.jsch.Random;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sort {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);
            Path partitionPath = new Path(args[2]);
            if(hdfs.exists(outputPath))
                hdfs.delete(outputPath);
            if(hdfs.exists(partitionPath))
                hdfs.delete(partitionPath);

            conf.set("mapreduce.totalorderpartitioner.naturalorder","false");
            conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, ",");
            Job job2 = Job.getInstance(conf, "sort");
            job2.setJarByClass(Sort.class);

            job2.setInputFormatClass(KeyValueTextInputFormat.class);

            job2.setSortComparatorClass(KeyComparator.class);
            job2.setMapperClass(SortMapper.class);
            job2.setReducerClass(SortReducer.class);
            job2.setNumReduceTasks(4);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, inputPath);
            FileOutputFormat.setOutputPath(job2, outputPath);

            TotalOrderPartitioner.setPartitionFile(job2.getConfiguration(), partitionPath);
            job2.setPartitionerClass(TotalOrderPartitioner.class);
            InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1,10000,10);
            InputSampler.writePartitionFile(job2, sampler);

            System.exit(job2.waitForCompletion(true)?0:1);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}

class KeyComparator extends WritableComparator {
    protected KeyComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable writableComparable1, WritableComparable writableComparable2) {
        double num1 = Double.parseDouble(writableComparable1.toString().split("\t")[1]);
        double num2 = Double.parseDouble(writableComparable2.toString().split("\t")[1]);
        if(num1 > num2)
            return 1;
        else if(num1 < num2)
            return -1;
        else
            return 0;
    }
}

class SortMapper extends Mapper<Text, Text, Text, Text>
{
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}

class SortReducer extends Reducer <Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //context.write(key, NullWritable.get());
        Iterator<Text> i = values.iterator();
        while(i.hasNext()) {
            context.write(key,i.next());
        }
    }
}
