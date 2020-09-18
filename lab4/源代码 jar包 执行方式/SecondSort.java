import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class SecondSort {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);
            FileSystem hdfs = FileSystem.get(conf);
            if(hdfs.exists(outputPath))
                hdfs.delete(outputPath, true);

            Job job = Job.getInstance(conf, "SecondSort");
            job.setJarByClass(SecondSort.class);
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(SecondSortMapper.class);
            job.setMapOutputKeyClass(SortPair.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setPartitionerClass(SecondSortPartitioner.class);

            job.setGroupingComparatorClass(SecondSortGroupingComparator.class);

            job.setNumReduceTasks(4);
            job.setReducerClass(SecondSortReducer.class);
            job.setOutputKeyClass(SortPair.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            System.exit(job.waitForCompletion(true)?0:1);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}

class SecondSortMapper extends Mapper<LongWritable, Text, SortPair, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //FileSplit fs = (FileSplit) context.getInputSplit();
        //String fileName = fs.getPath().getName();
        String[] elements = value.toString().split("\t");
        SortPair pair = new SortPair();
        pair.setFirst(Integer.parseInt(elements[0]));
        pair.setSecond(Integer.parseInt(elements[1]));
        context.write(pair, NullWritable.get());
    }
}

class SecondSortPartitioner extends Partitioner<SortPair, NullWritable> {
    @Override
    public int getPartition(SortPair pair, NullWritable nullWritable, int i) {
        if(i <= 1 || i >= 10)
            return pair.hashCode() % i;
        else {
            int num = 10/i +1;
            return pair.hashCode()/num;
        }
    }
}

class SecondSortGroupingComparator extends WritableComparator {
    SecondSortGroupingComparator() {
        super(SortPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        SortPair oa = (SortPair) a;
        SortPair ob = (SortPair) b;
        int c1 = oa.getFirst();
        int c2 = ob.getFirst();
        if(c1 < c2) {
            return -1;
        }
        if(c1 == c2) {
            return 0;
        }
        return 1;
    }
}

class SecondSortReducer extends Reducer<SortPair, NullWritable, SortPair, NullWritable> {
    @Override
    protected void reduce(SortPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> i = values.iterator();
        while(i.hasNext()) {
            i.next();
            context.write(key,NullWritable.get());
        }
    }
}

class SortPair implements WritableComparable<SortPair> {
    private IntWritable first;
    private IntWritable second;
    public SortPair() {
        first = new IntWritable(0);
        second = new IntWritable(0);
    }
    //将Pair作为结果写出来时用到
    @Override
    public String toString() {
        String res = first.toString()+"\t"+second.toString();
        return res;
    }
    //write readFields用于序列化和反序列化, 在网络上传输
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first.get());
        out.writeInt(second.get());
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        first.set(in.readInt());
        second.set(in.readInt());
    }
    //compareTo用于比较排序
    @Override
    public int compareTo(SortPair o) {
        int a1 = first.get();
        int b1 = second.get();
        int a2 = o.getFirst();
        int b2 = o.getSecond();
        if(a1 < a2)
            return -1;
        if(a1 > a2)
            return 1;
        if(b1 > b2)
            return -1;
        if(b1 < b2)
            return 1;
        return 0;
    }
    @Override
    public int hashCode() {
        return first.get();
    }
    public void setFirst(int val) { this.first.set(val);}
    public int getFirst() { return first.get(); }
    public void setSecond(int val) { this.second.set(val);}
    public int getSecond() { return second.get(); }
}