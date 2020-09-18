import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndexer {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            Job job = Job.getInstance(conf, "inverted index");
            job.setJarByClass(InvertedIndexer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setCombinerClass(InvertedIndexCombiner.class);
            job.setPartitionerClass(InvertedIndexPartitioner.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //job.setNumReduceTasks(10);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true)?0:1);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}

class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, values, context);
        // 输入键值对 是 行号 当前行内容
        // 输出键值对 是 词语#文件名 计数(1)

        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        // 去除文件名的后缀(.txt or .txt.segmented)
        if(fileName.indexOf(".txt.segmented") > 0)
            fileName = fileName.replace(".txt.segmented", "");
        if(fileName.indexOf(".txt") > 0)
            fileName = fileName.replace(".txt", "");
        if(fileName.indexOf(".TXT") > 0)
            fileName = fileName.replace(".TXT", "");
        if(fileName.indexOf(".TXT.segmented") > 0)
            fileName = fileName.replace(".TXT.segmented", "");
        if(fileName.indexOf(".segmented") > 0)
            fileName = fileName.replace(".segmented", "");

        Text word_file = new Text();
        StringTokenizer line = new StringTokenizer(value.toString());
        while(line.hasMoreTokens()){
            word_file.set(line.nextToken()+"#"+fileName);
            context.write(word_file, new IntWritable(1));
        }
    }
}

class InvertedIndexCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // super.reduce(key, values, context);
        // 自定义combiner, 将相同key的键值对合并, 实现对同一个文档内同一个词语的计数
        // 输入是具有相同key的键值对的iterator
        // 输出是将相同key的键值对合并(对计数求和)

        Iterator<IntWritable> i = values.iterator();
        int cnt = 0;
        while(i.hasNext())
            cnt = cnt + i.next().get(); // Intwritable -> int 调用get函数
        context.write(key, new IntWritable(cnt));
    }
}

class InvertedIndexPartitioner extends HashPartitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        // 稍加修改, 使用key进行hash转变成使用key的一部分进行hash
        String word = key.toString().split("#")[0];
        return super.getPartition(new Text(word), value, numReduceTasks);
    }
}

class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
    private String word = "";
    private int fileCnt = 0;
    private int wordCnt = 0;
    private StringBuffer info = new StringBuffer();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // super.reduce(key, values, context);
        String wordIn = key.toString().split("#")[0];
        String fileIn = key.toString().split("#")[1];
        if(!wordIn.equals(word) && !word.equals("")) {
            // System.out.println(word+"-"+wordIn);
            context.write(new Text(word), new Text(String.format("%.2f, %s",(double)wordCnt/(double)fileCnt, info.toString())));
            //if(word.equals("江湖") || word.equals("风雪"))
                //System.out.println(String.format("%.2f, %s",(double)wordCnt/(double)fileCnt, info.toString()));
            word = wordIn;
            fileCnt = 0;
            wordCnt = 0;
            info.setLength(0);
        }
        if(word.equals(""))
            word = wordIn;
        fileCnt += 1;
        int cnt = 0;
        Iterator<IntWritable> i = values.iterator();
        while(i.hasNext())
            cnt += i.next().get();
        wordCnt += cnt;
        if(info.length() == 0)
            info.append(fileIn+":"+cnt);
        else
            info.append("; "+fileIn+":"+cnt);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // super.cleanup(context);
        context.write(new Text(word), new Text(String.format("%.2f, %s",(double)wordCnt/(double)fileCnt, info.toString())));
    }
}
