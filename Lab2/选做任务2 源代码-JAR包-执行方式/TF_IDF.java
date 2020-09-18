import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class TF_IDF {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            Path originalPath = new Path(args[0]);
            Path inputPath = new Path(args[1]);
            Path outputPath = new Path(args[2]);
            FileSystem hdfs = FileSystem.get(conf);
            FileStatus[] files = hdfs.listStatus(originalPath);
            conf.set("fileNumbers", (Integer.toString(files.length)));

            if(hdfs.exists(outputPath))
                hdfs.delete(outputPath, true);

            //定义job
            Job job = Job.getInstance(conf, "TF-IDF");
            job.setJarByClass(TF_IDF.class);
            //指定输入格式和输入输出入境
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            //指定mapper
            job.setMapperClass(TFIDFMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //指定reducer
            job.setReducerClass(TFIDFReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // 伪分布测试时, 可以指定reducer个数，否则默认1个
            // job.setNumReduceTasks(3);

            System.exit(job.waitForCompletion(true)?0:1);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}

class TFIDFMapper extends Mapper<Text, Text, Text, Text>
{
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String word = key.toString();
        String[] info  = value.toString().split(",")[1].replace(" ", "").split(";");
        Map<String, Integer> author_TF = new HashMap<String, Integer>();
        int fileNumbers = Integer.parseInt(context.getConfiguration().get("fileNumbers"));
        // System.out.println(fileNumbers);
        double idf = Math.log((double)fileNumbers / (double)(info.length+1));
        for(String i : info)
        {
            String author = i.split(":")[0];
            int j = 0;
            for(; j < author.length(); j++)
                if (author.charAt(j) >= '0' && author.charAt(j) <= '9')
                    break;
            author = author.substring(0, j);
            int cnt = Integer.parseInt(i.split(":")[1]);
            if(author_TF.containsKey(author))
                author_TF.put(author, author_TF.get(author) + cnt);
            else
                author_TF.put(author, cnt);
        }
        for(Map.Entry<String, Integer> entry : author_TF.entrySet())
        {
            if(!entry.getKey().equals(""))
            {
                context.write(new Text(entry.getKey()), new Text(key.toString()+"#"+String.format("%.02f-%.02f",entry.getValue().doubleValue(),idf)));
            }
        }
    }
}

class TFIDFReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // super.reduce(key, values, context);
        Iterator<Text> i = values.iterator();
        while(i.hasNext())
        {
            context.write(key, i.next());
        }
    }
}

