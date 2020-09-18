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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyJoin {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);
            FileSystem hdfs = FileSystem.get(conf);
            if(hdfs.exists(outputPath))
                hdfs.delete(outputPath, true);

            Job job = Job.getInstance(conf, "join");
            job.setJarByClass(MyJoin.class);
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(JoinMapper.class);
            job.setMapOutputKeyClass(OrderBean.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setPartitionerClass(JoinPartitioner.class);

            job.setGroupingComparatorClass(JoinGroupingComparator.class);

            job.setNumReduceTasks(4);
            job.setReducerClass(JoinReducer.class);
            job.setOutputKeyClass(OrderBean.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            System.exit(job.waitForCompletion(true)?0:1);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}

class JoinMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        String fileName = fs.getPath().getName();
        String[] elements = value.toString().split(" ");
        OrderBean orderBean = new OrderBean();
        if(fileName.equals("order.txt")) {
            orderBean.setId(elements[0]);
            orderBean.setDate(elements[1]);
            orderBean.setPid(elements[2]);
            orderBean.setNum(elements[3]);
        } else {
            orderBean.setPid(elements[0]);
            orderBean.setName(elements[1]);
            orderBean.setPrice(elements[2]);
        }
        context.write(orderBean, NullWritable.get());
    }
}

class JoinPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        return orderBean.hashCode() % i;
    }
}

class JoinGroupingComparator extends WritableComparator {
	//不设置构造函数将会抛掷空指针异常
    JoinGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        long pid1 = Long.parseLong(oa.getPid());
        long pid2 = Long.parseLong(ob.getPid());
        if(pid1 < pid2) {
            return -1;
        }
        if(pid1 == pid2) {
            return 0;
        }
        return 1;
    }

}

class JoinReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> i = values.iterator();
        i.next();
        String name = key.getName();
        String price = key.getPrice();
        while(i.hasNext()) {
            i.next();
            key.setName(name);
            key.setPrice(price);
            context.write(key,NullWritable.get());
        }
    }
}

class OrderBean implements WritableComparable<OrderBean> {
    private String id;
    private String date;
    private String pid;
    private String name;
    private String price;
    private String num;
    public OrderBean() {
        id = "";
        date = "";
        pid = "";
        name = "";
        price = "";
        num = "";
    }
    //将Orderean作为结果写出来时用到
    @Override
    public String toString() {
        String res = id+"\t"+date+"\t"+pid+"\t"+name+"\t"+price+"\t"+num;
        return res;
    }
    //write readFields用于序列化和反序列化, 在网络上传输
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(date);
        out.writeUTF(pid);
        out.writeUTF(name);
        out.writeUTF(price);
        out.writeUTF(num);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        date = in.readUTF();
        pid = in.readUTF();
        name = in.readUTF();
        price = in.readUTF();
        num = in.readUTF();
    }
    //compareTo用于比较排序
    @Override
    public int compareTo(OrderBean o) {
        Long pid1 = Long.parseLong(pid);
        Long pid2 = Long.parseLong(o.getPid());
        if(pid1 < pid2)
            return -1;
        if(pid1 > pid2)
            return 1;
        // pid相同，只有一个是商品，其他都是订单
        if(!this.price.equals(""))
            return -1;
        if(!o.getPid().equals(""))
            return 1;
        Long oid1 = Long.parseLong(id);
        Long oid2 = Long.parseLong(o.getId());
        if(oid1 < oid2)
            return -1;
        if(oid1 > oid2)
            return 1;
        return 0;
    }
    @Override
    public int hashCode() {
        return Integer.parseInt(pid);
    }
    public void setId(String id) { this.id = id;}
    public String getId() { return id;}
    public void setDate(String date) { this.date = date;}
    public String getDate() { return date;}
    public void setPid(String pid) { this.pid = pid;}
    public String getPid() { return pid;}
    public void setName(String name) { this.name = name;}
    public String getName() { return name;}
    public void setPrice(String price) { this.price = price;}
    public String getPrice() { return price;}
    public void setNum(String num) { this.num = num;}
    public String getNum() { return num;}
}