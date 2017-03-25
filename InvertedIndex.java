import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex{

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text>{

        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            Text docId = new Text(words[0]);
            if(words.length >=2) {
                StringTokenizer itr = new StringTokenizer(words[1]);
                System.out.print(docId.toString());
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    context.write(word, docId);
                }
            }
        }

    }

    public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<String, Integer> hMap = new HashMap<>();

            for (Text val : values) {
                hMap.put(val.toString(), hMap.getOrDefault(val.toString(),0)+1);
            }

            StringBuilder result = new StringBuilder();
            for(String keyString: hMap.keySet()){
                result.append(keyString).append(":").append(hMap.get(keyString)).append("\t");
            }

            context.write(key, new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setMapOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}