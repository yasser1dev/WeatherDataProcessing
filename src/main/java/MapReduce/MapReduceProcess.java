package MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MapReduceProcess {
    public static final String outputDir= "C:\\Users\\oussa\\BigDataProject\\MapReduceIO\\output";
    public static final String inputFile = "C:\\Users\\oussa\\BigDataProject\\MapReduceIO\\input\\AllInOneFileData.csv";

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] data = line.split("\",\"");

            Text newKey = new Text();
            Double newValue;


            if(13 < data.length) {
                String date = data[1].replace("\"", "").split("T")[0];
                String year = date.split("-")[0];
                String month = date.split("-")[1];
                String temperatureField = data[13].replace("\"", "");
                String temperature = temperatureField.split(",")[0];
                String quality = temperatureField.split(",")[1];

                if(!temperature.equals("9999") && quality.equals("1")) {
                    newKey.set(year+"-"+month);
                    newValue = Double.valueOf(temperature)/10;

                    System.out.println("--------- MAP OUTPUT LINES : " + newKey.toString() + " :::: " + newValue);
                    context.write(newKey, new DoubleWritable(newValue));
                }
            }

        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double min = getMin(values);
            double max = getMax(values);
            double avg = getAvg(values);

            context.write(key, new Text(String.valueOf(min+","+max+","+avg)));
        }

        private double getAvg(Iterable<DoubleWritable> values) {
            double sum = 0;
            int length = 0;
            for(DoubleWritable value : values) {
                sum += value.get();
                length += 1;
            }
            System.out.println("########### AVG SUM : " + sum);
            System.out.println("########### AVG LEN : " + length);
            System.out.println("########### AVG : " + sum/length);
            return sum/length;
        }

        private double getMax(Iterable<DoubleWritable> values) {
            double max = Double.MIN_VALUE;
            for (DoubleWritable value : values) {
                if (value.get() > max) max = value.get();
            }
            System.out.println("########### MAX : " + max);
            return max;
        }

        private double getMin(Iterable<DoubleWritable> values) {
            System.out.println("############ values : ");
            for(DoubleWritable value: values) System.out.println(value.get());

            double min = Double.MAX_VALUE;
            for (DoubleWritable value : values) {
                if (value.get() < min) min = value.get();
            }
            System.out.println("########### MIN : " + min);
            return min;
        }
    }

    public static boolean main() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs=FileSystem.get(conf);

        Job job = new Job(conf,"Weather Data Processing Program");
        job.setJarByClass(MapReduceProcess.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputPath = new Path(outputDir);
        Path inputPath = new Path(inputFile);

        //Configuring the input/output path from the filesystem into the job
        FileInputFormat.addInputPath(job, inputPath);
        if(fs.isDirectory(outputPath)) fs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }
}
