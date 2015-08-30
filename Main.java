/*
 * Khushboo Tekchandani
 * Stock Volatility using Map reduce
 * Driver function
 */
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Main {

   public static void main(String[] args) throws Exception {		
      long start = new Date().getTime();		
      Configuration conf = new Configuration();

      Job job = Job.getInstance();
      job.setJarByClass(Stock_Map1.class);
      Job job2 = Job.getInstance();
      job2.setJarByClass(Stock_Map2.class);
      Job job3 = Job.getInstance();
      job3.setJarByClass(Stock_Map3.class);


      System.out.println("\n---------------------Stock Volatility Begins---------------------\n");

      job.setJarByClass(Stock_Map1.class);
      job.setMapperClass(Stock_Map1.Mapper1.class);
      job.setReducerClass(Stock_Map1.Reducer1.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);

      job2.setJarByClass(Stock_Map2.class);
      job2.setMapperClass(Stock_Map2.Mapper2.class);
      job2.setReducerClass(Stock_Map2.Reducer2.class);

      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);

      job3.setJarByClass(Stock_Map3.class);
      job3.setMapperClass(Stock_Map3.Mapper3.class);
      job3.setReducerClass(Stock_Map3.Reducer3.class);

      job3.setMapOutputKeyClass(Text.class);
      job3.setMapOutputValueClass(Text.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]+"Inter_1"));
      FileInputFormat.addInputPath(job2, new Path(args[1]+"Inter_1"));

      FileOutputFormat.setOutputPath(job2, new Path(args[1]+"Inter_2"));
      FileInputFormat.addInputPath(job3, new Path(args[1]+"Inter_2"));
      FileOutputFormat.setOutputPath(job3, new Path(args[1]+"output"));


      job.waitForCompletion(true);
      job2.waitForCompletion(true);
      boolean status1 = job3.waitForCompletion(true);
      if (status1 == true) {
         long end = new Date().getTime();
         System.out.println("\nJob took " + (end - start) + "milliseconds\n");
         System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
      }	
      System.out.println("Status: "+status1);
      System.out.println("\n---------------------Stock Volatility Map-Reduce Ends---------------------\n");
   }
}

