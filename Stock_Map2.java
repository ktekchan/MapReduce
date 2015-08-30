/*
 * Khushboo Tekchandani
 * Map reduce task 2
 */
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Stock_Map2 {

   public static class Mapper2 extends Mapper<Object, Text, Text, Text>{
      private Text key2 = new Text();
      private Text value2 = new Text();
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

         //Read a line
         String line = value.toString();				

         //Split
         String columns[] = null;
         columns = line.split("\t");
         String companyDate[] = null;
         companyDate = columns[0].split("/");
         key2.set(companyDate[0]);
         value2.set(columns[1]);

         context.write(key2, value2);
      }
   }

   public static class Reducer2 extends Reducer<Text, Text, Text, Text>{

      private Text valToWrite2 = null;
      private int Ncount = 0;
      private double Xsum = 0;
      private double Xbar = 0;
      private double Vol = 0;
      private double interimXiSum = 0;
      ArrayList<Double> Xi = new ArrayList<Double>();
      ArrayList<Double> interimXi = new ArrayList<Double>();
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

         try{
            for(Text value: values){
               Xi.add(Double.parseDouble(value.toString()));
               Ncount++;
            }

            for(Double x : Xi)
               Xsum += x;

            Xbar = Xsum/(Ncount);

            for (Double x : Xi){
               interimXi.add(x-Xbar);
            }	

            for (Double x : interimXi){
               interimXiSum += Math.pow(x, 2);
            }

            Vol = Math.sqrt(((1/(double)(Ncount-1)))*interimXiSum);

            valToWrite2 = new Text(String.valueOf(Vol));
         }
         catch(ArrayIndexOutOfBoundsException e){
            System.out.println("Print stack trace in Stock_Map2!");
            e.printStackTrace();
         }

         context.write(key, valToWrite2);

         Ncount = 0;
         Xsum = 0;
         Xbar = 0;
         Vol = 0;
         interimXiSum = 0;
         Xi.clear();
         interimXi.clear();

      }

   }
}

