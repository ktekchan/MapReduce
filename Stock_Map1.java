/*
 * Khushboo Tekchandani
 * Map reduce task 1
 */
import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Stock_Map1{

   public static class Mapper1 extends Mapper<Object, Text, Text, Text>{
      private Text key1 = new Text();
      private Text value1 = new Text();
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

         int ignore_first = 0;
         long length;

         //to make a note of the filename
         FileSplit fileSplit = (FileSplit)context.getInputSplit();
         String filename = (fileSplit.getPath().getName());

         length = fileSplit.getLength();
         if (length == 0){}
         else{
            //Read a line
            String line = value.toString();				

            //Parse the csv file one line at a time
            String columns[] = null;
            columns = line.split(",");				
            if (columns[0].equals("Date")){

            }

            else{

               //splits the date by '-'
               String date[] = null;
               date = columns[0].split("-");				

               //stores filename appended with month and year
               key1.set(filename+"/"+date[1]+date[0]);

               //stores adjusted closed price appended with the date
               value1.set(columns[6]+"/"+date[2]);

               context.write(key1, value1);
            }

         }

      }	
   }


   public static class Reducer1 extends Reducer<Text, Text, Text, Text>{

      private Text valToWrite = null;
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

         ArrayList<ArrayList<String>> valAndDate = new ArrayList<ArrayList<String>>();
         String valComponents[] = null;
         int indexCount = 0;

         for(Text value: values){

            //read the value and remove date from it
            String valClose = new String();
            valClose = value.toString();

            valComponents = valClose.split("/");

            //2d Arraylist with 1st column having the date(day) and 2nd column having the closing value

            try{
               ArrayList<String> temp = new ArrayList<String>();
               temp.add(0, valComponents[1]);
               temp.add(1, valComponents[0]);
               valAndDate.add(temp);
               indexCount++;
            }
            catch(ArrayIndexOutOfBoundsException e){
               System.out.println("Print Stack trace!!");
               e.printStackTrace();
            }
            catch(IndexOutOfBoundsException e){
               System.out.println("Print Index Stack trace!!");
               e.printStackTrace();
            }

         }

         //Sort the arraylist according to the date
         Collections.sort(valAndDate, new Comparator<ArrayList<String>>() {    
            @Override
            public int compare(ArrayList<String> o1, ArrayList<String> o2) {
               return o1.get(0).compareTo(o2.get(0));
            }               
         });


         double val1;
         double val2;
         double Xi;

         //Calculate xi
         val1 = Double.parseDouble(valAndDate.get(0).get(1)); 
         val2 = Double.parseDouble(valAndDate.get(indexCount-1).get(1));
         Xi = (val2-val1)/val1;
         valToWrite = new Text(String.valueOf(Xi));

         //Clear Arraylist
         valAndDate.clear();

         //write key and xi
         context.write(key, valToWrite); 

      }
   }
}


