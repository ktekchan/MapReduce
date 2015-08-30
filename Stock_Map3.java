/*
 * Khushboo Tekchandani
 * Map reduce task 3
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Stock_Map3 {

   public static class Mapper3 extends Mapper<Object, Text, Text, Text>{
      private Text key3 = new Text();
      private Text value3 = new Text();

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

         //Read a line
         String line = value.toString();				

         //Split
         String columns[] = null;
         columns = line.split("\t");
         System.out.println("Columns[]:" + columns[0]+" "+columns[1]);

         key3.set("Final Key");
         value3.set((columns[1]+"/"+columns[0]));

         context.write(key3, value3);
      }
   }

   public static class Reducer3 extends Reducer<Text, Text, Text, Text>{

      private Text keyFinal = null;
      private Text valToWrite3 = null;
      ArrayList<ArrayList<String>> companyVolatility = new ArrayList<ArrayList<String>>();
      ArrayList<ArrayList<Double>> Vol = new ArrayList<ArrayList<Double>>();

      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

         String companyVal [] = null;
         int companyCount = 0;

         HashMap<Double, String> Names = new HashMap<Double, String>();

         try{
            for(Text value: values){

               String valRead = new String();
               valRead = value.toString();

               companyVal = valRead.split("/");

               ArrayList<Double> temp1 = new ArrayList<Double>();
               temp1.add(0, Double.parseDouble(companyVal[0]));
               temp1.add(1, (double)companyCount);

               Names.put((double)companyCount, companyVal[1]);

               Vol.add(temp1);
               companyCount++;

            }

         }
         catch(ArrayIndexOutOfBoundsException e){
            System.out.println("Print stack trace in Stock_Map3!");
            e.printStackTrace();
         }

         try{

            Collections.sort(Vol, new Comparator<ArrayList<Double>>() {    
               @Override
               public int compare(ArrayList<Double> o1, ArrayList<Double> o2) {	        					
                  return o1.get(0).compareTo(o2.get(0));
               }               
            });
         }catch(IndexOutOfBoundsException e){
            System.out.println("Reduce 3!!");
            e.printStackTrace();
         }

         int i = 0;
         int count = 0 ;
         while(count != 10){
            if(Vol.get(i).get(0) == 0.0){
               i++;
               continue;
            }
            else{
               valToWrite3 = new Text(String.valueOf(Vol.get(i).get(0)));
               keyFinal = new Text (String.valueOf(Names.get(Vol.get(i).get(1))));
               context.write(keyFinal, valToWrite3);
               count++;
               i++;
            }
         }

         i = 1;
         count = 0;
         while(count != 10){
            if(Vol.get(companyCount-i).get(0) == 0.0 || (Vol.get(companyCount-i).get(0)).isNaN()){
               i++;
               continue;
            }
            else{
               valToWrite3 = new Text(String.valueOf(Vol.get(companyCount-i).get(0)));
               keyFinal = new Text (String.valueOf(Names.get(Vol.get(companyCount-i).get(1))));
               context.write(keyFinal, valToWrite3);
               i++;
               count++;
            }
         }

         companyVolatility.clear();
         Vol.clear();
      }
   }
}

