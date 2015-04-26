package Util;

import Writables.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by ydubale on 4/6/15.
 */
public class Util {

    public static final String JOB_TYPE = "JobType";
    public static final int RECORD_PART_NUMBER_START = 24;
    public static final int RECODR_PART_NUMBER_END = 28;

    public static boolean correctSegment(String line, int segment) {
        try{
            return Integer.parseInt(line.substring(RECORD_PART_NUMBER_START, RECODR_PART_NUMBER_END)) == segment;
        }
        catch (NumberFormatException nfe){
            return false;
        }
    }

    public static List<IntWritable> convertStringsToIntWritables(String... strings){
        List<IntWritable> nums = new LinkedList<>();

        for(String s : strings){
            nums.add(new IntWritable(Integer.parseInt(s)));
        }

        return nums;
    }

    public static int getSumRange(String line, int start, int end){
        int sum = 0;
        for(int i= start; i <= end; i+=9){
            sum += Integer.parseInt(line.substring(i, i+9));
        }
        return sum;
    }

    /**
     * Writes -1 in place of values for map output
     * @param numOfFields
     * @return list of -1s with size numOfFields
     */
    public static List<IntWritable> getPlaceHolder(int numOfFields){
        List<IntWritable> list = new LinkedList<>();
        for(int i=0; i < numOfFields; i++){
            list.add(new IntWritable(-1));
        }
        return list;
    }

    public static int sumFields(int[] fields, int offsetStart, int offsetEnd){
        int sum = 0;

        for(int i=offsetStart; i <= offsetEnd; i++){
            if(fields[i] != -1){
                sum += fields[i];
            }
        }
        return sum;
    }

    public static int generalMedianReducer(List<IntArrayWritable> value, int fieldOffset, int numFields) {
        int[] values = new int[numFields];

        for(IntArrayWritable field : value){
            int[] nums = field.get();

            for(int i=0; i < numFields; i++){
                values[i] += Util.sumFields(nums, fieldOffset + i, fieldOffset + i);
            }
        }

        long sum = 0;
        for(int val : values){
            sum += val;
        }

        long midVal = sum/2;

        long newSum = 0;

        for(int i=0; i < values.length; i++){
            if(newSum > midVal){
                return i;
            }
            newSum += values[i];
        }

        return -1;
    }

}
