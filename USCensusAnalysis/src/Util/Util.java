package Util;

/**
 * Created by ydubale on 4/6/15.
 */
public class Util {

    public static final String JOB_TYPE = "JobType";
    public static final int RECORD_PART_NUMBER_START = 24;
    public static final int RECODR_PART_NUMBER_END = 28;

    public static boolean correctSegment(String line, int segment){
        return Integer.parseInt(line.substring(RECORD_PART_NUMBER_START, RECODR_PART_NUMBER_END)) == segment;
    }

    public static int[] convertStringsToInts(String... strings){
        int[] nums = new int[strings.length];

        int count = 0;
        for(String s : strings){
            nums[count] = Integer.parseInt(s);
            count++;
        }

        return nums;
    }
}
