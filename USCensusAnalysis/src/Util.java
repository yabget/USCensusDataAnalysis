/**
 * Created by ydubale on 4/6/15.
 */
public class Util {

    public static int[] convertStringsToInts(String... strings){
        int[] nums = new int[strings.length];

        int count = 0;
        for(String s : strings){
            nums[count] = Integer.parseInt(s);
            count++;
        }

        return nums;
    }

    public static void printValidCommands(){
        System.out.println("Valid commands are: ");
        System.out.println("\t\tNM - Never Married");
        System.out.println("\t\tRvO - Rent vs Owned");
        System.out.println("\t\tGAD - Gender Age Distribution");
    }

}
