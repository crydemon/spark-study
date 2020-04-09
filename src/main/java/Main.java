import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class Main {

    public static int maxLen(String strs) {

        if(strs == null || strs.isEmpty()) return 0;
        int result = 0;
        String curStr = "";
        int curMax = 0;
        for (int i =0; i < strs.length(); i ++) {
            if(curStr.contains(strs.charAt(i) + "")) {
                result = Math.max(result, curMax);
                int j = curStr.indexOf(strs.charAt(i));
                curStr = curStr.substring(j);
            } else {
                curMax++;
                curStr = curStr + strs.charAt(i);
            }
        }
        return  Math.max(result, curMax);
    }
    public static void main(String[] args) {

        System.out.println(maxLen("bbbb"));
    }
}