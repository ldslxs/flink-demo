import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RR {
    @Test
    public void test1() {
        String s="race a _car";

    }

    public boolean isSubsequence(String s, String t) {
        if(Objects.equals(s, ""))return true;
        int count =0 ;
        for (int i = 0; i < t.length(); i++) {
            if(count<s.length()&&t.charAt(i)==s.charAt(count)){
                count++;
            }
            if(count==s.length())return true;
        }return false;
    }

}