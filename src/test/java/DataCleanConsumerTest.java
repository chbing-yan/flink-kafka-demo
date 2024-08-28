import org.junit.Test;

import java.io.IOException;
import java.util.regex.Pattern;

public class DataCleanConsumerTest {
    @Test
    public void testClean() throws IOException, InterruptedException {
        String input=DataCleanProducer.generateString();
        String cleanedString = DataCleanConsumer.clean(input);
        System.out.println(cleanedString);
    }
    @Test
    public void testRegex(){
        String time1 = "2024年5月8日 12时12分12秒12233";
        String time2 = "2024-5-8 12-12-12 12345";
        String time3 = "2024 5 8 12 12 12 000";
        String[] times=time1.split("\\D");

        String pattern = "\\d{4}\\D{1,2}\\d{1,2}\\D{1,2}\\d{1,2}\\D{1,2}\\d{1,2}\\D{1,2}\\d{1,2}\\D{1,2}\\d{1,2}(\\D{1,2}\\d{1,6}){0,1}";

        boolean isMatch = Pattern.matches(pattern, time1);
        boolean isMatch2 = Pattern.matches(pattern, time2);
        boolean isMatch3 = Pattern.matches(pattern, time3);
//        boolean isMatch4 = Pattern.matches(pattern, time4);
        return;

    }
}
