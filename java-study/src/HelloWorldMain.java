import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class HelloWorldMain {
    public static void main(String args[]){
        System.out.println("Hello World");

        List<String> words = Arrays.asList("kim", "taeng", "mad", "play");
        System.out.println(words);

        // 익명 클래스 방식
        Collections.sort(words, new Comparator<String>() {
            public int compare(String s1, String s2) {
                return Integer.compare(s1.length(), s2.length());
            }
        });

        // 람다 표현식
        Collections.sort(words,
                (s1, s2) -> Integer.compare(s1.length(), s2.length()));

        // 위에서 제안된 방식
        Collections.sort(words, Comparator.comparingInt(String::length));

        System.out.println(words);
    }
}
