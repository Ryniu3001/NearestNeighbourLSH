import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

/**
 * Created by Marcin on 24.01.2017.
 */
public class Test {

    public static void main(String [] args) {
        List<List<Integer>> a = new ArrayList<>();
        List<List<Integer>> b = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            a.add(new ArrayList<>());
            b.add(new ArrayList<>());
            for (int j=0; j < 1000; j++){
                a.get(i).add(ThreadLocalRandom.current().nextInt(1, 1000000));
                b.get(i).add(ThreadLocalRandom.current().nextInt(1, 1000000));
            }
        }
        System.out.println("!!!");
        long start = System.currentTimeMillis();
        IntStream.range(0,10000).boxed().parallel().forEach(integer -> jaccard(a.get(integer), b.get(integer)));
//        for (int i = 0; i < 10000; i++) {
//            jaccard(a.get(i), b.get(i));
//        }
        long stop = System.currentTimeMillis();
        System.out.println("Time [s]: " + (stop - start) * 1e-3);
    }

    public static double jaccard(Collection<Integer> a, Collection<Integer> b) {
        Set<Integer> intersection = new HashSet<Integer>(a);
        intersection.retainAll(b);
        Double result = (intersection.size() / (a.size() + b.size() - intersection.size() * 1.0));
        return Math.round(result * 100.0) / 100.0;
    }
}
