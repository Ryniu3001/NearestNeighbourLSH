import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by Marcin on 10.01.2017.
 */
public class App {

    private static final int p = 1000003;
    private static final int b = 12; //na ile czesci dzielimy wektory haashy
    private static final int r = 2; //długosc kazdego podzbioru
    private static long M = 1000000; //max id piosenki

    private static final List<Integer> aList = new ArrayList<>();
    private static final List<Integer> bList = new ArrayList<>();

    private static final String OUTPUT_FILE = "output.txt";
    private static final String USERS_BUCKETS_FILE = "usersBuckets.txt";

    static {
        IntStream.range(0, b*r).boxed().forEach(integer -> {
            aList.add(ThreadLocalRandom.current().nextInt(1, p));
            bList.add(ThreadLocalRandom.current().nextInt(0, p));
        });
    }

    public static void main(String[] args){
        List<Integer> times = new ArrayList<>(3);
        Map<Integer, Set<Integer>> users = readData();
        System.out.println("Users count: " + users.size());

        long start = System.currentTimeMillis();
        System.out.println("Generuje minhashe");
        Map<Integer, List<Integer>> userSignatures = new HashMap<>();
        users.keySet().parallelStream().forEach(key -> {
            List<Integer> minHash = getMinHash(users.get(key));
            synchronized (App.class) {
                userSignatures.put(key, minHash);
            }
        });
        long stop = System.currentTimeMillis();
        System.out.println("Time [s]" + (stop - start) * 1e-3);
        times.add((int)((stop-start) * 1e-3));
        users.clear();


        System.out.println("Przydzielam do bucketow i zapisuje do pliku user -> lista bucketow");
        start = System.currentTimeMillis();
        final Map<Integer, List<Integer>> buckets = new HashMap<>();
        Map<Integer, List<Integer>> usersBuckets = new HashMap<>();
        final int[] counter = {0};
        userSignatures.keySet().forEach(userId -> {
            usersBuckets.putIfAbsent(userId, new ArrayList<>());
            for (int i = 0; i < b ; i++) {
                Integer bucketKey = userSignatures.get(userId).subList(i*r, ((i+1)*r)).toString().concat("b" + i).hashCode();
                // zapisanie do bucketu
                synchronized (App.class) {
                    buckets.putIfAbsent(bucketKey, new ArrayList<>());
                    buckets.get(bucketKey).add(userId);
                    usersBuckets.get(userId).add(bucketKey);
                }
            }
            counter[0]++;
            if (counter[0] % 10000 == 0){
                saveUsersBucketsToFile(usersBuckets);
                usersBuckets.clear();
            }
        });

        saveUsersBucketsToFile(usersBuckets);
        usersBuckets.clear();

        stop = System.currentTimeMillis();
        System.out.println("Time [s]" + (stop - start) * 1e-3);
        times.add((int)((stop-start) * 1e-3));

        System.out.println("Bucketów: " + buckets.size());
        System.out.println("Liczba elementow: " + buckets.entrySet().stream().mapToInt(value -> value.getValue().size()).sum());
        Map<Integer, Map<Integer, Double>> similarities = new HashMap<>();
        start = System.currentTimeMillis();
        buckets.entrySet().removeIf(stringListEntry ->  stringListEntry.getValue().size() < 2);

        System.out.println("Bucketów po usunieciu pustych: " + buckets.size());

        Path path = Paths.get(USERS_BUCKETS_FILE);
        int counter2 = 0;
        int userCount = 0;
        try (BufferedReader reader = Files.newBufferedReader(path, Charset.defaultCharset()))
        {
            String value ;
            Integer bucketId;
            while ((value = reader.readLine()) != null) {
                Integer userId = Integer.valueOf(value);
                boolean foundNeighbour = false;

                while (!(value = reader.readLine()).equals("|")) {
                    bucketId = Integer.valueOf(value);
                    if (buckets.containsKey(bucketId)) {
                        similarities = calculateJaccard(similarities, buckets, userSignatures, userId, bucketId);
                        foundNeighbour = true;
                    }
                }
                if (foundNeighbour)
                    userCount++;
                counter2++;
                if (counter2 % 10000 == 0) {
                    saveToFile(similarities);
                    similarities.clear();
                    System.out.println(counter2);
                }
            }

            saveToFile(similarities);
            similarities.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }

        stop = System.currentTimeMillis();
        System.out.println("Time [s]: " + (stop - start) * 1e-3);

        times.add((int)((stop-start) * 1e-3));
        System.out.println("Total time [s]: " + times.stream().mapToInt(value -> value.intValue()).reduce(0, (int1, int2) -> int1+int2));
        System.out.println("Użytkowników dla których znaleziono sasiadow: " + userCount);
    }

    private static Map<Integer, Map<Integer, Double>> calculateJaccard(final Map<Integer, Map<Integer, Double>> similarities, final Map<Integer, List<Integer>> buckets,
                                                                       Map<Integer, List<Integer>> userSignatures, Integer userId, Integer bucketId){

        if (!similarities.containsKey(userId)) {
            similarities.putIfAbsent(userId, new HashMap<>());
            similarities.get(userId).put(userId, 1.0);
        }
        for (int i = 0; i < buckets.get(bucketId).size(); i++) {
            if (similarities.get(userId).size() > 99)
                break;
            Integer user2Id = buckets.get(bucketId).get(i);

            if (similarities.get(userId).containsKey(user2Id)) //
                continue;
            if (similarities.containsKey(user2Id) && similarities.get(user2Id).containsKey(userId)) {
                similarities.get(userId).put(user2Id, similarities.get(user2Id).get(userId));
                continue;
            }
            if (user2Id == userId)
                continue;
            similarities.get(userId).put(user2Id, jaccard(userSignatures.get(userId), userSignatures.get(user2Id)));
        }
        return similarities;
    }

    private static List<Integer> getMinHash(Set<Integer> songs){
        List<Integer> minHashes = new ArrayList<>(b*r);
        IntStream.range(0, b*r).boxed()
                .forEach(hashId -> minHashes.add(songs.stream()
                                                .map(songId -> getHash(hashId, songId))
                                                .min(Integer::compareTo).get()));
        return minHashes;
    }


    /**
     * @param i indeks funkcji hashujacej
     * @param x zmienna
     * @return
     */
    private static int getHash(int i, int x){
        long a = aList.get(i);
        long bb = aList.get(i);
        long m = M + 1;
        return (int)((((a * x) + bb) % p) % m);
    }

    private static int fileIndex = 1;
    private static void saveToFile(Map<Integer, Map<Integer, Double>> sims) throws IOException {
        Path path = Paths.get(OUTPUT_FILE);

        if (path.toFile().length() / 1024 / 1024 > 150)
            path.toFile().renameTo(new File("output" + fileIndex++ + ".txt"));



        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.APPEND, StandardOpenOption.CREATE))
        {
            sims.keySet().stream().forEach(key -> {
                try {
                    String user = key.toString();
                    Map<Integer, Double> map = sims.get(key);
                    String values = sims.get(key).entrySet()
                                                .stream()
                                                .sorted((o1, o2) -> (int)((o2.getValue()*100) - (o1.getValue()*100)))
                                                .map(Object::toString)
                                                .collect(Collectors.joining("\n"));
//                            .toString().replace(',', '\n');
                    writer.write("User: " + user + "\n" + values + "\n\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static void saveUsersBucketsToFile( Map<Integer, List<Integer>> usersBuckets) {
        Path path = Paths.get(USERS_BUCKETS_FILE);

        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(path, Charset.defaultCharset(), StandardOpenOption.APPEND, StandardOpenOption.CREATE)))
        {
            usersBuckets.keySet().stream().forEach(key -> {
                    String user = key.toString();
                    String values = usersBuckets.get(key).stream().map(Object::toString).collect(Collectors.joining("\n"));
                    writer.write(user + "\n" + values + "\n|\n");
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Map<Integer, Set<Integer>> readData(){
        Map<Integer, Set<Integer>> users = new HashMap<>();

        long start = System.currentTimeMillis();
        String fileName = "facts.csv";

        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            stream.skip(1).forEach(s -> {
                StringTokenizer st = new StringTokenizer(s, ",");
                Integer songId = Integer.valueOf((String) st.nextElement());
                Integer userId = Integer.valueOf((String) st.nextElement());
                if (users.get(userId) == null)
                    users.put(userId, new HashSet<>());
                users.get(userId).add(songId);
                M = Math.max(M, songId);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        long stop = System.currentTimeMillis();
        System.out.println("Time [s]" + (stop - start) * 1e-3);
        System.out.println("M = " + M);
        return users;
    }


    public static double jaccard(Collection<Integer> a, Collection<Integer> b) {
        Set<Integer> intersection = new HashSet<Integer>(a);
        intersection.retainAll(b);
        Double result = (intersection.size() / (a.size() + b.size() - intersection.size() * 1.0));
        return Math.round(result * 100.0) / 100.0;
    }
}
