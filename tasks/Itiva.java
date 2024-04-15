/**
 * Counts the frequency of words across a collection of files.
 * Each file is assumed to contain one word per line.
 */
public class WordsCounter {

    private Collection<File> files;

    public WordsCounter(Collection<File> files) {
        this.files = files;
    }

    /**
     * Gets the frequency map of words across the specified files.
     * <p>
     * Example output: {"cat" -> 5, "door" -> 2, "star" -> 3}
     */
    public Map<String, Integer> getWordsFrequencyMap() throws Exception {
        Map<String, Integer> result = new HashMap<>();
        //A separate thread to process each file concurrently
        ExecutorService executor = Executors.newFixedThreadPool(files.size());
        CountDownLatch latch = new CountDownLatch(files.size());
        for (File file : files) {
            executor.submit(() -> {
                handleFile(file, result);
                latch.countDown();
            });
        }
        latch.await(); //Wait until all the files are processed
        return result;
    }

    /**
     * Files are assumed to contain a single word in each line
     */
    private void handleFile(File file, Map<String, Integer> resultMap) {
        try {
            FileReader reader = new FileReader(file);
            int c;
            String str = "";
            while ((c = reader.read()) != -1) {
                char ch = (char) c;
                if (ch != '\n') { //Accumulating symbols while not the end of line
                    str = str + ch;
                } else { //End of line, process the accumulated word
                    Integer count = resultMap.get(str);
                    resultMap.put(str, count == null ? 1 : count + 1);
                    str = "";
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

/**
 * Find Duplicate
 * <p>
 * There is an integer array that consists of N elements. Each number from 1 to N-1 is present
 * in the array exactly once except for some number (let's call it K) which is present in the array
 * twice. There are no guarantees for the order of numbers within array.
 * <p>
 * Objective
 * Your task is to find what K is (the value, not the index)
 * <p>
 * Method signature
 */
public class DuplicateFinder {
    public int findDuplicate(int[] arr);

    HashMap storage = new HashMap<>();
        for(
    int i = 0;
    i<arr.length;i++)

    {
        int el = Array.get(arr, i);
        if (storage.get(el)) {
            return el;
        }
        storage.put(el, 1);
    }
}