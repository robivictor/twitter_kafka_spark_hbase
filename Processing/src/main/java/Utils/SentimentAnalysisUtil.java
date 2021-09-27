package Utils;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class SentimentAnalysisUtil implements Serializable {

    public static Set<String> loadFile(String pathToFile){
        Set<String> fileContent = new HashSet<>();
        String line = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(pathToFile));
            while ((line = reader.readLine()) != null){
                fileContent.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
        finally {
            return fileContent;
        }
    }

    public static Integer computeScore(List<String> words, Set<String> positiveWords, Set<String> negativeWords){
        return words.stream().map(word -> computeWordScore(word, positiveWords, negativeWords)).reduce(0, Integer::sum);
    }

    public static Integer computeScoreAlternative(List<String> words){
        words.forEach(System.out::println);
        Random rand = new Random();
        return rand.nextInt(25);
    }

    public static Integer computeWordScore(String word, Set<String> positiveWords, Set<String> negativeWords){
        if(positiveWords.contains(word))
            return 1;
        else if(negativeWords.contains(word))
            return -1;
        else
            return 0;
    }
}

