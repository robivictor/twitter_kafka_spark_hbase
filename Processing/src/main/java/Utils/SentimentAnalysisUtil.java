package Utils;

import org.apache.phoenix.util.PropertiesUtil;
import org.apache.spark.broadcast.Broadcast;
import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SentimentAnalysisUtil implements Serializable {

    //private static Set<String> uselessWords;
    private static Set<String> positiveWords;
    private static Set<String> negativeWords;
    private static ClassLoader classLoader;

    public SentimentAnalysisUtil() {
        classLoader = PropertiesUtil.class.getClassLoader();
       // uselessWords = loadFile(classLoader.getResource("stop-words.dat").getPath());
        positiveWords = loadFile(classLoader.getResource("pos-words.dat").getPath());
        negativeWords = loadFile(classLoader.getResource("neg-words.dat").getPath());
    }

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

    public static Integer computeScore(List<String> words){
        return words.stream().map(word -> computeWordScore(word)).reduce(0, Integer::sum);
    }

    public static Integer computeWordScore(String word){
        if(positiveWords.contains(word))
            return 1;
        else if(negativeWords.contains(word))
            return -1;
        else
            return 0;
    }
}

