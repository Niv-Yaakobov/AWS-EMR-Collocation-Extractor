package com.assignment2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StopWordsUtility {
    
    private Set<String> stopWords = new HashSet<>();

    // Load stop words from local resources (for local testing)
    public StopWordsUtility() {
        loadFromResource("eng-stopwords.txt");
        loadFromResource("heb-stopwords.txt");
    }

    private void loadFromResource(String fileName) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(getClass().getClassLoader().getResourceAsStream(fileName)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                stopWords.add(line.trim());
            }
        } catch (Exception e) {
            System.err.println("Error loading stop words: " + e.getMessage());
        }
    }

    public boolean isStopWord(String word) {
        return stopWords.contains(word);
    }
}