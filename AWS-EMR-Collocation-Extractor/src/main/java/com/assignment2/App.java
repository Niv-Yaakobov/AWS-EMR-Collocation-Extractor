package com.assignment2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class App {
    public static void main(String[] args) throws Exception {
        // This Main class is primarily for local testing or chaining if you use a custom runner.
        // However, on EMR, you usually define "Steps" where each step calls the 'main' of a specific class.
        
        // If you want to run the whole flow locally or via a single jar execution:
        if (args.length < 5) {
            System.out.println("Usage: App <1gram-input> <2gram-input> <output-base> <language> <run-step>");
            System.out.println("Example: App s3://.../1gram s3://.../2gram s3://.../output eng all");
            return;
        }

        String input1Gram = args[0];
        String input2Gram = args[1];
        String outputBase = args[2];
        String language = args[3]; // 'eng' or 'heb' (useful for selecting paths if hardcoded)
        
        // Define intermediate paths
        String step1Out = outputBase + "/step1_N";
        String step2Out = outputBase + "/step2_joinC1";
        String step3Out = outputBase + "/step3_joinC2";
        String step4Out = outputBase + "/step4_final";

        // Step 1: Calculate N
        System.out.println("Running Step 1...");
        Step1_CalcN.main(new String[]{input1Gram, step1Out});

        // Step 2: Join C1
        System.out.println("Running Step 2...");
        Step2_JoinC1.main(new String[]{input1Gram, input2Gram, step2Out});

        // Step 3: Join C2 and Calc LLR
        System.out.println("Running Step 3...");
        Step3_JoinC2AndCalcLLR.main(new String[]{step2Out, input1Gram, step1Out, step3Out});

        // Step 4: Sort
        System.out.println("Running Step 4...");
        Step4_SortAndFilter.main(new String[]{step3Out, step4Out});
        
        System.out.println("All steps completed. Final output in: " + step4Out);
    }
}