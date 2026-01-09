package com.assignment2;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class EmrRunner {

    public static void main(String[] args) {
        
        // --- 1. CONFIGURATION (UPDATE THESE) ---
        
        // YOUR BUCKET NAME (Created in Step 3)
        String bucketName = "dist-sys-romniv-assign2"; 
        
        // YOUR KEY PAIR NAME (Found in Step 4A - usually "vockey")
        String keyPairName = "vockey"; 
        
        // YOUR IAM ROLES (Found in Step 4B)
        // If you saw "EMR_DefaultRole" in IAM, leave these as is.
        // If you ONLY saw "LabRole", change BOTH to "LabRole".
        String serviceRole = "EMR_DefaultRole"; 
        String jobRole     = "EMR_EC2_DefaultRole";
        
        // ----------------------------------------

        String bucketPath = "s3n://" + bucketName;
        String logUri     = bucketPath + "/logs/";
        String jarPath    = bucketPath + "/collocation-extraction-1.0-SNAPSHOT.jar";
        
        
        String eng1Gram = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/1gram/data";
        String eng2Gram = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data";
        
        // Unique output folder so you don't overwrite previous runs
        String outputBase = bucketPath + "/output/run_" + System.currentTimeMillis(); 

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion("us-east-1")
                .build();

        // Step 1: Calculate N
        StepConfig step1 = new StepConfig()
            .withName("Step1_CalcN")
            .withActionOnFailure("TERMINATE_JOB_FLOW")
            .withHadoopJarStep(new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("com.assignment2.Step1_CalcN")
                .withArgs(eng1Gram, outputBase + "/step1_N"));

        // Step 2: Join C1
        StepConfig step2 = new StepConfig()
            .withName("Step2_JoinC1")
            .withActionOnFailure("TERMINATE_JOB_FLOW")
            .withHadoopJarStep(new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("com.assignment2.Step2_JoinC1")
                .withArgs(eng1Gram, eng2Gram, outputBase + "/step2_joinC1"));

        // Step 3: Join C2 & LLR
        StepConfig step3 = new StepConfig()
            .withName("Step3_JoinC2")
            .withActionOnFailure("TERMINATE_JOB_FLOW")
            .withHadoopJarStep(new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("com.assignment2.Step3_JoinC2AndCalcLLR")
                .withArgs(outputBase + "/step2_joinC1", eng1Gram, outputBase + "/step1_N", outputBase + "/step3_joinC2"));

        // Step 4: Sort
        StepConfig step4 = new StepConfig()
            .withName("Step4_Sort")
            .withActionOnFailure("TERMINATE_JOB_FLOW")
            .withHadoopJarStep(new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("com.assignment2.Step4_SortAndFilter")
                .withArgs(outputBase + "/step3_joinC2", outputBase + "/step4_final"));

        // Launch Cluster
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
            .withName("CollocationExtraction_StudentLab")
            .withInstances(new JobFlowInstancesConfig()
                .withInstanceCount(6) // Keep low for student budget
                .withMasterInstanceType("m4.large")
                .withSlaveInstanceType("m4.large")
                .withHadoopVersion("2.6.0")
                .withEc2KeyName(keyPairName)
                .withKeepJobFlowAliveWhenNoSteps(false) // Terminate automatically!
                .withPlacement(new PlacementType("us-east-1a")))
            .withLogUri(logUri)
            .withReleaseLabel("emr-5.20.0")
            .withServiceRole(serviceRole)
            .withJobFlowRole(jobRole)
            .withSteps(step1, step2, step3, step4);

        RunJobFlowResult result = mapReduce.runJobFlow(runFlowRequest);
        System.out.println("Cluster launched! Job ID: " + result.getJobFlowId());
        System.out.println("Go to AWS Console -> EMR to monitor progress.");
    }
}