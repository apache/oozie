/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package org.apache.oozie.service;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.DataOutputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.DataInputStream;
import java.io.DataInput;


public class MaprSubmitJob {

    static void usage() {
        System.out.println("Args: <jobconf input file> <jobid output file>");
    }
    public static void main(String[] args) throws Exception {

        System.out.println("Starting MaprJobSubmit");
        if (args.length != 2) {
            usage();
            System.exit(1);
        }
        try {
            DataInputStream inputFile = new DataInputStream(
                (InputStream) new FileInputStream(args[0]));
            JobConf conf = new JobConf();
            conf.readFields((DataInput)inputFile);
            JobClient jc = new JobClient(conf);
            RunningJob runJob = jc.submitJob(conf);
            JobID jobId = runJob.getID();
            DataOutputStream outputFile = new DataOutputStream(
                (OutputStream)new FileOutputStream(args[1]));
            jobId.write((DataOutput)outputFile);
            outputFile.flush();
            outputFile.close();
            System.out.println("MaprJobSubmit is successful");
        } catch (Exception e) {
            System.out.println("Exception occured in MaprJobSubmit" + e.toString());
            System.exit(1);
        }

        System.exit(0);
    }
}