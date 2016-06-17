/**
 * Created by asafchelouche on 13/6/16.
 */

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

public class CalculateF {

    private static final String INPUT_FILE_NEG_LIST = "/Users/asafchelouche/programming/dsp2/resource/wordsim-neg.txt";
    private static final String INPUT_FILE_POS_LIST = "/Users/asafchelouche/programming/dsp2/resource/wordsim-pos.txt";
    private static final String BUCKET = "dsps162assignment2benasaf/results/";
    private static final String INPUT_FILE_FROM_MAPREDUCE = "lastDecadePMIResults.txt";

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: CalculateF <threshold>");
            System.exit(1);
        }
        double threshold = Double.parseDouble(args[0]);
        if (threshold <= 0) {
            System.err.println("Error: threshold should be positive");
            System.exit(1);
        }

        HashMap<String, HashSet<String >> negPairs = new HashMap<>();
        HashMap<String, HashSet<String >> posPairs = new HashMap<>();
        processFile(negPairs, INPUT_FILE_NEG_LIST);
        processFile(posPairs, INPUT_FILE_POS_LIST);
        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        System.out.print("Downloading corpus description file from S3... ");
        S3Object object = s3.getObject(new GetObjectRequest(BUCKET, INPUT_FILE_FROM_MAPREDUCE));
        System.out.println("Done.");

            Scanner scanner = new Scanner(new InputStreamReader(object.getObjectContent()));
            double precision = 0;
            double recall = 0;
            double fMeasure;
            long tp = 0;
            long fp = 0;
            long tn = 0;
            long fn = 0;
            int tenthousand = 0;
            long counter = 1;
            while (scanner.hasNextLine()) {
                String[] components = scanner.nextLine().split("\t");
                String[] text = components[0].split("[$]");
                double PMI = Double.parseDouble(components[1]);
                if (PMI < threshold) {
                    // NOTE: the pair's components are always sorted lexicographically
                    if (negPairs.containsKey(text[1]) && negPairs.get(text[1]).contains(text[2]))
                        tn++;
                    else if (posPairs.containsKey(text[1]) && posPairs.get(text[1]).contains(text[2]))
                        fn++;
                } else {
                    if (posPairs.containsKey(text[1]) && posPairs.get(text[1]).contains(text[2]))
                        tp++;
                    else if (negPairs.containsKey(text[1]) && negPairs.get(text[1]).contains(text[2]))
                        fp++;
                }
                if (tenthousand++ == 10000) {
                    tenthousand = 0;
                    System.out.print("\rProcessed " + (counter++ * 10000l) + " from all key-value pairs in the last decade.");
                }
            }
            scanner.close();
            if (tp + fn == 0l || tp + fp == 0l) {
                System.err.println("Not enough data");
                System.exit(1);
            }
            precision = (double) tp / (tp + fp);
            recall = (double) tp / (tp + fn);
            fMeasure = 2 * ((precision * recall) / (precision + recall));
        System.out.format("\nPrecision: %f\nRecall: %f\nFmeasure: %f\nNo. of true positives: %d\nNo. of false positives: %d\nNo. of true negatives: %d\nNo. of false negatives: %d\n", precision, recall, fMeasure, tp, fp, tn, fn);
    }

    private static void processFile(HashMap<String, HashSet<String>> pairs, String fileName) throws FileNotFoundException {
        HashSet<String> value;
        File file = new File(fileName);
        Scanner scanner = new Scanner(file);
        // index enforces lexicographic order between the words in a pair
        int index = 0;
        while (scanner.hasNextLine()) {
            String[] components = scanner.nextLine().toLowerCase().split("\t");
            if (components[0].compareTo(components[1]) > 0)
                index = 1;
            value = pairs.get(components[index]);
            if (value == null) {
                pairs.put(components[index], new HashSet<String>());
                pairs.get(components[index]).add(components[1 - index]);
            } else {
                value.add(components[1 - index]);
            }
        }
        scanner.close();
    }

}
