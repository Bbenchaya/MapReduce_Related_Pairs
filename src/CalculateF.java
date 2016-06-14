import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;
import java.util.*;

/**
 * Created by asafchelouche on 13/6/16.
 */
public class CalculateF {

    private static final String INPUT_FILE_NEG_LIST = "/Users/asafchelouche/programming/dsp2/resource/wordsim-neg.txt";
    private static final String INPUT_FILE_POS_LIST = "/Users/asafchelouche/programming/dsp2/resource/wordsim-pos.txt";
    private static final String INPUT_FILE_FROM_MAPREDUCE = "part-r-00010";

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
        File file1 = new File(INPUT_FILE_NEG_LIST);
        File file2 = new File(INPUT_FILE_POS_LIST);
        Scanner sc1 = new Scanner(file1);
        Scanner sc2 = new Scanner(file2);
        processFile(negPairs, sc1);
        processFile(posPairs, sc2);
        sc1.close();
        sc2.close();

        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);

        System.out.print("Downloading corpus description file from S3... ");
        S3Object object = s3.getObject(new GetObjectRequest("dsps162assignment2benasaf/output4/", INPUT_FILE_FROM_MAPREDUCE));
        System.out.println("Done.");
        Scanner sc = new Scanner(new InputStreamReader(object.getObjectContent()));
        double precision = 0;
        double recall = 0;
        double fmeasure;
        double tp = 0;
        double fp = 0;
        double tn = 0;
        double fn = 0;
        while (sc.hasNextLine()) {
            String[] components = sc.nextLine().split("\t");
            String[] pair = components[0].split("[$]");
            double PMI = Double.parseDouble(components[1]);
            if (PMI < threshold) {
                if (negPairs.get(pair[0]) == null) {
                    fn++;
                } else if (negPairs.get(pair[0]).contains(pair[1])) {
                        tn++;
                } else if (negPairs.get(pair[1]) != null && negPairs.get(pair[1]).contains(pair[0]))
                        tn++;
                else fn++;
            } else {
                if (posPairs.get(pair[0]) == null) {
                    fp++;
                } else if (posPairs.get(pair[0]).contains(pair[1])) {
                    tp++;
                } else if (posPairs.get(pair[1]) != null && posPairs.get(pair[1]).contains(pair[0]))
                    tp++;
                else fp++;
            }
        }
        sc.close();
        if (tp + fn == 0l || tp + fp == 0l) {
            System.err.println("Not enough data");
            System.exit(1);
        }
        precision = tp / (tp + fp);
        recall = tp / (tp + fn);
        fmeasure = 2 * ((precision * recall) / (precision + recall));
        System.out.println("Fmeasure: " + fmeasure);
    }

    private static void processFile(HashMap<String, HashSet<String>> pairs, Scanner sc) {
        HashSet<String> temp;
        while (sc.hasNextLine()) {
            String[] components = sc.nextLine().toLowerCase().split("\t");
            if ((temp = pairs.putIfAbsent(components[0], new HashSet())) == null) {
                pairs.get(components[0]).add(components[1]);
            } else {
                temp.add(components[1]);
            }
        }
    }

}
