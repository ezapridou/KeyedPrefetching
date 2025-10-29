package net.michaelkoepf.spegauge.generators.sqbench.datageneration;

import net.michaelkoepf.spegauge.generators.sqbench.common.SQBenchUtils;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileWriterDistr {
    public static void writeFile() {
        System.out.println("Hereee");
        int count = 408000;// 18000;
        int min = 0;
        int max = 10000; // max is exclusive
        int distinctValues = 10000;
        double zipfExp = 1.0;

        long[] PKs = EntityRecordGeneratorUtils.getConsecutiveLongValues(100_000_000L, distinctValues);
        long[] generatedNumbers = new long[count];

        List<Double> parameters = new ArrayList<>(2);
        parameters.add(10000.0);
        parameters.add(zipfExp);
        UniformRandomProvider rng = RandomSource.XO_SHI_RO_256_PP.create();
        EntityRecordGenerator.UnionDistributionSampler distributionSamplerWrapperPK = new EntityRecordGenerator.UnionDistributionSampler(
                SQBenchUtils.Distribution.Type.ZIPF, parameters, rng);

        for (int i = 0; i < count; i++) {
            long PK = PKs[distributionSamplerWrapperPK.sample(PKs.length)];
            generatedNumbers[i] = PK;

        }
        //int[] randomNumbers = generateUniformRandomIntegers(count, min, max);

        String fileName = "./random_numbers_A.csv";
        writeNumbersToCSV(generatedNumbers, fileName);

        count = 72000;// 18000;
        long[] generatedNumbersB = new long[count];


        for (int i = 0; i < count; i++) {
            long PK = PKs[distributionSamplerWrapperPK.sample(PKs.length)];
            generatedNumbersB[i] = PK;

        }
        //int[] randomNumbers = generateUniformRandomIntegers(count, min, max);

        String fileName2 = "./random_numbers_B.csv";
        writeNumbersToCSV(generatedNumbersB, fileName2);
    }

    public static void writeNumbersToCSV(long[] numbers, String fileName) {
        try (FileWriter writer = new FileWriter(fileName)) {
            for (long number : numbers) {
                writer.append(String.valueOf(number));
                writer.append('\n');
            }
            System.out.println("CSV file created successfully: " + fileName);
        } catch (IOException e) {
            System.err.println("Error writing to CSV file: " + e.getMessage());
        }
    }

}
