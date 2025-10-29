package net.michaelkoepf.spegauge.flink.sdk.source.impl;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class SQBenchParallelTCPSourceFull extends SQBenchParallelTCPSource<EntityRecordFull> {

    public SQBenchParallelTCPSourceFull() {
    }

    public static long[] readIntegersFromCSV(String fileName) {
        ArrayList<Long> numberList = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;

            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    numberList.add(Long.parseLong(line.trim()));
                }
            }

        } catch (IOException e) {
            System.err.println("Error reading from CSV file: " + e.getMessage());
        }

        // Convert ArrayList<Integer> to int[]
        long[] numbers = new long[numberList.size()];
        for (int i = 0; i < numberList.size(); i++) {
            numbers[i] = numberList.get(i);
        }

        return numbers;
    }

    protected static EntityRecordFull attributesToEntityRecord(EntityRecordFull.Type type, String[] attrs) {
        return new EntityRecordFull(type, Long.parseLong(attrs[1]), Long.parseLong(attrs[2]), Long.parseLong(attrs[3]),
                Long.parseLong(attrs[4]), Long.parseLong(attrs[5]), Long.parseLong(attrs[6]), Long.parseLong(attrs[7]),
                Long.parseLong(attrs[8]), Long.parseLong(attrs[9]), attrs[10].isEmpty() ? null :
                Integer.parseInt(attrs[10]), attrs[11], attrs[12], attrs[13]);
    }

    protected static EntityRecordFull attributesToEntityRecordA(EntityRecordFull.Type type, String[] attrs, long PK) {
        return new EntityRecordFull(type, Long.parseLong(attrs[1]), Long.parseLong(attrs[2]), Long.parseLong(attrs[3]),
                PK, Long.parseLong(attrs[5]), Long.parseLong(attrs[6]), Long.parseLong(attrs[7]),
                Long.parseLong(attrs[8]), Long.parseLong(attrs[9]), attrs[10].isEmpty() ? null :
                Integer.parseInt(attrs[10]), attrs[11], attrs[12], attrs[13]);
    }

    protected static EntityRecordFull attributesToEntityRecordB(EntityRecordFull.Type type, String[] attrs, long FK) {
        return new EntityRecordFull(type, Long.parseLong(attrs[1]), Long.parseLong(attrs[2]), Long.parseLong(attrs[3]),
                Long.parseLong(attrs[4]), FK, Long.parseLong(attrs[6]), Long.parseLong(attrs[7]),
                Long.parseLong(attrs[8]), Long.parseLong(attrs[9]), attrs[10].isEmpty() ? null :
                Integer.parseInt(attrs[10]), attrs[11], attrs[12], attrs[13]);
    }

    @Override
    protected EntityRecordFull handleEvent(String record) {
        String[] attrs = record.split("\t", -1);
        EntityRecordFull entity;
        if (attrs[0].equals("JSONA")) {
            if (attrs.length != 3) {
                throw new RuntimeException("JSON entities must have length 3. Got " + attrs.length + " instead");
            }

            entity = new EntityRecordFull(EntityRecordFull.Type.JSONA, Long.parseLong(attrs[1]), attrs[2]);
        } else if (attrs[0].equals("JSONB")) {
            if (attrs.length != 3) {
                throw new RuntimeException("JSON entities must have length 3. Got " + attrs.length + " instead");
            }

            entity = new EntityRecordFull(EntityRecordFull.Type.JSONB, Long.parseLong(attrs[1]), attrs[2]);
        }else if (attrs[0].equals("A") || attrs[0].equals("B") || attrs[0].equals("C")) {
            if (attrs.length != 14) { // reduced to 14 because I removed the query set
                throw new RuntimeException("Entities must have length 14. Got " + attrs.length + " instead");
            }

            switch (attrs[0]) {
                case "A":
                    entity = attributesToEntityRecord(EntityRecordFull.Type.A, attrs);
                    /*entity = attributesToEntityRecordA(EntityRecordFull.Type.A, attrs, keysDistributionA[indexA]);
                    indexA++;
                    if (indexA >= keysDistributionA.length) {
                        indexA = 0;
                    }*/

                    break;
                case "B":
                    entity = attributesToEntityRecord(EntityRecordFull.Type.B, attrs);

                    /*entity = attributesToEntityRecordB(EntityRecordFull.Type.B, attrs, keysDistributionB[indexB]);
                    indexB++;
                    if (indexB >= keysDistributionB.length) {
                        indexB = 0;
                    }*/

                    break;
                case "C":
                    entity = attributesToEntityRecord(EntityRecordFull.Type.C, attrs);
                    break;
                default:
                    throw new RuntimeException("Invalid event type " + attrs[0]);
                    // TODO: extend for further entities, if necessary
            }
        } else {
            throw new RuntimeException("Invalid event type " + attrs[0]);
        }

        currentEventId.set(entity.uniqueTupleId);

        return entity;
    }
}
