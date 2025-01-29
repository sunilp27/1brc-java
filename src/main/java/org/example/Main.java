package org.example;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

public class Main {

    static TreeMap<String, CityData> mainMap = new TreeMap<>();

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        String filePath = "../measurements.txt";
        File file = new File(filePath);
        long fileSize = file.length();
        int numProcs = Runtime.getRuntime().availableProcessors();

        ExecutorService executor = Executors.newFixedThreadPool(numProcs);
        List<Future<HashMap<String, CityData>>> futures = new ArrayList<>();

        long chunkSize = fileSize / numProcs;
        for (int i = 0; i < numProcs; i++) {
            long start = i * chunkSize;
            long end = (i == numProcs - 1) ? fileSize : start + chunkSize;
            futures.add(executor.submit(() -> processChunk(filePath, start, end)));
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        // Combine results from all threads
        for (Future<HashMap<String, CityData>> future : futures) {
            HashMap<String, CityData> chunkResult = future.get();
            mergeResults(chunkResult);
        }

        printResults();
    }

    private static HashMap<String, CityData> processChunk(String filePath, long start, long end) {
        HashMap<String, CityData> map = new HashMap<>();
        try (FileChannel channel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
            long position = start;

            // Align to the next newline if not starting at 0
            if (start != 0) {
                position = findNextNewline(channel, start);
                if (position == -1)
                    return map; // No data in this chunk
            }

            ByteBuffer buffer = ByteBuffer.allocate(8*1024); // 8KB buffer
            byte[] lineBuilder = new byte[50];
            var sti = 0;
            var mid = -1;

            while (position < end) {
                buffer.clear();
                int bytesRead = channel.read(buffer, position);
                if (bytesRead == -1)
                    break;

                buffer.flip();

                for (int i = 0; i < buffer.limit(); i++) {
                    byte b = buffer.get();
                    if (b == '\n') {
                        processLine(lineBuilder, mid, sti, map);
                        lineBuilder = new byte[50];
                        sti=0;
                        mid=-1;
                    } else {
                        if (b == ';') {
                            mid = sti;
                        }
                        lineBuilder[sti++] = b;
                    }
                }
                position += bytesRead;
            }

            // Read beyond the chunk to complete the last line
            boolean foundNewline = false;
            while (position < channel.size() && !foundNewline) {
                buffer.clear();
                int bytesRead = channel.read(buffer, position);
                if (bytesRead == -1)
                    break;

                buffer.flip();
                for (int i = 0; i < buffer.limit(); i++) {
                    byte b = buffer.get();
                    if (b == ';') {
                        mid = sti;
                    }
                    if (b == '\n') {
                        processLine(lineBuilder, mid, sti, map);
                        lineBuilder = new byte[50];
                        foundNewline = true;
                        position += i + 1;
                        break;
                    } else {
                        lineBuilder[sti++] = b;
                    }
                }
                if (!foundNewline)
                    position += bytesRead;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    private static long findNextNewline(FileChannel channel, long start) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        long position = start;
        while (position < channel.size()) {
            if (channel.read(buffer, position) == -1)
                break;
            buffer.flip();
            if (buffer.get() == '\n') {
                return position + 1;
            }
            buffer.clear();
            position++;
        }
        return -1; // No newline found
    }

    private static void processLine(byte[] line, int mid, int end, Map<String, CityData> map) {

        if(mid == -1 || mid>end){
            return;
        }

        byte[] nameBytes = Arrays.copyOfRange(line, 0, mid);


        String city = new String(nameBytes, StandardCharsets.UTF_8);
        byte[] tempA  = Arrays.copyOfRange(line, mid+1, end);

        double temp;
        try {
            temp = parseDouble(tempA);
        } catch (NumberFormatException e) {
            return;
        }

        map.compute(city, (k, v) -> {
            if (v == null) {
                return new CityData(temp, temp, temp, 1);
            } else {
                v.count++;
                v.sum += temp;
                v.min = Math.min(v.min, temp);
                v.max = Math.max(v.max, temp);
                return v;
            }
        });
    }

    private static void mergeResults(HashMap<String, CityData> chunkResult) {
        for (Map.Entry<String, CityData> entry : chunkResult.entrySet()) {
            mainMap.merge(entry.getKey(), entry.getValue(), (v1, v2) -> {
                v1.count += v2.count;
                v1.sum += v2.sum;
                v1.min = Math.min(v1.min, v2.min);
                v1.max = Math.max(v1.max, v2.max);
                return v1;
            });
        }
    }

    private static void printResults() {
        for (Map.Entry<String, CityData> entry : mainMap.entrySet()) {
            CityData data = entry.getValue();
            System.out.printf("%s=%.1f/%.1f/%.1f%n",
                    entry.getKey(),
                    data.min,
                    data.sum / data.count,
                    data.max);
        }
    }

    public static class CityData {
        public int count;
        public double min;
        public double max;
        public double sum;

        public CityData(double sum, double max, double min, int count) {
            this.sum = sum;
            this.max = max;
            this.min = min;
            this.count = count;
        }
    }

    private static double parseDouble(byte[] temp)
    {


        var intStartIdx = 0;

        if(temp[0] == '-'){
            intStartIdx = 1;
        }

        double v = (temp[temp.length-1] - '0') / 10.0; // ← Fix here
        double place = 1.0;

        for(int i= temp.length-3; i>=intStartIdx; i--){
            v += (temp[i]-'0') * place;
            place *= 10;
        }

        if (intStartIdx == 1){
            v = v * -1;
        }

        return v;
    }
}