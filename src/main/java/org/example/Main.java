package org.example;

import javax.swing.text.html.parser.Entity;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Main {

    static HashMap<String, CityData> map = new HashMap<>();

    public static void main(String[] args) throws FileNotFoundException {

        File file = new File("/home/sunil/test.txt");

        // System.out.println(file.length());

        RandomAccessFile accessFile = new RandomAccessFile(file, "r");

        long fileSize = file.length();
        int chunkSize = 10100;
        long see = 0;

        while (see < fileSize) {

            byte[] arr = new byte[chunkSize];

            try {
                int ptr = accessFile.read(arr, 0, 10000);

                byte curr = accessFile.readByte();

                while (curr != (byte) '\n') {
                    arr[ptr] = curr;
                    ptr++;
                    curr = accessFile.readByte();
                }

                see += ptr + 1;  // Move past the processed data
                accessFile.seek(see);

            } catch (EOFException ex) {
                break;
            } catch (IOException ex) {
                System.out.println(ex);
            }
            processChunk(arr);
        }

        print();

    }

    private static void print() {
        List<String> sortedKeys = new ArrayList<>(map.keySet());
        Collections.sort(sortedKeys);

        for (String key : sortedKeys) {
            CityData data = map.get(key);
            System.out.printf("%s:%.1f/%.1f/%.1f, ", key, data.min, data.mean / data.count, data.max);
        }
    }

    public static void processChunk(byte[] arr) {
        int start = 0;

        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == (byte) '\n') {

                int delim = start;

                for (int j = delim; j < i; j++) {
                    if (arr[j] == (byte) ';') {
                        delim = j;
                        break;
                    }
                }

                //      byte[] city = Arrays.copyOfRange(arr, start, delim);
                //    byte[] temp = Arrays.copyOfRange(arr, delim+1, i);
                //    String str = new String(city);
                String str = new String(arr, start, delim - start);
                double temperature = Double.parseDouble(new String(arr, delim + 1, i - delim - 1));
                updateMap(str, temperature);
                //System.out.println(str + " temp is " + temperature);
                start = i + 1;
            }
        }
    }

    public static void updateMap(String city, Double temp) {
        if (map.get(city) == null) {
            CityData cityData = new CityData(temp, temp, temp, 1);
            map.put(city, cityData);
        } else {
            CityData cityData = map.get(city);
            cityData.count++;
            cityData.mean += temp;
            if (temp < cityData.min) {
                cityData.min = temp;
            }
            if (temp > cityData.max) {
                cityData.max = temp;
            }
        }
    }


    public static class CityData {
        public int count;
        public Double min;
        public Double max;
        public Double mean;

        public CityData(Double mean, Double max, Double min, int cnt) {
            this.mean = mean;
            this.max = max;
            this.min = min;
            this.count = cnt;
        }
    }


}