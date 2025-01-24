package org.example;

import javax.swing.text.html.parser.Entity;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Main {

    static TreeMap<String, CityData> Mainmap = new TreeMap<>();

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {

        File file = new File("/home/sunilpatil/Documents/measurements.txt");
        long fileSize = file.length();
        int numProcs = Runtime.getRuntime().availableProcessors();

        HashMap<String,CityData>[] cityList = new HashMap[numProcs];

        Thread[] threads = new Thread[numProcs];

       // System.out.println(numProcs);

        RandomAccessFile accessFile = new RandomAccessFile(file, "r");

        long chunkSize = fileSize/numProcs;
        long see = 0;
        int i=0;

        //System.out.println(chunkSize);

        while (see < fileSize) {

            byte[] arr = new byte[(int) chunkSize+20];

            try {
                int ptr = accessFile.read(arr, 0, (int) chunkSize);

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

            int finalI = i;
            long finalSee = see;
            threads[i] = new Thread(() -> {
               // System.out.println("finalI = " + finalI+" see= "+ finalSee);
                processChunk(finalI,cityList,arr);
            });

            i++;
        }

       // System.out.println("see:"+see + " length:" +threads.length);

        for (Thread thread : threads) {
            if(thread != null) {
                thread.start();
            }
        }

       for (Thread thread : threads) {
           if(thread != null) {
               thread.join();
           }
       }

       // combine action
        for(HashMap<String , CityData> map : cityList) {
            if(map != null) {
                for (Map.Entry<String, CityData> entry : map.entrySet()) {
                    if (Mainmap.get(entry.getKey()) == null) {
                        Mainmap.put(entry.getKey(), entry.getValue());
                    } else {
                        // merge action

                        CityData temp = Mainmap.get(entry.getKey());
                        CityData temp2 = entry.getValue();
                        if (temp.min > temp2.min) {
                            temp.min = temp2.min;
                        }
                        if (temp.max < temp2.max) {
                            temp.max = temp2.max;
                        }
                        temp.sum = temp.sum + temp2.sum;
                        temp.count = temp.count + temp2.count;
                    }
                }
            }
        }

       print();

    }

    private static void print() {

        for (Map.Entry entry : Mainmap.entrySet()) {
            System.out.print(entry.getKey().toString()+":"+entry.getValue());
        }
    }

    public static void processChunk(int idx,Map<String, CityData>[] cityDataList,byte[] arr) {

        Map<String , CityData> map = new HashMap<>();
        cityDataList[idx] = map;
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

                String str = new String(arr, start, delim - start);
                double temperature = Double.parseDouble(new String(arr, delim + 1, i - delim - 1));


                updateMap(map, str, temperature);
                start = i + 1;
            }
        }


    }

    public static void updateMap(Map<String , CityData> map, String city, Double temp) {
        if (map.get(city) == null) {
            CityData cityData = new CityData(temp, temp, temp, 1);
            map.put(city, cityData);
        } else {
            CityData cityData = map.get(city);
            cityData.count++;
            cityData.sum += temp;
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
        public Double sum;

        public CityData(Double sum, Double max, Double min, int cnt) {
            this.sum = sum;
            this.max = max;
            this.min = min;
            this.count = cnt;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f, ", min, sum / count, max);
        }
    }


}