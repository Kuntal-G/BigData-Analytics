package com.mapreduce.example.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Creates department map file from its text equivalent from HDFS
 *
 * @author ashrith
 */
public class CreateDepartmentsMapFile {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Requires 2 arguments: [input_text_file] [output_dir]");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputPath = args[1];

        Configuration conf = new Configuration();
        FileSystem outputFS = FileSystem.get(URI.create(outputPath), conf);

        Text key = new Text();
        Text value = new Text();

        MapFile.Writer writer = null;

        try {
            writer = new MapFile.Writer(conf, outputFS, outputPath, key.getClass(), value.getClass());

            FileSystem inputFS = FileSystem.get(conf);
            Path inputFilePath = new Path(inputFile);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputFS.open(inputFilePath)));
            try {
                String line;
                line = bufferedReader.readLine();
                while(line != null) {
                    key.set(line.split(",")[0]);
                    value.set(line.split(",")[1]);
                    writer.append(key, value);
                    line = bufferedReader.readLine();
                }
            } finally {
                bufferedReader.close();
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
