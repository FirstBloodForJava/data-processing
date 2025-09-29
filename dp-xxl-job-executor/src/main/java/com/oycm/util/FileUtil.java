package com.oycm.util;

import com.xxl.job.core.context.XxlJobHelper;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * @author ouyangcm
 * create 2025/9/25 20:48
 */
public class FileUtil {

    private static final String SPARK_JSON_DIR = "param/spark";

    private static final String LIB_DIR = "lib";


    public static String writeSparkJobJson(long jobId, String json) throws IOException {
        File tempFileDir = new File(SPARK_JSON_DIR);
        if (!tempFileDir.exists()) {
            tempFileDir.mkdirs();
        }
        String jobDir = String.format(SPARK_JSON_DIR + "/temp-%s-%s.json", jobId, System.currentTimeMillis());

        try (FileWriter fileWriter = new FileWriter(jobDir)) {
            fileWriter.append(json);
        }

        return jobDir;
    }

    public static String getDatasourceJar(Set<String> sets) {
        Set<String> jarPathSet = new HashSet<>();
        sets.forEach(name -> {
            File file = new File(String.format(LIB_DIR + "/%s", name));
            if (!file.isDirectory()) {
                XxlJobHelper.log("{} is not directory!", file.getAbsoluteFile());
                throw new RuntimeException(file.getAbsoluteFile() + " is not directory");
            }
            File[] files = file.listFiles();
            if (files == null || files.length == 0) {
                XxlJobHelper.log("{} is empty!", file.getAbsoluteFile());
                throw new RuntimeException(file.getAbsoluteFile() + " is not empty");
            }

            for (File f : files) {
                String jarName = f.getName();
                if (f.isFile() && jarName.endsWith(".jar")) {
                    jarPathSet.add(String.format(LIB_DIR + "/%s/%s", name, jarName));
                }
            }
        });

        return String.join(",", jarPathSet);
    }

    public static Boolean xxlLog(InputStream inputStream) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                XxlJobHelper.log(line);
            }
            return true;
        } catch (IOException e) {
            XxlJobHelper.log(e);
        }
        return false;
    }

}
