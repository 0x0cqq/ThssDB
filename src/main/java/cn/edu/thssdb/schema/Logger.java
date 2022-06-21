package cn.edu.thssdb.schema;


import cn.edu.thssdb.exception.FileIOException;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Logger {
    // log 所在的位置
    private String folder_name;
    private String file_name;
    private String full_path;

    public Logger(String folder_name, String file_name) {
        this.folder_name = folder_name;
        this.file_name = file_name;
        this.full_path = Paths.get(folder_name,file_name).toString();

        File dir = new File(this.folder_name);
        if(!dir.isDirectory()){
            dir.mkdirs();
        }
        File file = new File(this.full_path);
        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (Exception e){
                throw new FileIOException(this.full_path);
            }
        }
    }

    // Log control and recover from logs.
    public  void writeLog(String statement) {
        try {
            FileWriter writer = new FileWriter(this.full_path, true);
            writer.write(statement + "\n");
            writer.close();
        } catch (Exception e) {
            throw new FileIOException(this.full_path);
        }
    }

    // read in log, return in ArrayList<String>
    public ArrayList<String> readLog() {
        ArrayList<String> logList = new ArrayList<>();
        try {
            InputStreamReader reader = new InputStreamReader(new FileInputStream(this.full_path));
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                logList.add(line);
            }
        } catch (Exception e) {
            throw new FileIOException(this.full_path);
        }
        return logList;
    }
}
