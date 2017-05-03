package com.hugolinton;

import java.io.*;
import java.util.ArrayList;
import java.util.zip.*;

/**
 * Created by hugol on 09/10/2016.
 */
public class UnzipUtils {

    final static int BUFFER = 2048;

    // "/home/admin/Downloads/movies/stockv2@ingram.zip"
    public ArrayList<String> unzipFile(String compressFilePath, String uncompressFolderPath){

        ArrayList<String> files = new ArrayList<String>();

        String uncompressedFilePath="";

        try {
            BufferedOutputStream dest = null;
            FileInputStream fis = new   FileInputStream(compressFilePath);
            ZipInputStream zis = new     ZipInputStream(new BufferedInputStream(fis));
            ZipEntry entry;
            while((entry = zis.getNextEntry()) != null) {
                uncompressedFilePath=uncompressFolderPath+entry.getName();
                System.out.println("Extracting entry to: " + uncompressedFilePath);
                files.add(uncompressedFilePath);
                int count;
                byte data[] = new byte[BUFFER];
                // write the files to the disk
                FileOutputStream fos = new
                        FileOutputStream(uncompressedFilePath);
                dest = new
                        BufferedOutputStream(fos, BUFFER);
                while ((count = zis.read(data, 0, BUFFER))
                        != -1) {
                    dest.write(data, 0, count);
                }


                dest.flush();
                dest.close();
            }
            zis.close();
        } catch(Exception e) {
            e.printStackTrace();
        }



        System.err.println(uncompressedFilePath);

        return files;
    }
}
