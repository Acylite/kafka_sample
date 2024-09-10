package sample.data.util;

import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FileHandler.class);
    public static void copyFileToLocal (ReadableByteChannel fileHandler, String fileDestination) throws IOException {
        InputStream fileStreamRead = Channels.newInputStream(fileHandler);
        OutputStream fileStreamWrite = Files.newOutputStream(Paths.get(fileDestination));
        IOUtils.copy(fileStreamRead, fileStreamWrite);
    }

    public static void copyFileToLocal (InputStream byteStream, String fileDestination) throws IOException {
        OutputStream fileStreamWrite = Files.newOutputStream(Paths.get(fileDestination));
        IOUtils.copy(byteStream, fileStreamWrite);
    }

    public static InputStream readFile(String path) throws IOException, FileNotFoundException {
        InputStream inputStream = FileHandler.class.getClassLoader().getResourceAsStream(path);
        if (inputStream == null) {
            throw new FileNotFoundException("File: " + path + " not found in the class path");
        }
        return inputStream;
    }

}
