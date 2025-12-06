package com.example.family.SetGetCommand;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataStore {

    public enum IOMode { BUFFERED, UNBUFFERED }

    private final Map<String, String> map = new ConcurrentHashMap<>();
    private final Path messagesDir;
    private final IOMode ioMode;

    // Default: use JVM property 'messages.dir' or env 'MESSAGES_DIR', default to 'messages'
    // IO mode can be set with JVM property 'messages.io' or env 'MESSAGES_IO' (BUFFERED or UNBUFFERED)
    public DataStore() {
        String dirProp = System.getProperty("messages.dir");
        if (dirProp == null || dirProp.isBlank()) {
            dirProp = System.getenv("MESSAGES_DIR");
        }
        if (dirProp == null || dirProp.isBlank()) {
            dirProp = "messages";
        }

        String modeProp = System.getProperty("messages.io");
        if (modeProp == null || modeProp.isBlank()) {
            modeProp = System.getenv("MESSAGES_IO");
        }
        IOMode mode = IOMode.BUFFERED;
        if (modeProp != null) {
            try {
                mode = IOMode.valueOf(modeProp.toUpperCase());
            } catch (IllegalArgumentException ignored) {
            }
        }

        this.messagesDir = Paths.get(dirProp);
        this.ioMode = mode;

        try {
            if (!Files.exists(messagesDir)) {
                Files.createDirectories(messagesDir);
            }
        } catch (IOException e) {
            System.err.println("Could not create messages directory: " + e.getMessage());
        }
    }

    // Constructor for tests / explicit config
    public DataStore(Path messagesDir, IOMode ioMode) {
        this.messagesDir = messagesDir == null ? Paths.get("messages") : messagesDir;
        this.ioMode = ioMode == null ? IOMode.BUFFERED : ioMode;

        try {
            if (!Files.exists(this.messagesDir)) {
                Files.createDirectories(this.messagesDir);
            }
        } catch (IOException e) {
            System.err.println("Could not create messages directory: " + e.getMessage());
        }
    }

    public String set(String key, String value) {
        // update cache
        map.put(key, value);

        Path file = messagesDir.resolve(key + ".msg");

        if (ioMode == IOMode.BUFFERED) {
            try (BufferedWriter writer = Files.newBufferedWriter(file,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {

                writer.write(value);
                writer.flush();
                return "OK";
            } catch (IOException e) {
                System.err.println("Failed to write message to disk (buffered): " + e.getMessage());
                return "ERROR: " + e.getMessage();
            }

        } else { // UNBUFFERED
            try (FileOutputStream out = new FileOutputStream(file.toFile(), false)) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                out.write(bytes);
                out.getFD().sync();
                return "OK";
            } catch (IOException e) {
                System.err.println("Failed to write message to disk (unbuffered): " + e.getMessage());
                return "ERROR: " + e.getMessage();
            }
        }
    }

    public String get(String key) {
        // first look in cache
        String cached = map.get(key);
        if (cached != null) {
            return cached;
        }

        Path file = messagesDir.resolve(key + ".msg");
        if (!Files.exists(file)) {
            return "NOT_FOUND";
        }

        if (ioMode == IOMode.BUFFERED) {
            try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    if (sb.length() > 0)
                        sb.append(System.lineSeparator());
                    sb.append(line);
                }
                String value = sb.toString();
                map.put(key, value);
                return value;
            } catch (IOException e) {
                System.err.println("Failed to read message from disk (buffered): " + e.getMessage());
                return "ERROR: " + e.getMessage();
            }
        } else { // UNBUFFERED
            try (FileInputStream in = new FileInputStream(file.toFile())) {
                byte[] all = in.readAllBytes();
                String value = new String(all, StandardCharsets.UTF_8);
                map.put(key, value);
                return value;
            } catch (IOException e) {
                System.err.println("Failed to read message from disk (unbuffered): " + e.getMessage());
                return "ERROR: " + e.getMessage();
            }
        }
    }

}
