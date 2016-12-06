package ru.sbrf.ofep.kafka;

public class Utils {

    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception ioe) {
            // ignore
        }
    }
}
