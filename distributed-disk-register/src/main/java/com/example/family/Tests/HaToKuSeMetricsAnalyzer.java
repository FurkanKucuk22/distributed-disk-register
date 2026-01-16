package com.example.family.Tests;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * CSV Metrics Analyzer (Java)
 *
 * Reads: hatokuse_client_metrics.csv
 * Columns expected: ts,op,key,payload_bytes,ok,rtt_ms,response
 *
 * Produces:
 *  - total ops, success, error, error rate
 *  - large payload (>= 900000 bytes) stats
 *  - top 5 error reasons (ERROR... prefix up to ':')
 */
public class HaToKuSeMetricsAnalyzer {

    private static final String FILE_NAME = "hatokuse_client_metrics.csv";
    private static final int LARGE_PAYLOAD_THRESHOLD = 900_000;

    public static void main(String[] args) {
        String file = (args.length > 0 && args[0] != null && !args[0].isBlank()) ? args[0] : FILE_NAME;

        long totalOps = 0;
        long successOps = 0;
        long errorOps = 0;

        // error reason -> count
        Map<String, Long> errorReasons = new HashMap<>();

        long largePayloadTotal = 0;
        long largePayloadErrors = 0;

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {

            // header
            String header = br.readLine();
            if (header == null) {
                System.out.println("HATA: CSV boş.");
                return;
            }

            System.out.println("Dosya analiz ediliyor, lütfen bekle...");

            String line;
            while ((line = br.readLine()) != null) {
                // Parse one CSV row into fields
                List<String> row = parseCsvLine(line);

                // Expected 7 columns; if fewer, skip safely
                if (row.size() < 7) {
                    continue;
                }

                // row indices: 0 ts, 1 op, 2 key, 3 payload_bytes, 4 ok, 5 rtt_ms, 6 response
                totalOps++;

                boolean isOk = "1".equals(safeGet(row, 4));

                int payload = parseIntSafe(safeGet(row, 3), 0);

                String responseMsg = safeGet(row, 6);

                if (payload >= LARGE_PAYLOAD_THRESHOLD) {
                    largePayloadTotal++;
                    if (!isOk) largePayloadErrors++;
                }

                if (isOk) {
                    successOps++;
                } else {
                    errorOps++;

                    if (responseMsg != null && responseMsg.startsWith("ERROR")) {
                        String shortErr = responseMsg;
                        int idx = responseMsg.indexOf(':');
                        if (idx > 0) shortErr = responseMsg.substring(0, idx);
                        increment(errorReasons, shortErr);
                    } else {
                        increment(errorReasons, "UNKNOWN_FAILURE");
                    }
                }
            }

            printReport(totalOps, successOps, errorOps, largePayloadTotal, largePayloadErrors, errorReasons);

        } catch (IOException e) {
            System.out.println("HATA: Dosya okunamadı: " + e.getMessage());
        }
    }

    private static void printReport(long totalOps,
                                    long successOps,
                                    long errorOps,
                                    long largePayloadTotal,
                                    long largePayloadErrors,
                                    Map<String, Long> errorReasons) {

        System.out.println("\n" + "=".repeat(40));
        System.out.println("📊 TEST SONUÇ RAPORU");
        System.out.println("=".repeat(40));

        System.out.println("Toplam İşlem: " + totalOps);
        System.out.println("Başarılı    : " + successOps);
        System.out.println("Hatalı      : " + errorOps);

        if (totalOps > 0) {
            double errRate = (errorOps * 100.0) / totalOps;
            System.out.printf(Locale.US, "Genel Hata Oranı: %%%.2f%n", errRate);
        }

        System.out.println("-".repeat(40));
        System.out.println("📦 BÜYÜK DOSYA (1 MB) ANALİZİ");
        System.out.println("Toplam 1 MB Gönderimi: " + largePayloadTotal);
        System.out.println("Hata Veren 1 MB      : " + largePayloadErrors);

        if (largePayloadTotal > 0) {
            double bigErrRate = (largePayloadErrors * 100.0) / largePayloadTotal;
            System.out.printf(Locale.US, "Büyük Dosya Hata Oranı: %%%.2f%n", bigErrRate);
        }

        System.out.println("-".repeat(40));
        System.out.println("❌ HATA NEDENLERİ (Top 5)");

        if (errorReasons.isEmpty()) {
            System.out.println("  (Hata nedeni ayrıştırılamadı veya hata yok)");
            return;
        }

        // Sort by count desc
        List<Map.Entry<String, Long>> list = new ArrayList<>(errorReasons.entrySet());
        list.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

        int top = Math.min(5, list.size());
        for (int i = 0; i < top; i++) {
            var e = list.get(i);
            System.out.println("Count: " + e.getValue() + " -> " + e.getKey());
        }
    }

    /**
     * Minimal CSV parser supporting:
     *  - commas inside quoted fields
     *  - escaped quotes: "" inside quoted fields
     *
     * Returns list of fields for one line.
     */
    private static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        if (line == null) return fields;

        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (inQuotes) {
                if (c == '"') {
                    // if next char is also quote -> escaped quote
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        sb.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    sb.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    fields.add(sb.toString());
                    sb.setLength(0);
                } else {
                    sb.append(c);
                }
            }
        }
        fields.add(sb.toString());
        return fields;
    }

    private static String safeGet(List<String> row, int idx) {
        if (row == null) return null;
        if (idx < 0 || idx >= row.size()) return null;
        return row.get(idx);
    }

    private static int parseIntSafe(String s, int def) {
        if (s == null) return def;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static void increment(Map<String, Long> map, String key) {
        map.put(key, map.getOrDefault(key, 0L) + 1L);
    }
}
