package zx.hasher;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    static ExecutorService workexec = Executors.newFixedThreadPool(Math.max(
            Runtime.getRuntime().availableProcessors(), 1)
    );
    static ExecutorService resultexec = Executors.newSingleThreadExecutor();
    static FileWriter fileWriter;

    static boolean shouldHandle(File file) {
        try {
            return file.isFile()
                    && file.getAbsolutePath().equals(file.getCanonicalPath()) // 不是 sym link 类型文件
                    && file.length() > 5 * 1024 * 1024; // 必需大于 5M 的文件
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private static void handle(final File file) {
        workexec.submit(new Runnable() {

                            ThreadLocal<MessageDigest> threadDigest = new ThreadLocal<>();
                            ThreadLocal<ByteBuffer> threadBuffer = new ThreadLocal<>();

                            @Override
                            public void run() {
                                // 把文件进行Hash
                                try {
                                    if (threadDigest.get() == null) {
                                        threadDigest.set(MessageDigest.getInstance("SHA-1"));
                                    }
                                    if (threadBuffer.get() == null) {
                                        threadBuffer.set(ByteBuffer.allocateDirect(32 * 1024));
                                    }
                                    FileInputStream fileInputStream = new FileInputStream(file);
                                    FileChannel channel = fileInputStream.getChannel();

                                    MessageDigest digest = threadDigest.get();
                                    ByteBuffer buffer = threadBuffer.get();

                                    digest.reset();
                                    buffer.clear();
                                    while (true) {
                                        if (channel.read(buffer) == -1) {
                                            break;
                                        }
                                        buffer.flip();
                                        digest.update(buffer);
                                        buffer.clear();
                                    }
                                    channel.close();
                                    fileInputStream.close();
                                    final String filePath = file.getPath();
                                    final long fileSize = file.length();
                                    final String digestHex = new BigInteger(1, digest.digest()).toString(16);

                                    resultexec.submit(() -> {
                                        try {
                                            StringBuilder b = new StringBuilder();

                                            // format
                                            b.append(fileSize).append(" ").append(digestHex).append(" ").append
                                                    (filePath);

                                            // write
                                            System.out.println(b.toString());
                                            fileWriter.append(b.toString()).append("\n");
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    });

                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }

        );
    }

    public static void main(String[] args) throws Exception {

        String outPath = "hash.txt";
        String[] inPaths = new String[] {System.getenv("HOME")};

        // parse args
        //        if (args.length == 0) {
        //            System.out.println("Usage: -o {outPath} {inPaths...}");
        //            return;
        //        }

        int i = 0;
        ArrayList<String> inPathArray = new ArrayList<>();
        while (i < args.length) {
            if ("-o".equals(args[i])) {
                if (i < args.length) {
                    String argOutPath = args[++i];
                    if (argOutPath != null) {
                        outPath = argOutPath.replaceFirst("^~", System.getenv("HOME"));
                    }
                    i++;
                    continue;
                } else {
                    throw new IllegalArgumentException("argument -o must be followed by a file path");
                }
            }
            inPathArray.add(args[i].trim().replaceFirst("^~", System.getenv("HOME")));
        }
        if (!inPathArray.isEmpty()) {
            inPaths = inPathArray.toArray(new String[inPathArray.size()]);
        }

        // 准备输出文件
        fileWriter = new FileWriter(outPath);

        long startNanoTime = System.nanoTime();

        // write your code here
        LinkedList<File> fileToVisit = new LinkedList<>();
        for (String inPath : inPaths) {
            File beginFile = new File(inPath);
            fileToVisit.addFirst(beginFile);
        }
        while (!fileToVisit.isEmpty()) {
            File file = fileToVisit.removeFirst();
            if (file.isDirectory()) {
                File[] subFiles = file.listFiles();
                if (subFiles != null) {
                    fileToVisit.addAll(Arrays.asList(subFiles));
                }
            } else {
                // 访问文件
                if (shouldHandle(file)) {
                    //System.out.println("handle file: " + file);
                    handle(file);
                }
            }
        }

        workexec.shutdown();
        workexec.awaitTermination(2, TimeUnit.DAYS);

        resultexec.submit(() -> {
            try {
                fileWriter.flush();
                fileWriter.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

            // 异步结束
            resultexec.shutdown();
        });

        // 收尾工作 交给 resultexec 异步处理
        resultexec.awaitTermination(2, TimeUnit.DAYS);

        System.out.println();
        System.out.println("Total Time Used: " + ((double) (System.nanoTime() - startNanoTime)) / (1000 * 1000 *
                                                                                                           1000.0d));
    }

}
