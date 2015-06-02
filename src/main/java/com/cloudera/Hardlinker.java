package com.cloudera;

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Hardlinker {

  public static class Time {
    public static long monotonicNow() {
      return System.nanoTime() / 1000 / 1000;
    }
  }

  public static void usage() {
    System.out.println("Supported subcommands:");
    System.out.println("  generate <dst> <num>");
    System.out.println("  link <src> <dst>");
    System.exit(1);
  }

  public static class Generator implements Callable<Void> {
    private final String dst;
    private final int num;

    public Generator(String dst, int num) {
      this.dst = dst;
      this.num = num;
    }

    @Override
    public Void call() throws Exception {
      generate(dst, num);
      return null;
    }

    public void generate(String dst, int num) throws IOException {
      Path root = Paths.get(dst);
      if (Files.exists(root)) {
        throw new IOException("Path " + dst + " already exists");
      }
      Files.createDirectory(root);

      System.out.println("Starting generation...");
      final long start = Time.monotonicNow();
      // Create a subdirectory tree, where each dir has 64 subdirs and 64 files.
      // This is essentially like BFS.
      Queue<Path> queue = new LinkedList<>();
      queue.add(root);

      long lastPrint = 0;
      for (int i = 0; i < num; ) {
        Path p = queue.poll();
        // Create up to 64 subdirs
        for (int j = 0; i < num && j < 64; i++, j++) {
          Path subdir = p.resolve("subdir" + j);
          Files.createDirectory(subdir);
          queue.add(subdir);
        }
        // Create up to 64 files
        for (int j = 0; i < num && j < 64; i++, j++) {
          Path subfile = p.resolve("subfile" + j);
          Files.createFile(subfile);
        }
        if (i - lastPrint >= 10000) {
          System.out.println("Created " + i + " items...");
          lastPrint = i;
        }
      }
      final long end = Time.monotonicNow();
      final long elapsedMillis = end - start;
      System.out.println(
          "Created " + num + " files and directories in " + dst + " in " + elapsedMillis + "ms");
    }
  }

  public static class Linker implements Callable<Void> {

    private final Path srcRoot;
    private final Path dstRoot;

    private final ExecutorService exec;

    public Linker(String srcRoot, String dstRoot, int numThreads) {
      this.srcRoot = Paths.get(srcRoot);
      this.dstRoot = Paths.get(dstRoot);
      exec = Executors.newFixedThreadPool(numThreads);
      System.out.println("Using " + numThreads + " worker threads");
    }

    @Override
    public Void call() throws Exception {
      if (!Files.isDirectory(srcRoot)) {
        throw new IOException("Path " + srcRoot + " is not a directory");
      }

      if (Files.exists(dstRoot)) {
        throw new IOException("Path " + dstRoot + " already exists");
      }

      Files.createDirectory(dstRoot);

      System.out.println("Starting recursive link...");
      final long start = Time.monotonicNow();
      final int num = recursiveLink(srcRoot, dstRoot);
      exec.shutdown();
      exec.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
      final long end = Time.monotonicNow();

      final long elapsedMillis = (end - start);
      final double numPerSec = num / ((double) elapsedMillis / 1000.0f);
      System.out.println(
          "Created " + num + " files and directories in " + dstRoot + " in " + elapsedMillis
              + "ms");
      System.out.println("Number per second: " + String.format("%.02f", numPerSec));

      return null;
    }

    public int recursiveLink(Path src, Path dst) {
      int num = 0;
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(src)) {
        for (Path p : stream) {
          Path subdst = dst.resolve(p.getFileName());
          if (Files.isDirectory(p)) {
            Files.createDirectory(subdst);
            num++;
            num += recursiveLink(p, subdst);
          } else if (Files.isRegularFile(p)) {
            exec.submit(new LinkerWorker(src, dst));
            num++;
          }
        }
      } catch (IOException | DirectoryIteratorException x) {
        // IOException can never be thrown by the iteration.
        // In this snippet, it can only be thrown by newDirectoryStream.
        System.err.println(x);
      }
      return num;
    }

    public class LinkerWorker implements Callable<Void> {

      private final Path src;
      private final Path dst;

      public LinkerWorker(Path src, Path dst) {
        this.src = src;
        this.dst = dst;
      }

      @Override
      public Void call() throws Exception {
        Files.createLink(dst, src);
        return null;
      }
    }
  }

  public static void main(String[] args) throws Exception {

    if (args.length < 3) {
      usage();
    }

    String subcommand = args[0];

    switch (subcommand) {
    case "generate":
      new Generator(args[1], Integer.parseInt(args[2])).call();
      break;
    case "link":
      int numThreads = 12;
      if (args.length == 4) {
        numThreads = Integer.parseInt(args[3]);
      }
      new Linker(args[1], args[2], numThreads).call();
      break;
    default:
      System.err.println("Unknown subcommand");
      usage();
      break;
    }
  }
}
