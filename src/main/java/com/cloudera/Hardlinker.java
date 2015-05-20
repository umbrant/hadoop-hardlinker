package com.cloudera;

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.util.Time;

public class Hardlinker {
  public static void usage() {
    System.out.println("Supported subcommands:");
    System.out.println("  generate <dst> <num>");
    System.out.println("  link <src> <dst>");
    System.exit(1);
  }

  public static void generate(String dst, int num) throws IOException {
    Path root = Paths.get(dst);
    if (Files.exists(root)) {
      throw new IOException("Path " + dst + " already exists");
    }
    Files.createDirectory(root);
    
    // Create a subdirectory tree, where each dir has 64 subdirs and 64 files.
    // This is essentially like BFS.
    Queue<Path> queue = new LinkedList<>();
    queue.add(root);

    for (int i = 0; i < num;) {
      Path p = queue.poll();
      // Create 64 subdirs
      for (int j = 0; j < 64; j++) {
        Path subdir = p.resolve("subdir" + j);
        Files.createDirectory(subdir);
        queue.add(subdir);
      }
      // Create up to 64 files
      for (int j = 0; i < num && j < 64; i++, j++) {
        Path subfile = p.resolve("subfile" + i);
        Files.createFile(subfile);
      }
    }
    System.out.println("Created " + num + " subfiles in dst " + dst);
  }

  public static void link(String src, String dst) throws IOException {
    Path psrc = Paths.get(src);
    Path pdst = Paths.get(dst);

    if (!Files.isDirectory(psrc)) {
      throw new IOException("Path " + psrc + " is not a directory");
    }

    if (Files.exists(pdst)) {
      throw new IOException("Path " + pdst + " already exists");
    }

    Files.createDirectory(pdst);

    long start = Time.monotonicNow();
    int numLinks = 0;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(psrc)) {
      for (Path file : stream) {
        Path dstlink = pdst.resolve(file.getFileName());
        HardLink.createHardLink(file.toFile(), dstlink.toFile());
        numLinks++;
      }
    } catch (IOException | DirectoryIteratorException x) {
      // IOException can never be thrown by the iteration.
      // In this snippet, it can only be thrown by newDirectoryStream.
      System.err.println(x);
    }
    long end = Time.monotonicNow();

    long elapsedMillis = (end - start);
    System.out.println("Took " + elapsedMillis + " to create " + numLinks + " hardlinks");
  }

  public static void main(String[] args) throws IOException {

    if (args.length != 3) {
      usage();
    }

    String subcommand = args[0];

    switch (subcommand) {
    case "generate":
      generate(args[1], Integer.parseInt(args[2]));
      break;
    case "link":
      link(args[1], args[2]);
      break;
    default:
      System.err.println("Unknown subcommand");
      usage();
      break;
    }
  }
}
