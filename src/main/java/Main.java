import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;


public class Main {

  private static final int SQLITE_HEADER_SIZE = 100; // SQLite header is 100 bytes

  public static void main(String[] args) throws IOException {

    if (args.length < 2) {
      System.err.println("Usage: java Main <database path> <command>");
      System.exit(1);
    }

    String databaseFilePath = args[0];
    String command = args[1];

    // Only read the header
    byte[] header = readDatabaseHeader(databaseFilePath);
    if (header == null) {
      System.err.println("Failed to read database file");
      System.exit(1);
    }


    if (!isValidSQLiteFile(header)) {
      System.err.println("Invalid SQLite database file");
      System.exit(1);
    }

    switch (command) {
      case ".dbinfo" -> SQLiteInfoReader.displayDatabaseInfo(databaseFilePath);
      default -> {
        System.err.println("Unknown command: " + command);
        System.err.println("Available commands: .dbinfo");
        System.exit(1);
      }
    }
  }


  private static boolean isValidSQLiteFile(byte[] header) {
    if (header == null || header.length < 16) {
      return false;
    }
    String magic = new String(header, 0, 16, StandardCharsets.US_ASCII);
    return "SQLite format 3\0".equals(magic);
  }


  private static byte[] readDatabaseHeader(String filePath) {
    try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
      if (file.length() < SQLITE_HEADER_SIZE) {
        return null;
      }
      byte[] header = new byte[SQLITE_HEADER_SIZE];
      file.readFully(header);
      return header;
    } catch (IOException e) {
      System.err.println("Error reading file: " + e.getMessage());
      return null;
    }
  }

}


