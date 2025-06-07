# SQLite Database Parser

A lightweight SQLite database file parser written in Java that can read and analyze SQLite database files at a low level. This project is part of the CodeCrafters challenge and provides insights into the internal structure of SQLite databases.

## Overview

This parser reads SQLite database files directly, parsing the binary format to extract table information, records, and metadata without using the SQLite library. It's designed as a learning tool to understand how SQLite stores data internally.

**Note**: This is an educational project currently optimized for small databases. Performance improvements and support for larger databases are planned for future releases.

## Features

- Parse SQLite file headers and extract database metadata
- Read and decode B-tree pages (table and index pages)
- Extract table schemas from the `sqlite_master` table
- Decode SQLite records using variable-length integer (varint) encoding
- Display table information and record counts

## Commands

### `.dbinfo`
Displays basic information about the database file:
```bash
java -jar sqlite-parser.jar .dbinfo database.db
```

Output includes:
- Database page size
- Write format version
- Number of tables
- Schema information

### `.tables`
Lists all user-defined tables in the database (similar to SQLite's `.tables` command):
```bash
java -jar sqlite-parser.jar .tables database.db
```

Output includes:
- Table names in formatted columns
- Detailed information about each table (record count, root page, SQL definition)

### `.analyse`
Performs a deep analysis of all database pages:
```bash
java -jar sqlite-parser.jar .analyse database.db
```

Output includes:
- Page-by-page analysis
- B-tree structure information
- Cell contents and record data
- Page types (leaf/interior, table/index)

## Project Structure

```
src/
├── Main.java                 # Entry point and command dispatcher
├── SQLiteInfoReader.java     # Main database reading logic
├── parser/
│   ├── PageParser.java       # B-tree page parsing
│   ├── CellParser.java       # Cell content parsing
│   ├── SQLiteRecord.java     # Record structure and decoding
│   ├── DatabaseSchema.java   # Schema management
│   └── TableInfo.java        # Table metadata and queries
└── helpers/
    └── VarintDecoder.java    # Variable-length integer decoding
```

## Technical Details

### Supported Features
- SQLite format 3 databases
- Table B-tree pages (leaf and interior)
- Index B-tree pages (leaf and interior)
- Basic data types: NULL, INTEGER, REAL, TEXT, BLOB
- Variable-length integer encoding/decoding
- Schema parsing from `sqlite_master` table

### Limitations
- Currently optimized for small databases (< 100MB recommended)
- Does not support:
    - WITHOUT ROWID tables
    - Virtual tables
    - Full-text search tables
    - Overflow pages
    - Freelist pages
    - Write operations (read-only)

## Building and Running

### Prerequisites
- Java 17 or higher
- Maven or Gradle (optional, for dependency management)

### Building
```bash
javac -d out src/**/*.java
jar cvf sqlite-parser.jar -C out .
```

### Running
```bash
java -jar sqlite-parser.jar <command> <database-file>
```

Example:
```bash
java -jar sqlite-parser.jar .tables mydata.db
```

## Implementation Notes

### How It Works
1. **File Header**: Reads the first 100 bytes to extract database configuration
2. **Page Structure**: Each database file is divided into fixed-size pages
3. **B-tree Navigation**: Tables are stored as B-trees with leaf pages containing actual data
4. **Record Format**: Each record has a header followed by column values
5. **Varint Encoding**: SQLite uses variable-length integers to save space

### Key Classes
- `SQLiteInfoReader`: Main entry point for database operations
- `PageParser`: Handles B-tree page structure parsing
- `CellParser`: Extracts individual records from page cells
- `SQLiteRecord`: Represents a decoded database record
- `DatabaseSchema`: Manages table metadata and relationships

## Future Enhancements

- [ ] Support for larger databases with streaming/pagination
- [ ] Overflow page handling
- [ ] Index traversal and lookups
- [ ] SQL query execution (SELECT statements)
- [ ] Performance optimizations (caching, lazy loading)
- [ ] Support for all SQLite data types and features
- [ ] Command-line SQL REPL interface

## Contributing

This is a learning project, but suggestions and improvements are welcome! Areas that need work:
- Performance optimization for large databases
- More comprehensive error handling
- Support for additional SQLite features
- Unit tests and test coverage

## Acknowledgments

- Built as part of the [CodeCrafters](https://codecrafters.io/) SQLite challenge
- SQLite file format documentation: https://www.sqlite.org/fileformat.html

## License

This project is open source and available under the MIT License.

---

**Note**: This is an educational implementation and should not be used as a replacement for the official SQLite library in production environments.