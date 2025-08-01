# scomm - Stream Comparator for Unsorted Data

`scomm` is a high-performance command-line tool to compare two **unsorted** files or data streams, identifying lines that are common, new, or obsolete. It is designed with scalability and streaming in mind, handling massive datasets efficiently.

## Synopsis

```sh
scomm [ -1 | -2 | -3 ] [ -d DELIMITER ] [ -H headerLines ] [ -b 0|-1|number ] 3<FILE1 4<FILE2 [ 5>FILE3 ] [ 6>FILE4 ] [ 7>FILE5 ]
```

- When FILE1 or FILE2 (but not both) is `-`, read from standard input.
- FILE1 can be read from file descriptor 3 and FILE2 from file descriptor 4.
- FILE3, FILE4, and FILE5 are output destinations for specific line categories.

## Options

- `-1` — Output lines only in FILE1 (old data).
- `-2` — Output lines only in FILE2 (new data).
- `-3` — Output lines common to both files (default).
- `-D DELIMITER` — Emit `DELIMITER` between sections in the output when using -1/-2/-3.
- `-H N` — Skip the first N header lines from both files.
- `-k LIST` — Use character or field keys to determine matching lines.
- `-p LIST` — Use payload fields to detect content updates when keys match.

## Streaming File Descriptors

- `3<FILE1` — Read old data from file descriptor 3.
- `4<FILE2` — Read new data from file descriptor 4.
- `5>FILE3` — Write lines only in FILE2 (new data).
- `6>FILE4` — Write lines only in FILE1 (obsolete data).
- `7>FILE5` — Write matched lines as they are detected.

## Field-Based Comparison

You can extract "keys" and "payloads" to define how lines are compared:

### Character-Based Examples (`-k` and `-p`):

- `-k 1` = first character
- `-k 2-4` = characters 2, 3, 4
- `-k 5-` = characters 5 to end
- `-k -6` = characters 1 to 6
- `-k 2,4-6` = characters 2, 4, 5, 6

### Delimited Fields (`-d`):

When using `-d DELIM`, keys and payloads refer to field positions rather than characters.

## Use Cases

`scomm` is especially useful in data ingestion pipelines or ETL processes:

- Compare previous and current datasets (e.g. from databases, files).
- Identify new rows to insert or old rows to delete.
- Filter massive datasets down to meaningful changes.

## Examples

### Compare Two Files

```sh
scomm -1 -2 -3 3<old.txt 4<new.txt
```

### Use Custom Delimiter Between Result Sections

```sh
scomm -D "---" -1 -2 -3 3<old.txt 4<new.txt
```

### Save Changes to Files

```sh
scomm -3 3<old.txt 4<new.txt 5>new_only.txt 6>old_only.txt
```

### Process Huge ZIP Files

```sh
unzip file1.zip
unzip file2.zip

./scomm -H 1 -1 -2 -3 3<file1 4<file2

# Save diffs to files
./scomm -H 1 -3 3<file1 4<file2 5>5.txt 6>6.txt
```

### Example Output

```
File1: total 88053459 kept 677670 0.7696%
File2: total 88041880 kept 666091 0.7566%
Common: 87375789 99.2304% 99.2434%
End scomm, time taken 132 sec
```

## Advanced Example: Database vs File

```sh
scomm 3< <(mysql -B -e 'SELECT ...') 4<new_data.txt 5>insert.txt 6>delete.txt
```

## TODO

- Add batch option for KV based matching


---

© Valentin Rusu. `scomm` is in production and optimized for real-world large-scale diff operations.
