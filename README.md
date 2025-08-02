# scomm - Stream Comparator for Unsorted Data

`scomm` is a command-line tool to compare two **unsorted** files or data streams, identifying lines that are common, new, or obsolete. It is designed with scalability and streaming in mind, handling massive datasets efficiently. It is similar to the "comm" Linux utility, but it can handle unsorted data and has more options for filtering and outputs.

## Synopsis

```sh
scomm [ -1 | -2 | -3 ] [ -d DELIMITER ] [ -H headerLines ] [ -b 0|-1|number ] [ -k LIST -p LIST ] 3<INPUT1 4<INOUT2 [ 5>OUTPUT1 ] [ 6>OUTPUT2 ] [ 7>OUTPUT3 ]
```

- INPUT1 is read from file descriptor 3 (FD3), which can be a file, a pipe or a process
- OUTPUT1, if not suppressed by the -1 option, will contain the lines only in INPUT1
- OUTPUT2, if not suppressed by the -2 option, will contain the lines only in INPUT2
- OUTPUT3, if not suppressed by the -3 option, will contain lines common to both INPUT1 and INPUT2

## Options

- `-1` — Output lines only in INPUT1 (aka old data).
- `-2` — Output lines only in INPUT2 (new data).
- `-3` — Output lines common to both inputs (default).
- `-H NUMBER` — Skip the first NUMBER header lines from both files.
- `-k LIST` — Use character or field keys to determine matching lines.
- `-p LIST` — Use payload fields to detect content updates when keys match.
- `-d DELIMITER` — Emit `DELIMITER` between sections in the output when using -1/-2/-3.
- `-b 0 | -1 | number` — Use batch mode, where INPUT1 is not read entirely in memory, and INPUT2 is read in batches.
- `-m` - Output "merge-style" data. More info in the examples below.
- `-l` - Output the full line when compareing by key/value. More info in the examples below.
   
## Streaming File Descriptors

- `3<INPUT1` - Read old data from file descriptor 3.
- `4<INPUT2` - Read new data from file descriptor 4.
- `5>OUTPUT1` - Write lines only in FILE2 (new data).
- `6>OUTPUT2` - Write lines only in FILE1 (obsolete data).
- `7>OUTPUT3` - Write matched lines as they are detected.

Inputs can be files but also the standard output of other processes, for example:
`scomm ... 3< <(unzip -p zipfile.txt) ...`

## Line-based Comparison

This mode is used when none of `-k/-p` are used.
Each line from INPUT2 is compared in full against each line from INPUT1, if they are the same they will go to OUTPUT3, the lines only in INPUT1 will go to OUTPUT1 and the lines only in INPUT2 will go to OUTPUT2.

## Field-Based Comparison

Normally scomm compares full lines, deciding they are identical or not by matching a full line from INPUT2 against lines from INPUT1.
In some cases, a partial match of the lines is needed, for which case we defined a `key` with `-k` and a `value/payload` with `-p`.
They are defined the same way as the `-f` parameter of the well known `cut` command. Both `-k` and `-p` have to be used, or none to compare full lines.

For example, if we have lines like:
  `123,4,abcd`
where 123 is the key, 4 is the value/payload, and acbd is other data we are not interested in.

When comparing this line with:
  `123,4,bcde`
we have the same key (123) and the same value/payload (4) so the lines are equal.
Scomm will now output on OUTPUT3 either:
  `123,4,bcde` if `-l` is used (output the full INPUT2 line), or
  `123,4` if `-l` is not used (only display the key and value)

When comparing this line with:
  `123,5,bcde`
we have the same key (123) and a different value/payload (4 vs 5) so the lines are not equal.
Scomm will now output on OUTPUT1 either:
  `123,4,abcd` if `-l` is used (output the full INPUT1 line), or
  `123,4` if `-l` is not used (output only the key/value from INPUT1 line)
And on OUTPUT2 either
  `123,5,bcde` if `-l` is used (output the full INPUT2 line), or
  `123,5` if `-l` is not used (output only the key/value from INPUT2 line)

TODO need more clarity here about -m and -l

`-m=true` assumes that:
   - deleted data from INPUT1 (keys did not match any INPUT2) - gets output in OUTPUT1; these will be "delete" statements.
   - new data from INPUT2 (keys do not match any INPUT1) and updated data from INPUT2 (keys matched lines in INPUT1 but payloads differ) will be merged into a database - gets output on OUTPUT2 (there is no point in deleting first the existing value). These will be "merge" statements.
This will likey lead to less data on OUTPUT1 then with `-m=false`.

`-m=false` assumes that:
  - the deleted data from INPUT1 (keys did not match any INPUT2) will be deleted first - gets output on OUTPUT1; these will be "delete" statements.
  - the new data from INPUT2 will be inserted (keys did not match any INPUT1) - gets output on OUTPUT2. These will be "insert" statements.
  - the updated data from INPUT2 will also be inserted (keys matches, payloads did not) - gets output on OUTPUT1 and OUTPUT2. These will also be "delete" and then "insert" statements.


Keys and payloads/values are defined using a LIST of characters (without `-d`) or fields (with `-d`).

### Character-Based Examples (`-k` and `-p`):

- `-k 1` = first character
- `-k 2-4` = characters 2, 3, 4
- `-k 5-` = characters 5 to end
- `-k -6` = characters 1 to 6
- `-k 2,4-6` = characters 2, 4, 5, 6

### Delimited Fields (`-d`):

When using `-d DELIMITER`, keys and payloads refer to field positions rather than characters.

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
