scomm - compare two unsorted files or data streams.

scomm [ -1 | -2 | -3 ] [ -d DELIMITER ] FILE1 FILE2 [ 3<FILE1 ] [ 4<FILE2 ] [ 5>FILE3 ] [ 6>FILE4 ] [ 7>FILE5 ]

When FILE1 or FILE2 (but not both) is -, read standard input.

-1    output lines only in FILE1
-2    output lines only in FILE2
-3    produce on standard output the lines common to both files (default).

FILE1 can be read from file descriptor 3, like
    scomm 3<FILE1
of, using process substitution:
    scomm 3< <(cat FILE1)
(obviously any command can replace cat, for example reading from a database).

FILE2 can be read from file descriptor 4, similar to FILE1.

When DELIMITER (a string) is specified, the lines from from FILE1 and FILE2 will be produced on standard output in this order, obeying the -1/-2/-3 filters:
    lines common to both files
    DELIMITER
    lines in FILE2 only
    DELIMITER
    lines in FILE1 only

When file descriptor 7 is used, the lines common to both files will be output on it in a streaming fasion (as they are identified). When not used, and without DELIMITER, the common lines will be descarded (or when using 7>/dev/null).
When file descriptor 5 is used, the lines in FILE2 only will be output on it, after they are all identified (but FILE2 will NOT be cached in full).
When file descriptor 6 is used, the lines in FILE1 only will be output on it, after they are all identified (FILE1 WILL be cached in full).

Use cases:

The main idea of scomm is that I receive "new" data in FILE2, and FILE1 contains the "old/previous" data. Instead of processing the FILE2 data in full, I extract only what is new in FILE2 (does not exist in FILE1) and process these as "inserts" maybe, and only what is old in FILE1 (does not exist anymore in FILE2) and process these as "deletes" maybe.

Examples:

