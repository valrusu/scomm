scomm - compare two unsorted files or data streams.

scomm [ -1 | -2 | -3 ] [ -D DELIMITER ] FILE1 FILE2 [ 3<FILE1 ] [ 4<FILE2 ] [ 5>FILE3 ] [ 6>FILE4 ] [ 7>FILE5 ]

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
FILE1 in this case can be the previously received data in a file, or a query from a database returning existing data.
FILE2 can be the newly received data in a file, or a query from a database.

TODO Normally the comparison between lines is done using the full line. However, sometimes we need only certain fields to decide if the lines are identical or not. These fields are specified as -k LIST and -p LIST. They are lists of characters (similar to the "cut" program), or lists of fields if the data is delimited by -d character. 
The KEY fields are used to build the "key" by which lines are matched (deciding if they are "new" - only in FILE2, "old" - only in FILE1, or "matched" - in both files), and for the "matched" lines the PAYLOAD fields are used to compare the lines and decide if they are to be output. The line is output in full regardless of how they are identified.
The logic then becomes:
    - if lines match in full, they are common lines
    - lines are only in FILE2 if there is no corresponding KEY in FILE1
    - lines are only in FILE1 if there is no corresponding KEY in FILE2
    - lines with KEY fields indentical and PAYLOAD fields different are only in FILE2 (considered updates to lines from FILE1)

-k 1 = first character of the line
-k 2-4 = characters 2, 3 and 4 of the line
-k 5- = all characters from the 5th to the end of the line
-k -6 = characters 1 to 6 of the line
-k - = the full line (silly)
-k 2,4,6 = characters 2, 4 and 6 of the line
-k 2,4-6 = charcters 2, 4, 5 and 6 of the line

If -d LINEDELIMITER is specified, then the characters become fields separated by LINEDELIMITER, with the same format.

Examples:

simple - 

Process changes only between two existing files, one old and one new:

Process changes only between two existing files, one old which was compressed and one new:

If the second file is compressed too:

Compare database contents (mySQL in this case, but can be any as long as data can be extracted in the required format) against a new file:

Compare a "new" database, which contains the latest data, with an "old" database which contains the previous data:

    command
    ... and process the output to update the old database.
