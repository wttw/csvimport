# csvimport
Clean up CSV files for import into PostgreSQL.

For each CSV file it's given it will create a SQL script that will create a matching table and import the data into it.

This is a fairly quick hack for my own use rather than production grade code. It falls back to text types for anything it doesn't understand. Patches or pull requests welcome.

```shell
csvimport --clean my_file.csv
```
