# Run the program:
### Parameters:
    --gzip_filename : Input file of generated data.
    --reportfile    : Name of csv file for final output.
    --line_limit    : Approximate number of lines per intermediary file.

- All the parameters have default values that can be changed inside the script.
- By simply invoking the name of the script, it will run with the following values:
```python
--gzip_filename 'data_1.csv.gz'
--reportfile 'report.csv'
--line_limit 1000
```
- A `line_limit` of 1000 tends to keep RAM usage at about 4 Mb once the scale of the data file reaches 3.

### Automation:
As long as the script can run and create the intermediary files in the same directory, it should be easy to automate with any scheduling tool.  An Azure Runbook would be ideal because (I think) they natively support Python scripts. 

# General Workflow:

 - **create_subfiles** function loops through the input file line by line copying each one
   to a new file until the line_limit parameter is reached.  It includes logic so that a
   basket is not divided up between multiple files.  All the lines for any given basket
   are contained in one file.

 - **proc_baskets** function transforms each fragment file from the input format into the
   output format of distinct (Product_1, Product_2) tuples found in each basket.
   This function returns a dictionary of every occurence of a tuple found in a fragment file.

 - **count_product_tuples** function takes the aforementioned dictionary and sums the tuples
   together, reducing the number of lines to be written out to more intermediary files that
   are chunks for the final report file.

 - **proc_subdata** finally manipulates the data into the report.
   For each fragment file of tuple counts (primary), each one is read in sequential order and compared
   to every other fragment file (secondary).   
     - Tuples from secondary files that match to tuples in the primary file are summed together in the primary
     file and removed from the secondary file.  This ensures the same matching tuple does not get counted 
     more than once.
     - Once the primary file has been compared for matching tuples against all available secondaries, the updated
     primary content (DataFrame) is appended to the final report file.
     - The primary file loop then moves on to the next tuple counts file.  This file was the first "secondary" compared 
     in the previous loop, so any tuples that matched to the first primary loop have already been removed from
     this file.  At this point, this file, now promoted from secondary to primary, represents a new set of distinct tuples
     that do not yet exist in the final report file.
     **NOTE:**  If a secondary file becomes empty before it's turn as a primary, it will just be skipped over as we can
     assume that all of it's original contents have been accounted for.

# MEMORY CONSTRAINTS:
 - Splitting the gzip file by reading line-by-line uses almost no memory.
 - The processing of baskets into product tuples relies on dictionaries and a csv reader which also works on a 
   line-by-line file read.  The memory use of the dictionaries is controlled by the line_limit parameter, so a fixed line_limit can help keep memory usage down even if the file size continues to grow.  The script will take longer, but memory should stay about the same.
 - The bulk of memory comes from the use of DataFrames for the consolidating and file comparisons. The `line_limit` parameter affects RAM usage here as well, but this is a good area for improvement if DataFrames can be substituted for more advanced use of other data structures, or perhaps a file based database like Sqlite. 

# Miscellaneous Thoughts:
- I wish I had experimented with Sqlite to see if it can query without loading entire tables into memory; Google results are vague on that point. This project would have been radically simpler to implement if I'd thought of that earlier.
- Expanding the tuples to three or more members would have a big impact on the run time more than anything else.  Having said that, I do think that might be the breaking point to do away with the Pandas DataFrames in favor of dictionaries or something more fundamental.  More code steps, but easier to control memory usage.  It would allow for larger `line_limit` settings, in theory, that could help the run time.