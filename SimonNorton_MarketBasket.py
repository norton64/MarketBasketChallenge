#!/usr/env python

# User input parameters:
#    --gzip_filename : Input file of generated data.
#    --reportfile    : Name of csv file for final output.
#    --line_limit    : Approximate number of lines per intermediary file.

# General Workflow:
#
# - "create_subfiles" function loops through the input file line by line copying each one
#   to a new file until the line_limit parameter is reached.  It includes logic so that a
#   basket is not divided up between multiple files.  All the lines for any given basket
#   are contained in one file.
#
# - "proc_baskets" function transforms each fragment file from the input format into the
#   output format of distinct (Product_1, Product_2) tuples found in each basket.
#   This function returns a dictionary of every occurence of a tuple found in a fragment file.
#
# - "count_product_tuples" function takes the aforementioned dictionary and sums the tuples
#   together, reducing the number of lines to be written out to more intermediary files that
#   are chunks for the final report file.
#
# - "proc_subdata" finally manipulates the data into the report.
#   For each fragment file of tuple counts (primary), each one is read in sequential order and compared
#   to every other fragment file (secondary).   
#   - Tuples from secondary files that match to tuples in the primary file are summed together in the primary
#     file and removed from the secondary file.  This ensures the same matching tuple does not get counted 
#     more than once.
#   - Once the primary file has been compared for matching tuples against all available secondaries, the updated
#     primary content (DataFrame) is appended to the final report file.
#   - The primary file loop then moves on to the next tuple counts file.  This file was the first "secondary" compared 
#     in the previous loop, so any tuples that matched to the first primary loop have already been removed from
#     this file.  At this point, this file, now promoted from secondary to primary, represents a new set of distinct tuples
#     that do not yet exist in the final report file.
#     NOTE:  If a secondary file becomes empty before it's turn as a primary, it will just be skipped over as we can
#     assume that all of it's original contents have been accounted for.
#
# MEMORY CONSTRAINTS:
# - Splitting the gzip file by reading line-by-line uses almost no memory.
# - The processing of baskets into product tuples relies on dictionaries and a csv reader which also works on a 
#   line-by-line file read.  The memory use of the dictionaries is controlled by the line_limit parameter, so a fixed
#   line_limit can help keep memory usage down even if the file size continues to grow.  The script will take longer,
#   but memory should stay about the same.
# - The bulk of memory comes from the use of DataFrames for the consolidating and file comparisons.  The line_limit parameter
#   affects RAM usage here as well, but this is a good area for improvement if DataFrames can be substituted for more advanced
#   use of other data structures, or perhaps a file based database like Sqlite.   




import os
import psutil as psu
import glob
import csv
import gzip
import argparse
import collections as cll
import itertools as itl
import pandas as pd
import gc
import time 

MiB = 1024**2 # MebiBytes or MegaBytes multiplier constant

def show_mem():
    this_proc = psu.Process(os.getpid())
    MiBytes_used = this_proc.memory_info().rss / MiB

    return MiBytes_used



def create_subfiles(gzip_filename, line_limit):
    print (f'Create subfiles of {gzip_filename} of approximately {line_limit} lines each.')
    try:
        subfile_counter = 1

        # clear any old subdata files from previous runs
        for oldsubdata in glob.glob('subdata*.csv'):
            os.remove(oldsubdata)

        with gzip.open(gzip_filename, 'rt', encoding='utf8') as gzip_f:
            current_line = gzip_f.readline()

            while current_line:
                subfile = 'subdata_' + str(subfile_counter).rjust(3,'0') + '.csv'
                out_f = open(subfile, 'at')
                line_counter = 0

                while line_counter <= line_limit:
                    # print(f'start line_counter <= line_Limit: line_counter: {line_counter},  line_limit: {line_limit}')  # DEBUG statement
                    out_f.write(current_line)
                    writ_basket = current_line # Basket just written to subdata file
                    current_line = gzip_f.readline() # Next basket in line to be written
                    if current_line == '':
                        print(f'No more lines in gzip file to process.')
                        break
                    # print(f'Curline#: {line_counter} counter: {line_limit}   just wrote: {writ_basket},  Just read: {current_line}')  # DEBUG statement

                    # keep baskets in the same file by *not incrementing* if the next basket past the line_limit is the same as the last basket processed.
                    if (line_counter) == line_limit and current_line == writ_basket: 
                        continue
                    else: 
                        line_counter += 1   
                    # print(f'About to write Subfile: {subfile}    : current_line: {current_line}')  # DEBUG statement
                    # print(f'Mem usage during line reads: {round(show_mem(),4)}')  # Reality check that line reads doesn't temporarily inflate mem usage.
                out_f.close()
                subfile_counter +=1
            gzip_f.close()   
        gc.collect()

    except Exception as err:
        print(f'Error: {err}')
        return False
    return True    



def proc_baskets(subfile):
    #print (f'Return dictionary of baskets from input file {subfile} with tuples of products.')
    basket_dict = {}
    file_of_baskets = {}

    with open(subfile, 'rt') as f:
        csv_f = csv.reader(f)

        # Create dictionary of baskets, making the products into a list.
        for row in csv_f:
            if row[0] not in basket_dict.keys():
                basket_dict.update({row[0] : row[1:]})
            else:
                basket_dict[row[0]].extend(row[1:])

    # sort and deduplicate the basket contents
    deduped = {key : sorted(list(set(basket_dict[key]))) for key in sorted(basket_dict)}
    # print(f'basket_dict\n {basket_dict}') # DEBUG statement
    # print(f'deduped\n {deduped}') # DEBUG statement

    # Reorganize basket contents into distinct tuples
    for basket, contents in deduped.items():
        basket_tuples = {
            basket : sorted([(e1, e2) for e1 in contents
                                        for e2 in contents if (e1 < e2)])
        }

        # print(basket_tuples)
        file_of_baskets.update(basket_tuples)

    gc.collect()
    return file_of_baskets

     

def count_product_tuples(subfile, file_of_baskets):
    #print(f'Calculating files of tuple results.')

    tuple_counts_for_subfile = dict(cll.Counter((itl.chain.from_iterable(file_of_baskets.values()))))

    summed_tuples = cll.defaultdict(int) # Creates the empty dictionary in the right format to sum the values.

    for k,v in tuple_counts_for_subfile.items():
        summed_tuples[k] += v

    count_file = subfile.replace('.csv','_tuplecount.csv')

    with open(count_file,'wt') as out_f:
        out_csv = csv.writer(out_f)        
        for row in summed_tuples.items():
            output_format = []
            # Format row for csv file - turn the tuples back into lists for final output.
            output_format.append(list(list(row)[0])[0])
            output_format.append(list(list(row)[0])[1]) 
            output_format.append(list(row)[1])

            out_csv.writerow(output_format)
    
    gc.collect()



def proc_subdata(output_filename):
    print("Consolidating results and creating output file.")
    subdata_list = glob.glob('subdata*tuplecount.csv')
    subdata_list.sort()

    for proc_file in subdata_list:
        print(f'Processing proc_file: {proc_file}')
        if os.path.getsize(proc_file) == 0:
            #print(f'{proc_file} has been emptied out during processing, so we can skip.')
            continue
        proc_df = pd.read_csv(proc_file, header=None)
        proc_df.columns=['Product_1', 'Product_2', 'num_baskets']

        upd_df = pd.DataFrame(columns=['Product_1', 'Product_2', 'num_baskets'])

        proc_range = list(range(subdata_list.index(proc_file) + 1, len(subdata_list)))

        for next_idx in proc_range:
            comp_file = subdata_list[next_idx]
            print(f'    Comparing to: {comp_file}')

            if os.path.getsize(comp_file) == 0:
                # comparison file has been emptied out already, so we can skip it here too.
                continue

            comp_df = pd.read_csv(comp_file, header=None)
            comp_df.columns=['Product_1', 'Product_2', 'num_baskets']

            mrg_df = pd.merge(proc_df, comp_df, on=['Product_1', 'Product_2'])

            if mrg_df.empty:
                del comp_df
                del mrg_df
                continue

            upd_df = mrg_df.assign(num_baskets=mrg_df[['num_baskets_x', 'num_baskets_y']].sum(1)).drop(['num_baskets_x', 'num_baskets_y'], axis=1)
            del mrg_df

            proc_df = pd.merge(proc_df, upd_df, on=['Product_1', 'Product_2'], how='left')

            proc_df = proc_df.assign(num_baskets=proc_df[['num_baskets_x', 'num_baskets_y']].max(1)).drop(['num_baskets_x', 'num_baskets_y'], axis=1)
            proc_df = proc_df.astype({'num_baskets': int})

            # delete merged row from subfile so it doesn't get counted again.
            comp_df = pd.merge(comp_df, upd_df, on=['Product_1', 'Product_2'], how='left')
            comp_drop_idx = (comp_df['num_baskets_y'] > 0).index
            comp_df.drop(comp_drop_idx, inplace=True)

            comp_df = comp_df.assign(num_baskets=comp_df[['num_baskets_x', 'num_baskets_y']].max(1)).drop(['num_baskets_x', 'num_baskets_y'], axis=1)
            

            if upd_df.empty == False:
                comp_df.to_csv(comp_file, mode='wt', header=None, index=False)
            del comp_df

        if os.path.isfile(output_filename) == False:
            proc_df.to_csv(output_filename, index=False)
        else:
            proc_df.to_csv(output_filename, mode='at', header=None, index=False)
    gc.collect()

        

if __name__ == '__main__':
    time_at_start = time.time()
    mem_at_start = round(show_mem(),4)
    # print(f'MiBs in use at start: {mem_at_start}')

    parser = argparse.ArgumentParser(description='Gzip filename to process')
    parser.add_argument('--gzip_filename', default='data_1.csv.gz', type=str )
    parser.add_argument('--reportfile', default='report.csv', type=str)
    parser.add_argument('--line_limit', default=1000, type=int)
    args = parser.parse_args()

    print(f'Processing gzip file: {args.gzip_filename}')

    create_subfiles(args.gzip_filename, args.line_limit)

    for subdatafile in glob.glob('subdata*.csv'):
        basket_out = proc_baskets(subdatafile)
        count_product_tuples(subdatafile, basket_out)

    proc_subdata(args.reportfile)

    for oldsubdata in glob.glob('subdata*.csv'):
        os.remove(oldsubdata)

    mem_at_end = round(show_mem(),4) 
    time_at_end = time.time() 
    # print(f'MiBs in use at end: {mem_at_end}')

    print(f'Total MiBs used: {mem_at_end - mem_at_start:.4f}')
    print(f'Run time: {time_at_end - time_at_start} seconds.')
