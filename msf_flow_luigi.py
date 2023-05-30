import os, sys
import argparse
import re
from csv import DictReader, DictWriter
import itertools
from collections import OrderedDict
import logging
from wind_processor.running_windspeed \
    import compute_wind_stats, compute_emission_rate
from wind_processor.wind_type import WindType
from utils.dir_watcher import DirWatcher
from utils.logger import init_logger
from shutil import copyfile

def parse_args(default_minppmm=1000):
    """Retrieve command line parameters.
    
    Returns:
        ArgumentParse: command line parameters
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-p", "--plumedir", required=False,
                        help="path to input plume file directory")
    parser.add_argument("-r", "--regex", required=False,
                        default="ang.*_detections/ime_minppmm{}/ang.*_ime_minppmm{}.*".format(default_minppmm, default_minppmm),
                        help="Regular expression to match for plume files")
    parser.add_argument("-i", "--infile", required=False)
    parser.add_argument("-w", "--windir", required=True,
                        help="path to input wind file directory")
    parser.add_argument("-o", "--outfile", required=True,
                        help="path to output plume list")
    parser.add_argument("-f", "--force", help="Force reprocessing of all files (not just the new ones)", action='store_true')
    args = parser.parse_args()
    #return args.plumedir, args.regex, args.windir, args.outfile, args.force
    return args.infile, args.windir, args.outfile, args.force

def process_plume(plume, winds_dir, fill=None, logger=None):
    if logger is not None:
        logger.warning("processing plume {}".format(plume))
        logger.info(plume)
    emission_stats = OrderedDict()
    for subdir in sorted([f for f in os.listdir(winds_dir)
                          if not f.startswith('.')]):
        if logger is not None:
            logger.info("processing for winds in {}".format(subdir))
        wt = WindType(subdir)
        if not wt.is_unknown():
            wind_type = wt.type_as_str()
            winds_subdir = os.path.join(winds_dir, subdir)
            alts = sorted(wt.alts())
            for alt in alts:
                wind_stats = compute_wind_stats(plume, winds_subdir,
                                                fill=fill, wind_type=wind_type,
                                                wind_alt=alt, logger=logger)
                plume.update(wind_stats)
            emission_stats.update(compute_emission_rate(plume, wind_type,
                                                        fill=fill,
                                                        logger=logger))
    plume.update(emission_stats)
    if logger is not None:
        logger.info("Computed plume={}".format(plume))
    return plume
    
def get_minppmm_from_fname(fname):
    regex_str = "minppmm(\d+)"
    pattern = re.compile(regex_str)
    m = pattern.search(fname)
    ##m=1000
    if m is None:
        raise ValueError("No match for {} found in {}".format(regex_str, fname))
    return int(m[1])##(m)

def dict_reader_plus_update(fname, d):
#    try:
#        with open(fname, 'r') as f:
#            reader = DictReader(f, skipinitialspace=True)
#            # The dictionary "update" method returns None, but I need the updated
#            # dictionary in each row.  I am using the "or" operator below to
#            # get that.  This works because "None or X" returns X.
#            out = [row.update(d) or row for row in reader]
#            return out
#    except OSError:
#        print("Could not find file:",fname)
#        return
    with open(fname, 'r') as f:
        reader = DictReader(f, skipinitialspace=True)
        # The dictionary "update" method returns None, but I need the updated
        # dictionary in each row.  I am using the "or" operator below to
        # get that.  This works because "None or X" returns X.
        out = [row.update(d) or row for row in reader]
    return out

def process_plumes(flist, winds_dir, fill=None,
                   minppmm_key="Minimum Threshold (ppmm)", logger=None):
    # Read all of the plumes from all of the files, add minimum PPMM threshold
    # field to each plume (threshold value extracted from file name), and sort
    # the plumes by "Candidate ID".
    if logger is not None:
        logger.info("flist={}".format(flist))
    plumes = list(itertools.chain.from_iterable(
        [dict_reader_plus_update(fname,
                                 {minppmm_key: get_minppmm_from_fname(fname)})
         for fname in flist]))

    # Get plume keys (field header names) from the first plume. Sort plume
    # according to the values for the first key (expected to be Candidate ID).
    sort_by_key = list(plumes[0].keys())[0]
    plumes = sorted(plumes, key=lambda d: d[sort_by_key])
             
    if logger is not None:
        logger.info("plumes={}".format(list(plumes)))
    plumes_ext = [process_plume(plume, winds_dir, fill=fill, logger=logger)
                  for plume in plumes]
    if logger is not None:
        logger.info("Computed plumes={}".format(plumes_ext))
    return plumes_ext

def insert_plumes_in_file(plumes, fname, sort_by_key=None, logger=None):
    if len(plumes) > 0:
        # If output file already exists, insert new plumes into it
        # in sorted order (sorted by first column).
        if os.path.isfile(fname):
            with open(fname, 'r') as fin:
                reader = DictReader(fin)
                plumes = list(reader) + plumes

            # Make backup copy of original plume file
            fname_bak = fname + '.bak'
            copyfile(fname, fname_bak)
            if logger is not None:
                logger.critical("Original plume file backed up to {}".
                                format(fname_bak))

        # Sort plume list if sort_by_key is provided
        if sort_by_key is not None:
            if sort_by_key in plumes[0]:
                plumes = sorted(plumes, key=lambda d: d[sort_by_key])
            else:
                if logger is not None:
                    logger.warning("Sort key {} not found.".
                                   format(sort_by_key))
                    logger.warning("Plumes left unsorted.")

        # Get field names from the first plume
        field_names = list(plumes[0].keys())

        # Write processed plumes to output file
        with open(fname, 'w') as fout:
            writer = DictWriter(fout, fieldnames=field_names)
            writer.writeheader()
            for plume in plumes:
                try:
                    writer.writerow(plume)
                except:
                    if logger is not None:
                        logger.warning("Could not write plume: {}".
                                       format(plume))
                        logger.warning("Check that the plume fields match the header in {}".format(fname))
        if logger is not None:
            logger.critical("Extended plume file written to {}".
                            format(fname))

    else:
        if logger is not None:
            logger.warning("Skipped insertion because plume list was empty")

def main():
    # Initializer logger
    logger = init_logger()

    # Parse command line arguments.
#    plume_dir, plume_file_regex, plume_file, winds_dir, out_fname, force = parse_args()
    plume_file, winds_dir, out_fname, force = parse_args()

    # If new files are found, process each plume in the files.
    if plume_file is not None:
        # Process each plume.  We compute extra values for each plume
        # including wind statistics and emission rate and uncertainty.
        try:
            #import pdb;pdb.set_trace()
            plumes = process_plumes([plume_file], winds_dir, fill=-9999,
                                    logger=logger)
            #import pdb;pdb.set_trace()
            # Insert new plumes in to output file.
            insert_plumes_in_file(plumes, out_fname,
                                  sort_by_key="Candidate ID", logger=logger)
        except OSError:
            print("Could not find file:",plume_file)
            return
    else:
        logger.warning("Nothing to do")

    return    
        
if __name__ == "__main__":
    main()
