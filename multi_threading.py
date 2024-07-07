from threading import Thread, Lock
import logging
from pathlib import Path
import time
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="Multithreading keyword search in files")
    parser.add_argument("--source", "-s", required=True, help="Source folder")
    parser.add_argument("--keywords", "-k", required=True, nargs='+', help="Keywords to search for")
    parser.add_argument("--threads", "-t", type=int, default=4, help="Number of threads")
    return vars(parser.parse_args())

def worker(files, keywords, results, lock):
    for file in files:
        logging.debug('Worker starts searching')
        try:
            with open(file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read().lower()
                for keyword in keywords:
                    if keyword in content:
                        with lock:
                            if keyword in results:
                                results[keyword].append(file)
                            else:
                                results[keyword] = [file]
        except Exception as e:
            logging.error(f"Error reading file {file}: {e}")
        logging.debug('Worker ended searching')

def main_threading():
    logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')
    logging.debug('Start program')
    start_time = time.time()
    
    results = {}
    lock = Lock()
    all_files = [str(f) for f in Path(source).rglob('*') if f.is_file()]
    
    files_per_thread = [[] for _ in range(num_threads)]
    for i, file in enumerate(all_files):
        files_per_thread[i % num_threads].append(file)
    
    threads = []
    
    for files in files_per_thread:
        thread = Thread(target=worker, args=(files, keywords, results, lock))
        thread.start()
        threads.append(thread)
    
    for thread in threads:
        thread.join()

    end_time = time.time()
    logging.debug('End program')
    logging.debug(f"Multithreading execution time: {end_time - start_time:.6f} seconds")
    print(results)

if __name__ == '__main__':
    args = parse_arguments()
    source = args.get("source")
    keywords = [keyword.lower() for keyword in args.get("keywords")]
    num_threads = args.get("threads")   

    main_threading()
