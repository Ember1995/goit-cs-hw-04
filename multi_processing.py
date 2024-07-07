from multiprocessing import Queue, Process, current_process
import logging
from pathlib import Path
import time
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="Multiprocessing keyword search in files")
    parser.add_argument("--source", "-s", required=True, help="Source folder")
    parser.add_argument("--keywords", "-k", required=True, nargs='+', help="Keywords to search for")
    parser.add_argument("--processes", "-p", type=int, default=4, help="Number of processes")
    return vars(parser.parse_args())

logger = logging.getLogger()
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)

def worker(task_queue, result_queue, keywords):
    name = current_process().name
    while not task_queue.empty():
        try:
            file = task_queue.get_nowait()
        except Exception as e:
            break

        results = {}
        logger.debug(f'{name} starts searching in {file}')
        try:
            with open(file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read().lower()
                for keyword in keywords:
                    if keyword in content:
                        if keyword in results:
                            results[keyword].append(file)
                        else:
                            results[keyword] = [file]
        except Exception as e:
            logger.error(f"Error reading file {file}: {e}")
        logger.debug(f'{name} ended searching in {file}')
        
        result_queue.put(results)
    logger.debug(f'{name} finished')

def main_multiprocessing():
    logger.debug('Start program')
    start_time = time.time()

    all_files = [str(f) for f in Path(source).rglob('*') if f.is_file()]

    task_queue = Queue()
    result_queue = Queue()

    for file in all_files:
        task_queue.put(file)

    processes = []
    for _ in range(num_processes):
        process = Process(target=worker, args=(task_queue, result_queue, keywords))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    results = {}
    while not result_queue.empty():
        result = result_queue.get()
        for keyword, files in result.items():
            if keyword in results:
                results[keyword].extend(files)
            else:
                results[keyword] = files

    end_time = time.time()
    logger.debug('End program')
    logger.debug(f"Multiprocessing execution time: {end_time - start_time:.6f} seconds")
    print(results)

if __name__ == '__main__':
    args = parse_arguments()
    source = args.get("source")
    keywords = [keyword.lower() for keyword in args.get("keywords")]
    num_processes = args.get("processes")

    main_multiprocessing()
