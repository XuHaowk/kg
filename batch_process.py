#!/usr/bin/env python3
"""
PubMed Batch Processing Script

This script processes multiple PubMed JSON files to extract biomedical entities and relations,
generating knowledge graphs for each file. It supports parallel processing to improve efficiency
when handling large numbers of files.
"""

import os
import sys
import glob
import argparse
import time
import json
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any

# 确保模块导入路径正确 - Windows路径处理
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_dir)

# Import the main processing function from pubmed_main
from pubmed_main import process_pubmed_file

def process_batch(input_files: List[str], output_dir: str, output_format: str = 'json', 
                 parallel: bool = False, max_workers: int = 4, verbose: bool = False) -> Dict[str, Any]:
    """
    Process a batch of PubMed JSON files, extracting entities and relations

    Args:
        input_files: List of input PubMed JSON file paths
        output_dir: Base output directory
        output_format: Output format (json, csv, rdf)
        parallel: Whether to use parallel processing
        max_workers: Maximum number of parallel workers
        verbose: Whether to print verbose output

    Returns:
        Dictionary containing processing results
    """
    total_start_time = time.time()
    print(f"Starting batch processing of {len(input_files)} files...")
    
    # Create timestamp for this batch run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_output_dir = os.path.join(output_dir, f"batch_run_{timestamp}")
    os.makedirs(batch_output_dir, exist_ok=True)
    
    # Store results for each file
    results = {}
    
    if parallel and len(input_files) > 1:
        # 并行处理 - Windows多进程支持
        print(f"Using parallel processing with {max_workers} workers")
        # 使用ProcessPoolExecutor，适用于Windows
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Create a dictionary that maps each future to its corresponding file
            future_to_file = {
                executor.submit(process_pubmed_file, 
                               file_path, 
                               output_format, 
                               os.path.join(batch_output_dir, os.path.basename(file_path).split('.')[0]),
                               verbose): file_path 
                for file_path in input_files
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    results[file_path] = result
                    print(f"Completed processing: {file_path}")
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
                    results[file_path] = {"error": str(e)}
    else:
        # Sequential processing
        for file_path in input_files:
            print(f"Processing file: {file_path}")
            try:
                file_output_dir = os.path.join(batch_output_dir, os.path.basename(file_path).split('.')[0])
                result = process_pubmed_file(file_path, output_format, file_output_dir, verbose)
                results[file_path] = result
                print(f"Completed processing: {file_path}")
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
                results[file_path] = {"error": str(e)}
    
    # Create summary report
    summary = {
        "batch_run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_files": len(input_files),
        "successful_files": sum(1 for r in results.values() if "error" not in r),
        "failed_files": sum(1 for r in results.values() if "error" in r),
        "total_entities": sum(sum(len(r.get("entities", {}).get(entity_type, [])) 
                              for entity_type in r.get("entities", {})) 
                            for r in results.values() if "entities" in r),
        "total_relations": sum(len(r.get("relations", [])) for r in results.values() if "relations" in r),
        "processing_time": time.time() - total_start_time
    }
    
    # 保存摘要，Windows编码处理
    summary_path = os.path.join(batch_output_dir, "batch_summary.json")
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump({
            "summary": summary,
            "file_details": {os.path.basename(file_path): 
                           {"status": "error" if "error" in result else "success",
                            "entity_count": sum(len(result.get("entities", {}).get(entity_type, [])) 
                                            for entity_type in result.get("entities", {})) 
                                            if "entities" in result else 0,
                            "relation_count": len(result.get("relations", [])) 
                                            if "relations" in result else 0,
                            "error": result.get("error", "") if "error" in result else ""}
                           for file_path, result in results.items()}
        }, f, ensure_ascii=False, indent=2)
    
    print("\nBatch processing summary:")
    print(f"Total files processed: {len(input_files)}")
    print(f"Successful: {summary['successful_files']}, Failed: {summary['failed_files']}")
    print(f"Total entities extracted: {summary['total_entities']}")
    print(f"Total relations extracted: {summary['total_relations']}")
    print(f"Total processing time: {summary['processing_time']:.2f} seconds")
    print(f"Results saved to: {batch_output_dir}")
    
    return {
        "summary": summary,
        "results": results,
        "output_dir": batch_output_dir
    }

def main():
    """Main entry point for batch processing"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='PubMed batch file processing for biomedical knowledge graph extraction')
    parser.add_argument('input', help='Input directory containing PubMed JSON files or glob pattern')
    parser.add_argument('--output', '-o', default='data/batch_output', help='Output directory for batch results')
    parser.add_argument('--format', '-f', default='json', choices=['json', 'csv', 'rdf'], 
                        help='Output format: json, csv, or rdf')
    parser.add_argument('--pattern', '-p', default='pubmed_results_batch_*.json', 
                        help='File pattern for JSON files to process (default: pubmed_results_batch_*.json)')
    parser.add_argument('--parallel', action='store_true', help='Use parallel processing')
    parser.add_argument('--workers', '-w', type=int, default=4, 
                        help='Maximum number of parallel workers (default: 4)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Display verbose output')
    
    args = parser.parse_args()
    
    # Determine input files - Windows路径处理
    if os.path.isdir(args.input):
        # Input is a directory, use pattern to find files
        input_path = os.path.join(args.input, args.pattern)
        input_files = glob.glob(input_path)
    else:
        # Input might be a glob pattern itself
        input_files = glob.glob(args.input)
    
    if not input_files:
        print(f"Error: No files matching pattern '{args.pattern}' found in '{args.input}'")
        return
    
    print(f"Found {len(input_files)} files to process")
    
    # Process the batch
    process_batch(
        input_files=input_files,
        output_dir=args.output,
        output_format=args.format,
        parallel=args.parallel,
        max_workers=args.workers,
        verbose=args.verbose
    )

if __name__ == "__main__":
    main()