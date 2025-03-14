#!/usr/bin/env python3
"""
PubMed Main Processing Script

This script processes PubMed JSON files to extract biomedical entities and relations,
generating knowledge graphs that can be visualized and analyzed.
"""

import os
import sys
import json
import time
import argparse
from typing import Dict, List, Any, Tuple

# 确保模块导入路径正确 - Windows路径处理
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_dir)

# Import project modules
from config import ENTITY_TYPES, RELATION_TYPES, OUTPUT_FORMAT, OUTPUT_DIR
from utils.pubmed_processor import PubmedProcessor
from utils.text_processor import TextProcessor
from extractor.entity_extractor import EntityExtractor
from extractor.relation_extractor import RelationExtractor
from utils.output_formatter import OutputFormatter


def process_pubmed_file(file_path: str, output_format: str = OUTPUT_FORMAT, 
                        output_dir: str = OUTPUT_DIR, verbose: bool = False) -> Dict[str, Any]:
    """
    Process a single PubMed JSON file, extracting entities and relations
    
    Args:
        file_path: Input PubMed JSON file path
        output_format: Output format
        output_dir: Output directory
        verbose: Whether to print verbose output
        
    Returns:
        Dictionary containing processing results
    """
    print(f"Starting to process PubMed file: {file_path}")
    start_time = time.time()
    
    # Read and process PubMed file
    print("Reading and parsing PubMed JSON file...")
    text, metadata_list = PubmedProcessor.process_pubmed_file(file_path)
    
    if not text:
        print(f"Error: Could not extract text content from file: {file_path}")
        return {"error": "Could not extract text content"}
    
    # Pre-process text
    print("Pre-processing text...")
    text_processor = TextProcessor()
    chunks = text_processor.split_text_into_chunks(text)
    
    print(f"Split text into {len(chunks)} chunks")
    
    # Extract entities
    print("Extracting entities...")
    entity_extractor = EntityExtractor(allowed_types=ENTITY_TYPES)
    entities = {}
    
    for chunk_idx, chunk in enumerate(chunks):
        print(f"Processing chunk {chunk_idx+1}/{len(chunks)} for entity extraction...")
        chunk_entities = entity_extractor.extract_entities(chunk)
        
        # Merge entities from this chunk
        for entity_type, entity_list in chunk_entities.items():
            if entity_type not in entities:
                entities[entity_type] = []
            
            # Add unique entities
            for entity in entity_list:
                if entity not in entities[entity_type]:
                    entities[entity_type].append(entity)
    
    # Count entities
    total_entities = sum(len(entities.get(entity_type, [])) for entity_type in ENTITY_TYPES)
    print(f"Extracted {total_entities} entities across {len(entities)} categories")
    
    # Extract relations
    print("Extracting relations...")
    relation_extractor = RelationExtractor(allowed_relation_types=RELATION_TYPES)
    relations = []
    
    for chunk_idx, chunk in enumerate(chunks):
        print(f"Processing chunk {chunk_idx+1}/{len(chunks)} for relation extraction...")
        chunk_relations = relation_extractor.extract_relations(chunk, entities)
        
        # Add unique relations
        for relation in chunk_relations:
            if relation not in relations:
                relations.append(relation)
    
    print(f"Extracted {len(relations)} relations")
    
    # Generate output directory based on input filename - Windows路径处理
    file_name = os.path.basename(file_path)
    file_base = os.path.splitext(file_name)[0]
    output_path = os.path.join(output_dir, file_base)
    
    # For debugging, save the raw entities and relations
    if verbose:
        os.makedirs(output_path, exist_ok=True)
        # Windows使用utf-8编码保存JSON
        with open(os.path.join(output_path, "raw_entities.json"), "w", encoding="utf-8") as f:
            json.dump(entities, f, ensure_ascii=False, indent=2)
        with open(os.path.join(output_path, "raw_relations.json"), "w", encoding="utf-8") as f:
            json.dump(relations, f, ensure_ascii=False, indent=2)
        print("Raw entities and relations saved to output directory")
    
    # Format output
    print("Formatting output...")
    formatter = OutputFormatter()
    result = formatter.format_output(entities, relations, metadata_list, output_format, output_path)
    
    # Summarize results
    processing_time = time.time() - start_time
    print(f"Processing completed in {processing_time:.2f} seconds")
    print(f"Results saved to: {output_path}")
    
    return {
        "entities": entities,
        "relations": relations,
        "metadata": metadata_list,
        "output_format": output_format,
        "output_path": output_path,
        "processing_time": processing_time
    }


def main():
    """Main entry point"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process PubMed JSON files for biomedical knowledge graph extraction')
    parser.add_argument('input', help='Input PubMed JSON file')
    parser.add_argument('--output', '-o', default=OUTPUT_DIR, help='Output directory')
    parser.add_argument('--format', '-f', default=OUTPUT_FORMAT, choices=['json', 'csv', 'rdf'], 
                        help='Output format: json, csv, or rdf')
    parser.add_argument('--verbose', '-v', action='store_true', help='Display verbose output')
    
    args = parser.parse_args()
    
    # Process the file
    process_pubmed_file(
        file_path=args.input,
        output_format=args.format,
        output_dir=args.output,
        verbose=args.verbose
    )

if __name__ == "__main__":
    main()