from cnki_crawler import CNKIWrapper
import os
import sys

def main():
    # Get command line arguments or use defaults
    keyword = sys.argv[1] if len(sys.argv) > 1 else "矽肺"
    max_results = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    
    # Create wrapper with appropriate output directory
    output_dir = os.path.join("E:", "kg_副本", "results", "cnki_data")
    os.makedirs(output_dir, exist_ok=True)
    wrapper = CNKIWrapper(output_dir=output_dir)
    
    # Perform search
    result = wrapper.search_cnki(
        term=keyword,
        max_results=max_results
    )
    
    print(f"Search completed. Found {result.get('count', 0)} results.")
    print(f"Results saved to {result.get('csv_path', 'N/A')}")

if __name__ == "__main__":
    main()