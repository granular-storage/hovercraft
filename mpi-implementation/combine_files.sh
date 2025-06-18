#!/bin/bash

# Script to combine all files in a directory into one single file
# Usage: ./combine_files.sh [directory] [output_file]

# Default values
DIRECTORY=${1:-.}  # Current directory if not specified
OUTPUT_FILE=${2:-combined_output.txt}  # Default output filename

# Check if directory exists
if [ ! -d "$DIRECTORY" ]; then
    echo "Error: Directory '$DIRECTORY' does not exist."
    exit 1
fi

# Remove output file if it already exists
if [ -f "$OUTPUT_FILE" ]; then
    rm "$OUTPUT_FILE"
fi

echo "Combining files from directory: $DIRECTORY"
echo "Output file: $OUTPUT_FILE"
echo "----------------------------------------"

# Counter for files processed
file_count=0

# Loop through all files (not directories) in the specified directory
for file in "$DIRECTORY"/*; do
    # Skip if it's a directory or doesn't exist
    if [ -d "$file" ] || [ ! -e "$file" ]; then
        continue
    fi
    
    # Get just the filename without path
    filename=$(basename "$file")
    
    # Add separator and filename header to output
    echo "" >> "$OUTPUT_FILE"
    echo "=== FILE: $filename ===" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    
    # Append file contents
    cat "$file" >> "$OUTPUT_FILE"
    
    # Add separator after file contents
    echo "" >> "$OUTPUT_FILE"
    echo "=== END OF $filename ===" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    
    ((file_count++))
    echo "Processed: $filename"
done

echo "----------------------------------------"
echo "Total files processed: $file_count"
echo "Combined output saved to: $OUTPUT_FILE"
