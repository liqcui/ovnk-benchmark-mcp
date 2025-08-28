#!/usr/bin/env python3
"""
OCP Benchmark JSON Extract and Round Decimals
Processes JSON data and rounds all decimal values to 2 decimal places
"""

import json
import argparse
import sys
from typing import Any, Dict, List, Union
from decimal import Decimal, ROUND_HALF_UP
import os


class JSONDecimalRounder:
    """Class to process JSON and round all decimal values to specified precision"""
    
    def __init__(self, decimal_places: int = 2):
        """
        Initialize with decimal precision
        
        Args:
            decimal_places: Number of decimal places to round to (default: 2)
        """
        self.decimal_places = decimal_places
        self.quantizer = Decimal('0.01') if decimal_places == 2 else Decimal(10) ** -decimal_places
    
    def round_value(self, value: Any) -> Any:
        """
        Round a single value if it's a float/decimal
        
        Args:
            value: Value to potentially round
            
        Returns:
            Rounded value or original value if not numeric
        """
        if isinstance(value, (int, float)):
            if isinstance(value, int):
                return value  # Don't modify integers
            
            # Handle special float values
            if value != value:  # NaN check
                return None
            if value == float('inf') or value == float('-inf'):
                return value
            
            # Round to specified decimal places
            decimal_value = Decimal(str(value))
            rounded = decimal_value.quantize(self.quantizer, rounding=ROUND_HALF_UP)
            return float(rounded)
        
        return value
    
    def process_data(self, data: Any) -> Any:
        """
        Recursively process data structure and round all decimal values
        
        Args:
            data: Data structure to process (dict, list, or primitive)
            
        Returns:
            Processed data structure with rounded decimals
        """
        if isinstance(data, dict):
            return {key: self.process_data(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.process_data(item) for item in data]
        else:
            return self.round_value(data)
    
    def extract_and_round_json(self, input_data: Union[str, Dict, List]) -> Dict[str, Any]:
        """
        Extract and round JSON data
        
        Args:
            input_data: JSON string, dictionary, or list
            
        Returns:
            Processed dictionary with rounded decimals
        """
        # Parse JSON string if needed
        if isinstance(input_data, str):
            try:
                data = json.loads(input_data)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON string: {e}")
        else:
            data = input_data
        
        # Process and round all decimal values
        return self.process_data(data)
    
    def process_file(self, input_file: str, output_file: str = None) -> Dict[str, Any]:
        """
        Process JSON file and round all decimal values
        
        Args:
            input_file: Input JSON file path
            output_file: Optional output file path
            
        Returns:
            Processed data
        """
        # Read input file
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Input file not found: {input_file}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in file {input_file}: {e}")
        
        # Process data
        processed_data = self.process_data(data)
        
        # Write output file if specified
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(processed_data, f, indent=2, ensure_ascii=False)
        
        return processed_data
    
    def get_statistics(self, data: Any) -> Dict[str, int]:
        """
        Get statistics about the processing
        
        Args:
            data: Processed data structure
            
        Returns:
            Statistics dictionary
        """
        stats = {
            'total_values': 0,
            'rounded_values': 0,
            'null_values': 0,
            'integer_values': 0,
            'string_values': 0,
            'boolean_values': 0,
            'objects': 0,
            'arrays': 0
        }
        
        def count_values(item):
            if isinstance(item, dict):
                stats['objects'] += 1
                for value in item.values():
                    count_values(value)
            elif isinstance(item, list):
                stats['arrays'] += 1
                for value in item:
                    count_values(value)
            else:
                stats['total_values'] += 1
                if item is None:
                    stats['null_values'] += 1
                elif isinstance(item, bool):
                    stats['boolean_values'] += 1
                elif isinstance(item, int):
                    stats['integer_values'] += 1
                elif isinstance(item, float):
                    stats['rounded_values'] += 1
                elif isinstance(item, str):
                    stats['string_values'] += 1
        
        count_values(data)
        return stats


def main():
    """Main function to handle command line arguments"""
    parser = argparse.ArgumentParser(
        description='Extract and round decimal values in JSON data to specified precision'
    )
    parser.add_argument('input', help='Input JSON file or JSON string')
    parser.add_argument('-o', '--output', help='Output JSON file (default: stdout)')
    parser.add_argument('-d', '--decimal-places', type=int, default=2, 
                       help='Number of decimal places to round to (default: 2)')
    parser.add_argument('-s', '--stats', action='store_true', 
                       help='Show processing statistics')
    parser.add_argument('--in-place', action='store_true', 
                       help='Modify input file in place (only works with file input)')
    parser.add_argument('--validate', action='store_true', 
                       help='Validate JSON structure after processing')
    
    args = parser.parse_args()
    
    # Initialize rounder
    rounder = JSONDecimalRounder(args.decimal_places)
    
    try:
        # Determine if input is a file or JSON string
        is_file = os.path.isfile(args.input)
        
        if is_file:
            # Process file
            output_file = args.input if args.in_place else args.output
            processed_data = rounder.process_file(args.input, output_file)
            
            if args.output and not args.in_place:
                print(f"Processed data written to: {args.output}")
            elif args.in_place:
                print(f"File processed in place: {args.input}")
            elif not args.output:
                # Output to stdout
                print(json.dumps(processed_data, indent=2, ensure_ascii=False))
                
        else:
            # Process JSON string
            processed_data = rounder.extract_and_round_json(args.input)
            
            if args.output:
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(processed_data, f, indent=2, ensure_ascii=False)
                print(f"Processed data written to: {args.output}")
            else:
                print(json.dumps(processed_data, indent=2, ensure_ascii=False))
        
        # Show statistics if requested
        if args.stats:
            stats = rounder.get_statistics(processed_data)
            print("\n=== Processing Statistics ===", file=sys.stderr)
            print(f"Total values processed: {stats['total_values']}", file=sys.stderr)
            print(f"Decimal values rounded: {stats['rounded_values']}", file=sys.stderr)
            print(f"Integer values: {stats['integer_values']}", file=sys.stderr)
            print(f"String values: {stats['string_values']}", file=sys.stderr)
            print(f"Boolean values: {stats['boolean_values']}", file=sys.stderr)
            print(f"Null values: {stats['null_values']}", file=sys.stderr)
            print(f"Objects: {stats['objects']}", file=sys.stderr)
            print(f"Arrays: {stats['arrays']}", file=sys.stderr)
            print(f"Decimal places: {args.decimal_places}", file=sys.stderr)
        
        # Validate JSON structure if requested
        if args.validate:
            try:
                json.dumps(processed_data)
                print("\n✓ JSON structure validation passed", file=sys.stderr)
            except (TypeError, ValueError) as e:
                print(f"\n✗ JSON structure validation failed: {e}", file=sys.stderr)
                return 1
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())