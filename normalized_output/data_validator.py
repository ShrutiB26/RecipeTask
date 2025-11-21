import pandas as pd
import os
import csv
from datetime import datetime

# --- Configuration ---
INPUT_DIR = 'normalized_output'
REPORT_FILE = 'data_quality_report.txt'

INPUT_FILES = {
    'recipe': os.path.join(INPUT_DIR, 'recipe.csv'),
    'ingredients': os.path.join(INPUT_DIR, 'ingredients.csv'),
    'steps': os.path.join(INPUT_DIR, 'steps.csv'),
    'interactions': os.path.join(INPUT_DIR, 'interactions.csv')
}

VALID_DIFFICULTIES = {'Easy', 'Medium', 'Hard'}
VALID_INTERACTION_TYPES = {'VIEW', 'LIKE', 'COOK_ATTEMPT'}

# --- Core Validation Logic ---
def validate_data(file_key, df):
    """Applies validation rules to a single DataFrame and generates a report for invalid records."""
    
    validation_report = []
    
    # 1. Define required fields and validation rules based on the file type
    if file_key == 'recipe':
        required_fields = ['recipe_id', 'name', 'serves', 'prep_time_minutes', 'cook_time_minutes', 'difficulty']
        
        for index, row in df.iterrows():
            is_valid = True
            reasons = []
            
            # Rule 1: Required fields present (non-null)
            for field in required_fields:
                if pd.isna(row[field]):
                    reasons.append(f"Missing required field: {field}")
                    is_valid = False
            
            # Rule 2: Positive Numeric Fields (serves, times)
            numeric_fields = ['serves', 'prep_time_minutes', 'cook_time_minutes']
            for field in numeric_fields:
                if row[field] is not None and row[field] < 0:
                    reasons.append(f"Field {field} must be positive, found: {row[field]}")
                    is_valid = False

            # Rule 3: Valid difficulty values
            if row['difficulty'] not in VALID_DIFFICULTIES:
                reasons.append(f"Invalid difficulty value: {row['difficulty']}")
                is_valid = False

            if not is_valid:
                validation_report.append({
                    'id': row['recipe_id'],
                    'status': 'INVALID',
                    'reasons': reasons
                })

    elif file_key == 'ingredients':
        required_fields = ['ingredient_pk_id', 'recipe_id', 'name', 'quantity']
        
        for index, row in df.iterrows():
            is_valid = True
            reasons = []

            # Rule 1: Required fields present (non-null)
            for field in required_fields:
                if pd.isna(row[field]):
                    reasons.append(f"Missing required field: {field}")
                    is_valid = False
            
            # Rule 2: Positive Numeric Fields (quantity)
            if row['quantity'] is not None and row['quantity'] < 0:
                reasons.append(f"Quantity must be positive, found: {row['quantity']}")
                is_valid = False

            if not is_valid:
                validation_report.append({
                    'id': row['ingredient_pk_id'],
                    'status': 'INVALID',
                    'reasons': reasons
                })
                
    elif file_key == 'steps':
        required_fields = ['step_pk_id', 'recipe_id', 'step_number', 'description']
        
        for index, row in df.iterrows():
            is_valid = True
            reasons = []

            # Rule 1: Required fields present (non-null)
            for field in required_fields:
                if pd.isna(row[field]):
                    reasons.append(f"Missing required field: {field}")
                    is_valid = False
            
            # Rule 2: Positive Numeric Fields (step_number)
            if row['step_number'] is not None and row['step_number'] < 1:
                reasons.append(f"Step number must be positive, found: {row['step_number']}")
                is_valid = False
                
            if not is_valid:
                validation_report.append({
                    'id': row['step_pk_id'],
                    'status': 'INVALID',
                    'reasons': reasons
                })
                
    elif file_key == 'interactions':
        required_fields = ['interaction_id', 'user_id', 'recipe_id', 'interaction_type', 'timestamp']
        
        for index, row in df.iterrows():
            is_valid = True
            reasons = []

            # Rule 1: Required fields present (non-null)
            for field in required_fields:
                if pd.isna(row[field]):
                    reasons.append(f"Missing required field: {field}")
                    is_valid = False

            # Rule 2: Valid Interaction Type
            if row['interaction_type'] not in VALID_INTERACTION_TYPES:
                reasons.append(f"Invalid interaction type: {row['interaction_type']}")
                is_valid = False

            # Rule 3: Rating validation (only 1-5 for COOK_ATTEMPT)
            if row['interaction_type'] == 'COOK_ATTEMPT' and (pd.isna(row['rating']) or row['rating'] < 1 or row['rating'] > 5):
                reasons.append(f"Cook attempt must have a rating between 1 and 5.")
                is_valid = False
            elif row['interaction_type'] != 'COOK_ATTEMPT' and not pd.isna(row['rating']):
                 # Non-Cook Attempts should not have a rating (soft validation)
                reasons.append(f"Interaction type {row['interaction_type']} should not have a rating.")
                is_valid = False
                
            if not is_valid:
                validation_report.append({
                    'id': row['interaction_id'],
                    'status': 'INVALID',
                    'reasons': reasons
                })

    # Calculate final status
    total_records = len(df)
    invalid_records = len(validation_report)
    valid_records = total_records - invalid_records

    return total_records, valid_records, invalid_records, validation_report

# --- Main Execution ---
def run_validator():
    """Reads all normalized files, validates them, and generates a final report."""
    
    # Check if the input directory exists
    if not os.path.isdir(INPUT_DIR):
        print(f"ERROR: Input directory '{INPUT_DIR}' not found. Please run the ETL script first.")
        return

    # Initialize report content
    report_content = [
        f"Data Quality Validation Report - Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "="*70,
        "Summary of Validation Rules Applied:",
        "1. Required fields must be non-null.",
        "2. Numeric fields (e.g., times, serves, quantity) must be positive.",
        "3. Difficulty values must be 'Easy', 'Medium', or 'Hard'.",
        "4. Interaction types must be 'VIEW', 'LIKE', or 'COOK_ATTEMPT'.",
        "5. 'COOK_ATTEMPT' must have a rating between 1 and 5.",
        "-"*70
    ]

    all_reports = {}
    
    for file_key, file_path in INPUT_FILES.items():
        if not os.path.exists(file_path):
            report_content.append(f"\nSkipping {file_key}: File not found at {file_path}")
            continue

        try:
            # Load the CSV data
            df = pd.read_csv(file_path)
            # Fill NaN values in 'rating' with None for explicit validation checks later
            if 'rating' in df.columns:
                df['rating'] = df['rating'].fillna(-1) 
            
            # Run validation
            total, valid, invalid, report = validate_data(file_key, df)
            all_reports[file_key] = report
            
            # Append summary to report
            report_content.append(f"\nFile: {file_key.upper()}.CSV")
            report_content.append(f"Total Records: {total}")
            report_content.append(f"Valid Records: {valid}")
            report_content.append(f"Invalid Records: {invalid}")
            
            if invalid > 0:
                report_content.append(f"--- {invalid} INVALID RECORDS FOUND ---")
                for record in report:
                    reasons_str = "; ".join(record['reasons'])
                    report_content.append(f"ID: {record['id']} | Reasons: {reasons_str}")
            else:
                report_content.append("Data quality check PASSED. No invalid records found.")
            
            report_content.append("-"*70)

        except Exception as e:
            report_content.append(f"\nERROR processing {file_key}: {e}")

    # Write final report
    with open(REPORT_FILE, 'w') as f:
        f.write('\n'.join(report_content))

    print("\n" + "="*70)
    print(f"Data Quality Validation Complete. Report saved to: {REPORT_FILE}")
    print("="*70)

if __name__ == "__main__":
    run_validator()