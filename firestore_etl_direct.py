import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pandas as pd
import os
import uuid
from datetime import datetime

# --- Configuration ---
# NOTE: Update this path to your actual service account key file
SERVICE_ACCOUNT_KEY_PATH = 'ServiceAcc.json'
OUTPUT_DIR = 'normalized_output'

# Initialize Firebase App
try:
    cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("Firebase initialized successfully.")
except FileNotFoundError:
    print(f"ERROR: Service account key not found at {SERVICE_ACCOUNT_KEY_PATH}. Please check the path.")
    exit()
except Exception as e:
    print(f"An error occurred during Firebase initialization: {e}")
    exit()

# --- 1. EXTRACT Function ---
def fetch_collection_data(collection_name):
    """Fetches all documents from a specified Firestore collection."""
    print(f"\nExtracting data from '{collection_name}' collection...")
    
    docs = db.collection(collection_name).stream()
    data = []
    for doc in docs:
        # Get the dictionary representation of the document
        doc_data = doc.to_dict()
        # Add the document ID (which we used as the primary key in the setup script)
        doc_data['doc_id'] = doc.id 
        data.append(doc_data)
        
    print(f"Successfully fetched {len(data)} documents from '{collection_name}'.")
    return data

# --- 2. TRANSFORMATION Logic (Normalization) ---
def transform_and_normalize(recipes_data, interactions_data):
    """
    Transforms denormalized recipes and interactions data into four normalized DataFrames.
    """
    
    # Initialize lists for normalized data
    normalized_recipes = []
    normalized_ingredients = []
    normalized_steps = []
    
    print("\nStarting normalization of Recipes (Denormalized -> Normalized)...")
    
    for recipe_doc in recipes_data:
        # Use the 'recipe_id' field which was explicitly set in the previous step
        recipe_id = recipe_doc.get('recipe_id')
        
        if not recipe_id:
            print(f"Skipping document with missing recipe_id: {recipe_doc.get('name', 'Unknown')}")
            continue

        # A. Create Recipe Record (Core fields)
        normalized_recipes.append({
            'recipe_id': recipe_id,
            'name': recipe_doc.get('name'),
            'serves': recipe_doc.get('serves'),
            'prep_time_minutes': recipe_doc.get('prep_time_minutes'),
            'cook_time_minutes': recipe_doc.get('cook_time_minutes'),
            'difficulty': recipe_doc.get('difficulty'),
            # Handle Firestore Timestamp objects by converting them to ISO format string
            'created_at': recipe_doc.get('created_at').isoformat() if recipe_doc.get('created_at') else None
        })
        
        # B. Create Ingredients Records (Extracting from embedded array)
        for ingredient in recipe_doc.get('ingredients', []):
            normalized_ingredients.append({
                'ingredient_pk_id': str(uuid.uuid4()),  # Generate a unique PK for the new row
                'recipe_id': recipe_id,                 # Foreign Key
                'name': ingredient.get('name'),
                'quantity': ingredient.get('quantity'),
                'unit': ingredient.get('unit')
            })

        # C. Create Steps Records (Extracting from embedded array)
        for step in recipe_doc.get('steps', []):
            normalized_steps.append({
                'step_pk_id': str(uuid.uuid4()),       # Generate a unique PK for the new row
                'recipe_id': recipe_id,                # Foreign Key
                'step_number': step.get('step_number'),
                'description': step.get('description')
            })

    print(f"Normalized {len(normalized_recipes)} recipes, {len(normalized_ingredients)} ingredients, and {len(normalized_steps)} steps.")
    
    # D. Process Interactions Collection
    print("Processing Interactions data...")
    for interaction in interactions_data:
        # Convert Timestamp objects
        if interaction.get('timestamp'):
            interaction['timestamp'] = interaction['timestamp'].isoformat()
    
    # Convert lists to DataFrames
    recipe_df = pd.DataFrame(normalized_recipes)
    ingredients_df = pd.DataFrame(normalized_ingredients)
    steps_df = pd.DataFrame(normalized_steps)
    interactions_df = pd.DataFrame(interactions_data)

    return recipe_df, ingredients_df, steps_df, interactions_df

# --- 3. LOAD (Export to CSV) Function ---
def export_to_csv(dataframes):
    """Exports DataFrames to CSV files in the specified output directory."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    output_mapping = {
        'recipe': 'recipe.csv',
        'ingredients': 'ingredients.csv',
        'steps': 'steps.csv',
        'interactions': 'interactions.csv'
    }
    
    for name, df in dataframes.items():
        file_path = os.path.join(OUTPUT_DIR, output_mapping[name])
        
        # Ensure 'recipe_id' is present and not null for FK columns if data integrity is key
        # (This is better handled in the Data Quality step, but good to ensure string type here)
        df = df.astype({'recipe_id': 'str'}, errors='ignore')
        
        df.to_csv(file_path, index=False, encoding='utf-8')
        print(f"Exported {len(df)} records to {file_path}")

# --- 4. MAIN EXECUTION ---
def run_pipeline():
    """Runs the complete ETL pipeline."""
    
    # 1. EXTRACT
    recipes_data = fetch_collection_data('recipes')
    # Fetch users data only for completeness/future joins (not required for the 4 CSVs)
    users_data = fetch_collection_data('users') 
    interactions_data = fetch_collection_data('interactions')

    if not recipes_data or not interactions_data:
        print("\nPipeline failed because core data collections were empty or missing.")
        return

    # 2. TRANSFORM
    recipe_df, ingredients_df, steps_df, interactions_df = transform_and_normalize(
        recipes_data, 
        interactions_data
    )

    # 3. LOAD
    dataframes = {
        'recipe': recipe_df,
        'ingredients': ingredients_df,
        'steps': steps_df,
        'interactions': interactions_df
    }
    
    export_to_csv(dataframes)

    print("\nETL/ELT Pipeline: Firestore data successfully extracted, normalized, and exported to CSV.")

if __name__ == "__main__":
    run_pipeline()