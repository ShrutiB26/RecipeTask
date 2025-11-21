import pandas as pd
import os
import numpy as np
import uuid
import matplotlib.pyplot as plt

# --- Configuration ---
INPUT_DIR = 'normalized_output'

INPUT_FILES = {
    'recipe': os.path.join(INPUT_DIR, 'recipe.csv'),
    'ingredients': os.path.join(INPUT_DIR, 'ingredients.csv'),
    'steps': os.path.join(INPUT_DIR, 'steps.csv'),
    'interactions': os.path.join(INPUT_DIR, 'interactions.csv')
}

# --- Data Loading and Preparation ---
def load_and_merge_data():
    """Loads CSV files and merges them for analysis."""
    try:
        # Load DataFrames
        recipes_df = pd.read_csv(INPUT_FILES['recipe'])
        ingredients_df = pd.read_csv(INPUT_FILES['ingredients'])
        interactions_df = pd.read_csv(INPUT_FILES['interactions'])

        # Data Cleaning/Type Conversion (Essential for analysis)
        recipes_df['prep_time_minutes'] = pd.to_numeric(recipes_df['prep_time_minutes'], errors='coerce')
        interactions_df['rating'] = pd.to_numeric(interactions_df['rating'], errors='coerce')

        # Create 'name_clean' column (already done in mock script, but ensuring for robustness)
        ingredients_df['name_clean'] = ingredients_df['name'].astype(str).str.strip()

        # Merge Recipes and Interactions for engagement analysis
        recipe_interaction_df = pd.merge(
            recipes_df,
            interactions_df,
            on='recipe_id',
            how='left'
        )

        # Merge Ingredients and Recipes for ingredient analysis
        ingredient_recipe_df = pd.merge(
            ingredients_df,
            recipes_df[['recipe_id', 'difficulty']],
            on='recipe_id',
            how='left'
        )

        return recipes_df, ingredients_df, interactions_df, recipe_interaction_df, ingredient_recipe_df

    except FileNotFoundError as e:
        print(f"ERROR: Could not find necessary input files. Run the ETL and Validation steps first. Missing: {e}")
        return None, None, None, None, None
    except Exception as e:
        print(f"An error occurred during data loading or merging: {e}")
        return None, None, None, None, None

# --- Visualization Generation Function ---
def generate_visualizations(recipes_df, ingredients_df, interactions_df):
    """Generates and saves key visualization charts."""
    print("\nGenerating Visualization Charts...")
    os.makedirs(INPUT_DIR, exist_ok=True)

    plt.style.use('ggplot')

    # 1. Recipe Difficulty Distribution (Pie Chart)
    difficulty_counts = recipes_df['difficulty'].value_counts()
    plt.figure(figsize=(7, 7))
    plt.pie(difficulty_counts, labels=difficulty_counts.index, autopct='%1.1f%%', startangle=90, wedgeprops={'edgecolor': 'black'})
    plt.title('Recipe Difficulty Distribution', fontsize=14)
    plt.savefig(os.path.join(INPUT_DIR, '01_difficulty_distribution.png'))
    plt.close()

    # 2. Top 5 Most Viewed Recipes (Bar Chart)
    views_per_recipe = interactions_df[interactions_df['interaction_type'] == 'VIEW'] \
                                     .groupby('recipe_id').size().reset_index(name='total_views')
    views_ranked = pd.merge(views_per_recipe, recipes_df[['recipe_id', 'name']], on='recipe_id') \
                         .sort_values(by='total_views', ascending=False).head(5)

    plt.figure(figsize=(10, 6))
    plt.bar(views_ranked['name'], views_ranked['total_views'], color='skyblue')
    plt.title('Top 5 Most Viewed Recipes', fontsize=14)
    plt.xlabel('Recipe Name', fontsize=12)
    plt.ylabel('Total Views', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(INPUT_DIR, '02_top_viewed_recipes.png'))
    plt.close()

    # 3. Average Rating by Difficulty (Bar Chart)
    recipe_interaction_df = pd.merge(recipes_df, interactions_df, on='recipe_id', how='left')
    difficulty_ratings = recipe_interaction_df[recipe_interaction_df['interaction_type'] == 'COOK_ATTEMPT'] \
                                            .groupby('difficulty')['rating'].mean().round(2).sort_values(ascending=False)

    plt.figure(figsize=(8, 5))
    plt.bar(difficulty_ratings.index, difficulty_ratings.values, color=['green', 'orange', 'red'])
    plt.title('Average Rating by Difficulty Level', fontsize=14)
    plt.xlabel('Difficulty', fontsize=12)
    plt.ylabel('Average Rating (1-5)', fontsize=12)
    if not difficulty_ratings.empty:
        plt.ylim(difficulty_ratings.min() * 0.95, 5.0) 
    else:
        plt.ylim(3.0, 5.0) 
    plt.tight_layout()
    plt.savefig(os.path.join(INPUT_DIR, '03_avg_rating_by_difficulty.png'))
    plt.close()

    # 4. Top 5 Most Common Ingredients (Bar Chart)
    common_ingredients = ingredients_df['name_clean'].value_counts().head(5)

    plt.figure(figsize=(10, 6))
    plt.bar(common_ingredients.index, common_ingredients.values, color='teal')
    plt.title('Top 5 Most Common Ingredients', fontsize=14)
    plt.xlabel('Ingredient Name', fontsize=12)
    plt.ylabel('Count Across Recipes', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(INPUT_DIR, '04_top_common_ingredients.png'))
    plt.close()

    print("4 Visualization charts saved to 'normalized_output/' directory.")

# --- Analytics Calculations ---
def generate_insights(recipes_df, ingredients_df, interactions_df, recipe_interaction_df, ingredient_recipe_df):
    """Calculates and presents the required 10+ insights."""
    insights = []
    
    # --- INSIGHT 1: Most Common Ingredients ---
    common_ingredients = ingredients_df['name_clean'].value_counts().head(5)
    insights.append({
        "ID": 1,
        "Name": "Most Common Ingredients",
        "Result": common_ingredients.to_dict()
    })

    # --- INSIGHT 2: Average Preparation Time ---
    avg_prep_time = recipes_df['prep_time_minutes'].mean().round(2)
    insights.append({
        "ID": 2,
        "Name": "Average Preparation Time (Minutes)",
        "Result": f"{avg_prep_time}"
    })

    # --- INSIGHT 3: Difficulty Distribution ---
    difficulty_dist = recipes_df['difficulty'].value_counts(normalize=True).mul(100).round(2)
    insights.append({
        "ID": 3,
        "Name": "Recipe Difficulty Distribution (%)",
        "Result": difficulty_dist.to_dict()
    })

    # --- INSIGHT 4: Correlation between Prep Time and Likes ---
    likes_per_recipe = interactions_df[interactions_df['interaction_type'] == 'LIKE'] \
                                     .groupby('recipe_id').size().reset_index(name='total_likes')
                                     
    prep_like_df = pd.merge(recipes_df, likes_per_recipe, on='recipe_id', how='left').fillna(0)
    
    correlation = prep_like_df[['prep_time_minutes', 'total_likes']].corr().iloc[0, 1].round(4)
    insights.append({
        "ID": 4,
        "Name": "Correlation (Prep Time vs. Likes)",
        "Result": f"{correlation} (Positive means longer prep time, more likes)"
    })

    # --- INSIGHT 5: Most Frequently Viewed Recipes ---
    views_per_recipe = interactions_df[interactions_df['interaction_type'] == 'VIEW'] \
                                     .groupby('recipe_id').size().reset_index(name='total_views')
    
    views_ranked = pd.merge(views_per_recipe, recipes_df[['recipe_id', 'name']], on='recipe_id') \
                         .sort_values(by='total_views', ascending=False).head(5)
                         
    insights.append({
        "ID": 5,
        "Name": "Top 5 Most Viewed Recipes",
        "Result": views_ranked.set_index('name')['total_views'].to_dict()
    })

    # --- INSIGHT 6: Ingredients Associated with High Engagement (Avg. Rating) ---
    avg_ratings = interactions_df[interactions_df['interaction_type'] == 'COOK_ATTEMPT'] \
                                .groupby('recipe_id')['rating'].mean().round(2).reset_index(name='avg_rating')
    
    high_rated_recipes = avg_ratings[avg_ratings['avg_rating'] >= 4.0]['recipe_id']
    
    high_engagement_ingredients = ingredient_recipe_df[
        ingredient_recipe_df['recipe_id'].isin(high_rated_recipes)
    ]['name_clean'].value_counts().head(5)
    
    insights.append({
        "ID": 6,
        "Name": "Top 5 Ingredients in High-Rated Recipes (Avg Rating >= 4.0)",
        "Result": high_engagement_ingredients.to_dict()
    })
    
    # --- INSIGHT 7: Average Rating per Difficulty Level ---
    difficulty_ratings = recipe_interaction_df[recipe_interaction_df['interaction_type'] == 'COOK_ATTEMPT'] \
                                            .groupby('difficulty')['rating'].mean().round(2).sort_values(ascending=False)
    insights.append({
        "ID": 7,
        "Name": "Average Rating by Difficulty",
        "Result": difficulty_ratings.to_dict()
    })

    # --- INSIGHT 8: Average Number of Steps per Recipe ---
    steps_df = pd.read_csv(INPUT_FILES['steps'])
    steps_count = steps_df.groupby('recipe_id').size().mean().round(2)
    insights.append({
        "ID": 8,
        "Name": "Average Number of Steps per Recipe",
        "Result": f"{steps_count}"
    })
    
    # --- INSIGHT 9: Top 5 Users by Total Interactions ---
    top_users = interactions_df['user_id'].value_counts().head(5)
    insights.append({
        "ID": 9,
        "Name": "Top 5 Most Active Users (Total Interactions)",
        "Result": top_users.to_dict()
    })
    
    # --- INSIGHT 10: Percentage of Recipes that have been 'COOK_ATTEMPT'ed ---
    total_recipes = len(recipes_df)
    attempted_recipes = interactions_df[interactions_df['interaction_type'] == 'COOK_ATTEMPT']['recipe_id'].nunique()
    
    attempt_percentage = round((attempted_recipes / total_recipes * 100), 2)
    
    insights.append({
        "ID": 10,
        "Name": "Percentage of Recipes with at least one 'COOK_ATTEMPT'",
        "Result": f"{attempt_percentage}% (Attempted: {attempted_recipes} out of {total_recipes})"
    })
    
    # --- INSIGHT 11: Most Liked Recipe by Unique User Count (Bonus Insight) ---
    unique_likes = interactions_df[interactions_df['interaction_type'] == 'LIKE'] \
                                  .drop_duplicates(subset=['recipe_id', 'user_id']) \
                                  .groupby('recipe_id').size().reset_index(name='unique_likes')
                                  
    most_unique_liked = pd.merge(recipes_df[['recipe_id', 'name']], unique_likes, on='recipe_id') \
                              .sort_values(by='unique_likes', ascending=False).head(1)
                              
    insights.append({
        "ID": 11,
        "Name": "Most Liked Recipe by Unique User Count",
        "Result": most_unique_liked.set_index('name')['unique_likes'].to_dict()
    })

    return insights

def print_insights(insights):
    """Prints the calculated insights in a readable format."""
    print("\n" + "="*80)
    print("5. ANALYTICS REQUIREMENTS - DATA INSIGHTS SUMMARY")
    print("="*80)
    for insight in insights:
        print(f"\n--- INSIGHT {insight['ID']}: {insight['Name']} ---")
        if isinstance(insight['Result'], dict):
            for key, value in insight['Result'].items():
                print(f"- {key}: {value}")
        else:
            print(f"- {insight['Result']}")
    print("\n" + "="*80)

# --- Main Execution ---
if __name__ == "__main__":
    
    recipes_df, ingredients_df, interactions_df, recipe_interaction_df, ingredient_recipe_df = load_and_merge_data()

    if recipes_df is not None:
        # 1. Generate and save the Visualization Charts
        generate_visualizations(recipes_df, ingredients_df, interactions_df)

        # 2. Generate and print the Analytical Insights Summary
        insights = generate_insights(recipes_df, ingredients_df, interactions_df, recipe_interaction_df, ingredient_recipe_df)
        print_insights(insights)