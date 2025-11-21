import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import datetime
import random
import uuid

# --- 1. INITIALIZE FIREBASE APP ---
try:
    cred = credentials.Certificate('ServiceAcc.json')
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("Firebase initialized successfully.")
except FileNotFoundError:
    print("ERROR: Service account key not found. Please check the path.")
    exit()
except Exception as e:
    print(f"An error occurred during Firebase initialization: {e}")
    exit()

# --- 2. DATA DEFINITION ---

# A. Masala Rice Embedded Data (R001)
masala_rice_ingredients_embedded = [
    {"name": "Rice (Prefer Basmati Rice)", "quantity": 1.5, "unit": "cup"},
    {"name": "Water", "quantity": 3, "unit": "cups"},
    {"name": "Red Chilli Powder", "quantity": 2, "unit": "spoons"},
    {"name": "Haldi Powder", "quantity": 1, "unit": "spoon"},
    {"name": "Coriander Powder", "quantity": 2, "unit": "spoons"},
    {"name": "Garam Masala", "quantity": 0.5, "unit": "spoon"},
    {"name": "Salt", "quantity": 2, "unit": "spoons"},
    {"name": "Cumin seeds", "quantity": 1, "unit": "spoon"},
    {"name": "Ginger Garlic paste", "quantity": 1, "unit": "spoon"},
    {"name": "Onion", "quantity": 1, "unit": "whole"},
    {"name": "Tomato", "quantity": 1, "unit": "whole"},
    {"name": "Potato", "quantity": 1, "unit": "whole"},
    {"name": "Coriander leaves", "quantity": None, "unit": "garnish"}
]

masala_rice_steps_embedded = [
    {"step_number": 1, "description": "Wash and soak rice for 10 minutes."},
    {"step_number": 2, "description": "Heat pan/kadhai."},
    {"step_number": 3, "description": "Heat oil. Add Cumin seeds, then Ginger Garlic paste."},
    {"step_number": 4, "description": "Add chopped Onion and cook until golden brown."},
    {"step_number": 5, "description": "Add potato cubes and sauté."},
    {"step_number": 6, "description": "Add chopped tomatoes and all masalas from the ingredient list. Stir."},
    {"step_number": 7, "description": "Cover and cook for 2-3 mins until oil separates."},
    {"step_number": 8, "description": "Add soaked rice and mix well with masala."},
    {"step_number": 9, "description": "Add 3 cups of water. Cover and cook for 10 minutes on medium-high flame."},
    {"step_number": 10, "description": "Lower flame, keep covered for 2-3 minutes to enhance taste with steam."},
    {"step_number": 11, "description": "Open lid, add chopped coriander leaves, and fluff the rice. Serve hot."}
]

masala_rice_recipe = {
    "recipe_id": "R001",
    "name": "Masala Rice",
    "serves": 2,
    "prep_time_minutes": 15,
    "cook_time_minutes": 25,
    "difficulty": "Medium",
    "created_at": datetime.datetime.now(),
    "ingredients": masala_rice_ingredients_embedded,
    "steps": masala_rice_steps_embedded
}

# B. 19 REALISTIC SYNTHETIC RECIPES
real_recipe_names = [
    "Paneer Butter Masala", "Chicken Biryani", "Veg Hakka Noodles", "Aloo Paratha", "Egg Curry",
    "Palak Paneer", "Dal Tadka", "Chole Bhature", "Fish Fry", "Matar Paneer",
    "Pav Bhaji", "Sambar Rice", "Veg Fried Rice", "Chicken Curry", "Besan Ladoo",
    "Gajar Halwa", "Upma", "Poha", "Veg Sandwich"
]

realistic_ingredients = [
    "Salt", "Turmeric", "Cumin Seeds", "Onion", "Tomato", "Ginger", "Garlic", "Green Chilli",
    "Oil", "Butter", "Milk", "Rice", "Wheat Flour", "Black Pepper",
    "Coriander Powder", "Chilli Powder", "Paneer", "Chicken", "Eggs",
    "Potato", "Carrot", "Peas"
]

difficulties = ["Easy", "Medium", "Hard"]

synthetic_recipes = []

for i in range(2, 21):
    recipe_id = f"R{i:03d}"

    # random ingredients
    embedded_ingredients = []
    for j in range(random.randint(4, 10)):
        embedded_ingredients.append({
            "name": random.choice(realistic_ingredients),
            "quantity": round(random.uniform(0.1, 3.0), 2),
            "unit": random.choice(["cup", "tsp", "tbsp", "g", "ml", "whole"])
        })

    # random steps
    embedded_steps = []
    for k in range(random.randint(3, 7)):
        embedded_steps.append({
            "step_number": k + 1,
            "description": f"Step {k+1}: Follow standard cooking procedure."
        })

    recipe = {
        "recipe_id": recipe_id,
        "name": real_recipe_names.pop(0),
        "serves": random.randint(1, 6),
        "prep_time_minutes": random.randint(5, 30),
        "cook_time_minutes": random.randint(10, 60),
        "difficulty": random.choice(difficulties),
        "created_at": datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 365)),
        "ingredients": embedded_ingredients,
        "steps": embedded_steps
    }

    synthetic_recipes.append(recipe)

# C. 10 USERS WITH REAL HUMAN NAMES
human_names = [
    "Aarav Sharma", "Isha Patel", "Rohan Mehta", "Ananya Singh",
    "Kabir Khanna", "Meera Joshi", "Sahil Verma", "Tanya Kapoor",
    "Vikram Nair", "Shruti Deshmukh"
]

users_data = []
for i in range(1, 11):
    user_id = f"U{i:03d}"
    users_data.append({
        "user_id": user_id,
        "username": human_names[i-1],
        "join_date": datetime.datetime.now() - datetime.timedelta(days=random.randint(10, 500))
    })

# D. INTERACTION DATA
interactions_data = []
all_recipe_ids = [f"R{i:03d}" for i in range(1, 21)]
interaction_types = ["VIEW", "LIKE", "COOK_ATTEMPT"]
user_ids = [u["user_id"] for u in users_data]

# Random interactions
for i in range(50):
    user_id = random.choice(user_ids)
    recipe_id = random.choice(all_recipe_ids)
    interaction_type = random.choices(interaction_types, weights=[70, 20, 10], k=1)[0]
    rating = random.randint(3, 5) if interaction_type == "COOK_ATTEMPT" else None

    interactions_data.append({
        "interaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "recipe_id": recipe_id,
        "interaction_type": interaction_type,
        "rating": rating,
        "timestamp": datetime.datetime.now() - datetime.timedelta(minutes=random.randint(1, 1000))
    })

# Extra engagement for R001
for _ in range(5):
    interactions_data.append({
        "interaction_id": str(uuid.uuid4()),
        "user_id": random.choice(user_ids),
        "recipe_id": "R001",
        "interaction_type": "LIKE",
        "rating": None,
        "timestamp": datetime.datetime.now() - datetime.timedelta(minutes=random.randint(1, 50))
    })

for _ in range(3):
    interactions_data.append({
        "interaction_id": str(uuid.uuid4()),
        "user_id": random.choice(user_ids),
        "recipe_id": "R001",
        "interaction_type": "COOK_ATTEMPT",
        "rating": random.randint(4, 5),
        "timestamp": datetime.datetime.now() - datetime.timedelta(minutes=random.randint(1, 50))
    })


# --- 3. UPLOAD FUNCTION ---
def upload_data(collection_name, data_list, document_id_field=None):
    print(f"\nStarting upload to '{collection_name}'...")
    collection_ref = db.collection(collection_name)
    uploaded_count = 0

    for item in data_list:
        try:
            if document_id_field:
                collection_ref.document(item[document_id_field]).set(item)
            else:
                collection_ref.add(item)
            uploaded_count += 1
        except Exception as e:
            print(f"Error uploading item to {collection_name}: {e}")

    print(f"Successfully uploaded {uploaded_count} documents to '{collection_name}'.")


# --- 4. EXECUTION ---
if __name__ == "__main__":

    all_recipes = [masala_rice_recipe] + synthetic_recipes
    upload_data("recipes", all_recipes, document_id_field="recipe_id")

    upload_data("users", users_data, document_id_field="user_id")

    upload_data("interactions", interactions_data, document_id_field="interaction_id")

    print("\nFirestore data setup complete with 3 collections!")
