import pandas as pd
import os

# -----------------------------
# 1️⃣ Load Raw Datasets
# -----------------------------
def load_datasets():
    ikea = pd.read_csv("raw/IKEA_SA_Furniture_Web_Scrapings_sss.csv")
    bar = pd.read_csv("raw/bar.csv")
    amazon = pd.read_csv("raw/furniture_amazon_dataset_sample copy.csv")
    return ikea, bar, amazon


# -----------------------------
# 2️⃣ Standardize Columns
# -----------------------------
def standardize_columns(ikea, bar, amazon):

    # ----- IKEA -----
    ikea = ikea.rename(columns={
        "name": "product_name",
        "category": "category",
        "price": "price",
        "short_description": "description",
        "link": "source_link"
    })

    ikea = ikea[["product_name", "category", "price", "description", "source_link"]]
    ikea["platform"] = "IKEA"


    # ----- BAR -----
    bar = bar.rename(columns={
        "name": "product_name",
        "furniture_type": "category",
        "discounted_price": "price",
        "product_details": "description"
    })

    bar = bar[["product_name", "category", "price", "description"]]
    bar["source_link"] = None
    bar["platform"] = "Other"


    # ----- AMAZON -----
    amazon = amazon.rename(columns={
        "title": "product_name",
        "categories": "category",
        "price": "price",
        "description": "description",
        "url": "source_link"
    })

    amazon = amazon[["product_name", "category", "price", "description", "source_link"]]
    amazon["platform"] = "Amazon"

    return ikea, bar, amazon


# -----------------------------
# 3️⃣ Merge Datasets
# -----------------------------
def merge_datasets(ikea, bar, amazon):
    final_df = pd.concat([ikea, bar, amazon], ignore_index=True)
    return final_df


# -----------------------------
# 4️⃣ Clean Data
# -----------------------------
def clean_data(df):

    # Remove duplicates
    df = df.drop_duplicates(subset="product_name")

    # Convert price to numeric
    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace("₹", "", regex=False)
        .str.replace(",", "", regex=False)
    )

    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Drop rows where name or price missing
    df = df.dropna(subset=["product_name", "price"])

    # Fill missing descriptions
    df["description"] = df["description"].fillna("No description available")

    return df


# -----------------------------
# 5️⃣ Add Dynamic Links
# -----------------------------
def add_dynamic_links(df):

    df["search_query"] = df["product_name"].str.replace(" ", "+", regex=False)

    df["amazon_link"] = "https://www.amazon.in/s?k=" + df["search_query"]
    df["flipkart_link"] = "https://www.flipkart.com/search?q=" + df["search_query"]

    df.drop(columns=["search_query"], inplace=True)

    return df


# -----------------------------
# 6️⃣ Save Final Dataset
# -----------------------------
def save_dataset(df):

    os.makedirs("processed", exist_ok=True)
    df.to_csv("processed/final_products.csv", index=False)
    print("✅ Final dataset saved at data/processed/final_products.csv")


# -----------------------------
# 🚀 Main Execution
# -----------------------------
if __name__ == "__main__":

    print("Loading datasets...")
    ikea, bar, amazon = load_datasets()

    print("Standardizing columns...")
    ikea, bar, amazon = standardize_columns(ikea, bar, amazon)

    print("Merging datasets...")
    final_df = merge_datasets(ikea, bar, amazon)

    print("Cleaning data...")
    final_df = clean_data(final_df)

    print("Adding dynamic marketplace links...")
    final_df = add_dynamic_links(final_df)

    print("Saving final dataset...")
    save_dataset(final_df)

    print("🎉 Data processing complete!")