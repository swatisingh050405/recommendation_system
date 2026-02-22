import pandas as pd
import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def clean_price(price):
    if pd.isna(price):
        return None
    price = str(price)
    price = price.replace("â‚¹", "").replace("₹", "")
    price = price.replace(",", "").strip()
    try:
        return float(price)
    except:
        return None

def load_datasets():
    fashion = os.path.join(BASE_DIR, "raw", "Fashion Dataset.csv")
    khaadi = os.path.join(BASE_DIR, "raw", "khaadi_data.csv")
    kurti = os.path.join(BASE_DIR, "raw", "kurtiData.csv")

    fashion_df = pd.read_csv(fashion)
    khaadi_df = pd.read_csv(khaadi)
    kurti_df = pd.read_csv(kurti)
    return fashion_df, khaadi_df, kurti_df

def standardize_columns(fashion, khaadi, kurti):

    
    # -------- FASHION DATASET --------
    fashion.columns = fashion.columns.str.lower()

    fashion["price"] = fashion["price"].apply(clean_price)

    fashion = pd.DataFrame({
        "product_id": fashion.get("p_id", fashion.index),
        "product_name": fashion.get("name"),
        "price": fashion.get("price"),
        "rating": fashion.get("avg_rating"),
        "description": fashion.get("description"),
        "image_url": fashion.get("image"),
        "product_url": None,
        "platform": fashion.get("brand")
    })
 # Placeholder for image links (if needed in future)


     # -------- KHAADI --------
    khaadi["price"] = khaadi["Price"].apply(clean_price)

    khaadi = pd.DataFrame({
        "product_id": khaadi["ID"],
        "product_name": khaadi["Product Name"],
        "price": khaadi["Price"],
        "rating": None,
        "description": khaadi["Product Description"],
        "image_url": khaadi["Img Path"],
        "product_url": khaadi["Product Link"],
        "platform": "khaadi"
    })
  # Placeholder for image links (if needed in future)


    # -------- KURTI (MYNTRA) --------

# 🔥 Take random 5000 rows
    kurti = kurti.sample(n=5000, random_state=42)

    kurti["price"] = kurti["price"].apply(clean_price)

    kurti = pd.DataFrame({
    "product_id": kurti["product_id"],
    "product_name": "kurti",
    "price": kurti["price"],
    "rating": kurti["rating"],
    "description": None,
    "image_url": kurti["image_url"],
    "product_url": kurti["product_url"],
    "platform": "myntra"
})
    return fashion, khaadi, kurti


# 
def merge_datasets(*datasets):
    final_df = pd.concat(datasets, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=["product_id"])
    return final_df


def load_women_folder(selected_files=None):
    """
    If selected_files is None → merge all CSVs
    If selected_files is list → merge only those
    """

    women_folder = os.path.join(BASE_DIR, "raw", "women")
    all_data = []

    for file in os.listdir(women_folder):
        if file.endswith(".csv"):

            if selected_files and file not in selected_files:
                continue

            file_path = os.path.join(women_folder, file)
            df = pd.read_csv(file_path)

            df.columns = df.columns.str.lower()

            if "price" in df.columns:
                df["price"] = df["price"].apply(clean_price)

            file_prefix = file.replace(".csv", "")

            df["product_id"] = [
                f"{file_prefix}_{i}" for i in range(len(df))
            ]

            temp = pd.DataFrame({
                "product_id": df.get("product_id", df.index),
                "product_name": df.get("product_name"),
                "price": df.get("price"),
                "rating": None,
                "description": df.get("details"),
                "image_url": df.get("product_url"),
                "product_url": df.get("link"),
                "platform": "Zara"
            })

            all_data.append(temp)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()




def save_dataset(df):
    processed_dir = os.path.join(BASE_DIR, "processed")
    os.makedirs(processed_dir, exist_ok=True)

    save_path = os.path.join(processed_dir, "final_women_fashion.csv")
    df.to_csv(save_path, index=False)

    print(f"✅ Final dataset saved at {save_path}")


if __name__ == "__main__":

    print("Loading main datasets...")
    fashion, khaadi, kurti = load_datasets()

    print("Standardizing columns...")
    fashion, khaadi, kurti = standardize_columns(fashion, khaadi, kurti)

    print("Merging main datasets...")
    main_merged = merge_datasets(fashion, khaadi, kurti)

    print("Loading women folder datasets...")
    
    # 🔥 OPTION 1: Merge ALL CSVs
    #women_data = load_women_folder()

    # 🔥 OPTION 2: Merge only selected CSVs
    women_data = load_women_folder(selected_files=[ "WORKWEARNEW", "KNITWEAR", "CO-ORD SETS", "BLAZERS", "DRESSES_JUMPSUITS", "JACKETS" , "JEANS" , "SHIRTS" , "SKIRTS" , "TROUSERS" ])

    print("Merging women folder with main dataset...")
    final_df = merge_datasets(main_merged, women_data)

    

    print("Saving final dataset...")
    save_dataset(final_df)

    print("All done! 🚀")
    print(f"Final dataset shape: {final_df.shape}")
    print(final_df.head())