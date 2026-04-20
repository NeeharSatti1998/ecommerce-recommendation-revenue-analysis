from flask import Flask, request, jsonify
import pandas as pd
import numpy as np
from itertools import combinations
from collections import defaultdict
from scipy.sparse import csr_matrix
from sklearn.neighbors import NearestNeighbors
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import anthropic
import os
from dotenv import load_dotenv
load_dotenv()


client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

def explain_recommendations(customer_id, recommendations, method):
    prompt = f"""You are a retail analytics assistant. A customer with ID {customer_id} has been recommended these products using {method}:

{chr(10).join(f"- {r}" for r in recommendations)}

In 2-3 sentences explain why these products were recommended and what this says about the customer's shopping preferences. Be specific and business-focused. Keep it concise."""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=150,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text


def conversational_recommender(user_message, customer_id=None):
    bought = []
    if customer_id and customer_id in customer_products.index:
        bought = list(customer_products[customer_id])[:10]
    
    prompt = f"""You are a helpful e-commerce shopping assistant for an online retail store that sells home decor, gifts and lifestyle products.

Customer purchase history (if available): {bought if bought else 'No history available'}

Customer message: {user_message}

Based on their message and purchase history, recommend 3-5 specific products from a home decor/gifts store and explain why. Be conversational and helpful. If they mention a budget, stick to it. Keep response under 100 words."""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text


app = Flask(__name__)

print("Loading data...")
df  = pd.read_csv("../data/data_clean.csv", low_memory=False)
df  = df.dropna(subset=["CustomerID"])
df["CustomerID"]      = df["CustomerID"].astype(int)
df["InvoiceDate_ISO"] = pd.to_datetime(df["InvoiceDate_ISO"])
pop = pd.read_csv("../outputs/tableau/product_popularity.csv")
print("Data loaded.")


customer_products = df.groupby("CustomerID")["Description"].apply(set)

print("Building co-purchase matrix...")
co_purchase = defaultdict(int)
for prods in customer_products:
    for a, b in combinations(sorted(prods), 2):
        co_purchase[(a, b)] += 1

product_recs = defaultdict(list)
for (a, b), count in co_purchase.items():
    product_recs[a].append((b, count))
    product_recs[b].append((a, count))
for p in product_recs:
    product_recs[p] = sorted(product_recs[p], key=lambda x: x[1], reverse=True)
print("Rule-based ready.")


print("Building user-item matrix...")
user_item    = df.groupby(["CustomerID","Description"])["Quantity"].sum().unstack(fill_value=0)
matrix       = csr_matrix(user_item.values)
customer_ids = list(user_item.index)
cust_to_idx  = {cid: i for i, cid in enumerate(customer_ids)}

knn_model = NearestNeighbors(metric="cosine", algorithm="brute", n_neighbors=11)
knn_model.fit(matrix)
print("Collaborative filtering ready.")

print("Building TF-IDF matrix...")
products_df  = pop[["Description"]].drop_duplicates().reset_index(drop=True)
tfidf        = TfidfVectorizer(stop_words="english")
tfidf_matrix = tfidf.fit_transform(products_df["Description"])
cosine_sim   = cosine_similarity(tfidf_matrix, tfidf_matrix)
prod_to_idx  = {desc: i for i, desc in enumerate(products_df["Description"])}
print("Content-based ready.")

print("All models ready. Starting API...")

def rule_based(customer_id, top_n=5):
    bought = customer_products.get(customer_id, set())
    scores = defaultdict(int)
    for product in bought:
        for rec, score in product_recs.get(product, [])[:20]:
            if rec not in bought:
                scores[rec] += score
    return [p for p, _ in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]]

def collaborative(customer_id, top_n=5):
    if customer_id not in cust_to_idx:
        return []
    idx                = cust_to_idx[customer_id]
    distances, indices = knn_model.kneighbors(matrix[idx], n_neighbors=11)
    similar_idxs       = indices.flatten()[1:]
    similar_dists      = distances.flatten()[1:]
    bought             = set(user_item.columns[(user_item.iloc[idx] > 0)])
    scores             = defaultdict(float)
    for sim_idx, dist in zip(similar_idxs, similar_dists):
        similarity = 1 - dist
        sim_row    = user_item.iloc[sim_idx]
        for product in sim_row[sim_row > 0].index:
            if product not in bought:
                scores[product] += similarity * sim_row[product]
    return [p for p, _ in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]]

def content_based(customer_id, top_n=5):
    bought = customer_products.get(customer_id, set())
    scores = defaultdict(float)
    for product in bought:
        if product not in prod_to_idx:
            continue
        idx        = prod_to_idx[product]
        sim_scores = sorted(enumerate(cosine_sim[idx]), key=lambda x: x[1], reverse=True)
        for i, score in [s for s in sim_scores if s[0] != idx][:10]:
            rec = products_df["Description"].iloc[i]
            if rec not in bought:
                scores[rec] += score
    return [p for p, _ in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]]


@app.route("/")
def home():
    return jsonify({
        "project": "E-Commerce Recommender API",
        "routes": {
            "/recommend/rule/<customer_id>":           "Rule-based recommendations",
            "/recommend/collaborative/<customer_id>":  "Collaborative filtering",
            "/recommend/content/<customer_id>":        "Content-based recommendations",
            "/recommend/all/<customer_id>":            "All three methods",
            "/chat":                                   "Conversational recommender chatbot (POST)"
        }
    })

@app.route("/recommend/rule/<int:customer_id>")
def recommend_rule(customer_id):
    recs = rule_based(customer_id)
    if not recs:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify({
        "customer_id":     customer_id,
        "method":          "rule_based",
        "recommendations": recs
    })

@app.route("/recommend/collaborative/<int:customer_id>")
def recommend_collaborative(customer_id):
    recs = collaborative(customer_id)
    if not recs:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify({
        "customer_id":     customer_id,
        "method":          "collaborative_filtering",
        "recommendations": recs
    })

@app.route("/recommend/content/<int:customer_id>")
def recommend_content(customer_id):
    recs = content_based(customer_id)
    if not recs:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify({
        "customer_id":     customer_id,
        "method":          "content_based",
        "recommendations": recs
    })


@app.route("/recommend/all/<int:customer_id>")
def recommend_all(customer_id):
    rule = rule_based(customer_id)
    collab = collaborative(customer_id)
    content = content_based(customer_id)
    
    explanation = explain_recommendations(
        customer_id, rule, "rule-based co-purchase analysis"
    )
    
    return jsonify({
        "customer_id":             customer_id,
        "rule_based":              rule,
        "collaborative_filtering": collab,
        "content_based":           content,
        "ai_explanation":          explanation
    })

@app.route("/chat", methods=["POST"])
def chat():
    data       = request.get_json()
    message    = data.get("message", "")
    customer_id = data.get("customer_id", None)
    
    if not message:
        return jsonify({"error": "No message provided"}), 400
    
    response = conversational_recommender(message, customer_id)
    return jsonify({
        "customer_id": customer_id,
        "message":     message,
        "response":    response
    })


if __name__ == "__main__":
    app.run(debug=True)