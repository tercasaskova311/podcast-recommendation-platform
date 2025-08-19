# file: demo_knn_json.py
import json
import numpy as np
from sentence_transformers import SentenceTransformer

# ---------------- CONFIG ----------------
TRANSCRIPTS_PATH = "transcripts.json"
HISTORY_PATH = "history_vectors.json"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
TOP_K = 3

# --------- 1) Load JSONs ---------
with open(TRANSCRIPTS_PATH, "r", encoding="utf-8") as f:
    transcripts = json.load(f)

with open(HISTORY_PATH, "r", encoding="utf-8") as f:
    history = json.load(f)

print(f"[INFO] Loaded {len(transcripts)} transcripts and {len(history)} historical vectors.")

# --------- 2) Embed new transcripts ---------
texts = [t["transcript"] for t in transcripts]
ids = [t["episode_id"] for t in transcripts]

print(f"[INFO] Loading embedding model: {MODEL_NAME}")
model = SentenceTransformer(MODEL_NAME, device="cpu")

X = model.encode(texts, normalize_embeddings=True)  # shape (n, d)
X = np.array(X, dtype="float32")

# --------- 3) Prepare history matrix ---------
H_ids = [h["episode_id"] for h in history]
H = np.array([h["embedding"] for h in history], dtype="float32")

# Normalize to unit length (cosine sim)
H_norm = np.linalg.norm(H, axis=1, keepdims=True)
H_norm[H_norm == 0] = 1.0
H = H / H_norm

# --------- 4) Compute Top-K neighbors ---------
results = []
for i, qid in enumerate(ids):
    sims = H.dot(X[i])                     # cosine similarity
    k = min(TOP_K, sims.shape[0])
    idx = np.argpartition(-sims, k-1)[:k]  # top-k indices
    idx = idx[np.argsort(-sims[idx])]      # sort by similarity
    for j in idx:
        results.append({
            "new_episode_id": qid,
            "historical_episode_id": H_ids[j],
            "cosine_similarity": float(sims[j])
        })

# --------- 5) Save results ---------
with open("similarities.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2)

print(f"[INFO] Saved {len(results)} similarity pairs → similarities.json")
