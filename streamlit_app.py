"""Streamlit FE — one page: search bar + result grid.

Calls the Query Coordinator's GET /search only; it does no embedding or
OpenSearch access itself. On EC2 the coordinator runs on the same host
(localhost:8000); docker-compose overrides HS_API_URL to the API service name.

Run:
    uv run streamlit run streamlit_app.py --server.port 8501 --server.address 0.0.0.0
"""
from __future__ import annotations

import os

import requests
import streamlit as st

API_URL = os.getenv("HS_API_URL", "http://localhost:8000").rstrip("/")

st.set_page_config(page_title="Hybrid Image Search", page_icon="🔎", layout="wide")
st.title("🔎 Hybrid Image Search")
st.caption("BM25(키워드) + kNN(시맨틱) 하이브리드 검색")

with st.sidebar:
    st.header("검색 옵션")
    size = st.slider("결과 개수 (top-k)", min_value=1, max_value=50, value=12)
    n_cols = st.slider("그리드 열 수", min_value=2, max_value=6, value=4)
    tune = st.checkbox("BM25/시맨틱 가중치 직접 조정", value=False)
    w_bm25 = w_knn = None
    if tune:
        w_bm25 = st.slider("BM25 (키워드) 가중치", 0.0, 1.0, 0.4, 0.05)
        w_knn = st.slider("kNN (시맨틱) 가중치", 0.0, 1.0, 0.6, 0.05)
    st.divider()
    st.caption(f"BE: {API_URL}")

query = st.text_input(
    "검색어", placeholder="예: two dogs playing on the beach", label_visibility="collapsed"
)


def _search(q: str) -> dict:
    params: dict = {"q": q, "size": size}
    if w_bm25 is not None:
        params["w_bm25"] = w_bm25
        params["w_knn"] = w_knn
    resp = requests.get(f"{API_URL}/search", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


if query:
    with st.spinner("검색 중…"):
        try:
            data = _search(query)
        except Exception as e:  # noqa: BLE001 - any backend/transport error -> UI message
            st.error(f"검색 실패: {e}")
            st.stop()

    results = data.get("results", [])
    st.caption(f"'{data.get('query', query)}' — {len(results)}건")
    if not results:
        st.info("결과가 없습니다.")

    cols = st.columns(n_cols)
    for i, hit in enumerate(results):
        with cols[i % n_cols]:
            url = hit.get("image_url")
            if url:
                st.image(url, use_container_width=True)
            else:
                st.write("(이미지 URL 없음)")
            desc = hit.get("description") or ""
            st.caption(desc)
            st.write(f"**score** {hit.get('score', 0.0):.4f}")
