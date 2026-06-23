"""
Hierarchical topic classification and gold layer aggregation for YouTube Mundial Trend Radar.

This module:
1. Reads silver tables (videos_daily, comments_current, channels_daily).
2. Builds one text document per video from title, description, tags, search query, and comments.
3. Assigns one broad macro topic per video.
4. Derives specific sub-topics inside each macro topic using TF-IDF n-grams and deterministic clustering.
5. Writes gold tables with backward-compatible topic_id/topic_name aliases for the macro topic.
"""

import argparse
import hashlib
import logging
import math
import os
import re
import unicodedata
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS, TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
SILVER_DATASET = "mundial_trends_silver"
GOLD_DATASET = "mundial_trends_gold"
os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")

TOPIC_RULES = {
    "tourism": {
        "name": "Turismo / Ciudad",
        "keywords": [
            "turismo", "tourism", "hotel", "hoteles", "hotels", "hospedaje",
            "aeropuerto", "airport", "visitantes", "visitors", "travel",
            "viaje", "viajes", "comida", "restaurante", "restaurantes",
            "monterrey", "nuevo leon", "city", "ciudad",
        ],
    },
    "teams": {
        "name": "Selecciones / Partidos",
        "keywords": [
            "seleccion", "seleccion mexicana", "national team", "mexico",
            "world cup", "fifa", "partido", "partidos", "match", "matches",
            "grupo", "group stage", "rival", "rivales", "fans", "fanaticos",
            "futbol",
        ],
    },
    "stadium": {
        "name": "Estadio / Sede",
        "keywords": [
            "estadio", "stadium", "bbva", "estadio bbva", "gigante de acero",
            "sede", "venue", "host venue", "cancha", "rayados", "capacidad",
        ],
    },
    "fan_fest": {
        "name": "Fan Fest",
        "keywords": [
            "fan fest", "fanfest", "festival", "fiesta", "macrofest",
            "evento", "evento gratuito", "free event", "watch party",
            "artistas", "concierto", "transmision",
        ],
    },
    "tickets": {
        "name": "Boletos",
        "keywords": [
            "boleto", "boletos", "ticket", "tickets", "entrada", "entradas",
            "precio", "precios", "price", "prices", "sale", "venta",
            "reventa", "disponibilidad", "fases de venta",
        ],
    },
    "infrastructure": {
        "name": "Infraestructura",
        "keywords": [
            "obra", "obras", "infraestructura", "infrastructure",
            "remodelacion", "renovation", "construccion", "construction",
            "metro", "transporte", "trafico", "traffic", "movilidad",
            "transportation", "transit", "parking", "estacionamiento",
            "rutas", "route", "routes", "seguridad", "security", "safety",
            "policia", "police", "operativo", "aeropuerto",
        ],
    },
}

SPANISH_STOP_WORDS = {
    "a", "al", "algo", "algunas", "algunos", "ante", "antes", "como", "con",
    "contra", "cual", "cuando", "de", "del", "desde", "donde", "durante",
    "e", "el", "ella", "ellas", "ellos", "en", "entre", "era", "eran", "eres",
    "es", "esa", "esas", "ese", "eso", "esos", "esta", "estaba", "estado",
    "estan", "estar", "este", "esto", "estos", "fue", "fueron", "ha", "han",
    "hasta", "hay", "la", "las", "le", "les", "lo", "los", "mas", "me", "mi",
    "mis", "muy", "no", "nos", "o", "para", "pero", "por", "que", "se", "ser",
    "si", "sin", "sobre", "su", "sus", "tambien", "te", "tiene", "todo",
    "todos", "tu", "un", "una", "unas", "uno", "unos", "y", "ya",
}

DOMAIN_STOP_WORDS = {
    "mundial", "mundial2026", "world", "cup", "copa", "mundo", "2026",
    "video", "videos", "shorts", "youtube", "monterrey", "mexico",
    "nuevo", "leon", "fifa",
}

STOP_WORDS = sorted(set(ENGLISH_STOP_WORDS).union(SPANISH_STOP_WORDS).union(DOMAIN_STOP_WORDS))

PROTECTED_TERMS = {
    "monterrey": "Monterrey",
    "estadio bbva": "Estadio BBVA",
    "bbva": "BBVA",
    "fifa": "FIFA",
    "fan fest": "Fan Fest",
    "fanfest": "Fan Fest",
    "rayados": "Rayados",
    "tigres": "Tigres",
    "seleccion mexicana": "Seleccion Mexicana",
    "nuevo leon": "Nuevo Leon",
}

nlp_model = None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process YouTube data through hierarchical topic gold layer."
    )
    parser.add_argument("--run-date", type=str, help="Process single date (YYYY-MM-DD format).")
    parser.add_argument("--start-date", type=str, help="Backfill start date (YYYY-MM-DD format), inclusive.")
    parser.add_argument("--end-date", type=str, help="Backfill end date (YYYY-MM-DD format), inclusive.")
    return parser.parse_args()


def normalize_text(text: Optional[str]) -> str:
    if not text or not isinstance(text, str):
        return ""
    text = text.lower()
    text = "".join(
        c for c in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(c)
    )
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def slugify(text: str, max_len: int = 42) -> str:
    slug = normalize_text(text)
    slug = re.sub(r"[^a-z0-9]+", "_", slug).strip("_")
    return slug[:max_len].strip("_") or "topic"


def load_spacy_model():
    global nlp_model
    if nlp_model is not None:
        return nlp_model
    try:
        import spacy

        nlp_model = spacy.load("es_core_news_sm")
        logger.info("Loaded spaCy model es_core_news_sm successfully.")
        return nlp_model
    except (ImportError, OSError) as exc:
        logger.warning("SpaCy model unavailable (%s). Using TF-IDF and keyword rules.", exc)
        return None


def build_source_text(row: pd.Series, comments_by_video: dict, max_comments: int = 30, max_chars: int = 12000) -> str:
    parts = []
    for field in ["title", "description", "tags", "search_query"]:
        if pd.notna(row.get(field)):
            parts.append(str(row[field]))

    video_id = row.get("video_id")
    if video_id and video_id in comments_by_video:
        for comment in comments_by_video[video_id][:max_comments]:
            if pd.notna(comment):
                parts.append(str(comment))

    return " ".join(parts)[:max_chars]


def macro_seed_text(topic_info: dict) -> str:
    return " ".join([topic_info["name"], *topic_info["keywords"]])


def classify_macro_topic(source_text: str) -> dict:
    normalized_source = normalize_text(source_text)
    candidates = []

    for topic_id, topic_info in TOPIC_RULES.items():
        matched_keywords = []
        score = 0.0
        for keyword in topic_info["keywords"]:
            normalized_keyword = normalize_text(keyword)
            if not normalized_keyword:
                continue
            if re.search(rf"\b{re.escape(normalized_keyword)}\b", normalized_source):
                matched_keywords.append(keyword)
                score += 2.0 if " " in normalized_keyword else 1.0

        if matched_keywords:
            confidence = min(1.0, score / max(3.0, len(topic_info["keywords"]) / 2))
            candidates.append(
                {
                    "topic_id": topic_id,
                    "topic_name": topic_info["name"],
                    "topic_keywords": ", ".join(matched_keywords),
                    "topic_confidence": float(confidence),
                    "topic_method": "macro_keyword_rules_v2",
                    "matched_source": "title,description,tags,search_query,comments",
                    "score": score,
                }
            )

    if candidates:
        return sorted(candidates, key=lambda item: (item["score"], item["topic_confidence"]), reverse=True)[0]

    seed_texts = [macro_seed_text(info) for info in TOPIC_RULES.values()]
    topic_ids = list(TOPIC_RULES.keys())
    try:
        vectorizer = TfidfVectorizer(stop_words=STOP_WORDS, ngram_range=(1, 2), strip_accents="unicode")
        matrix = vectorizer.fit_transform([source_text, *seed_texts])
        similarities = cosine_similarity(matrix[0], matrix[1:]).flatten()
        best_index = int(similarities.argmax())
        best_topic_id = topic_ids[best_index]
        if similarities[best_index] > 0:
            return {
                "topic_id": best_topic_id,
                "topic_name": TOPIC_RULES[best_topic_id]["name"],
                "topic_keywords": "",
                "topic_confidence": float(similarities[best_index]),
                "topic_method": "macro_tfidf_fallback_v2",
                "matched_source": "title,description,tags,search_query,comments",
            }
    except ValueError:
        pass

    return {
        "topic_id": "other",
        "topic_name": "Otros",
        "topic_keywords": "",
        "topic_confidence": 0.0,
        "topic_method": "fallback",
        "matched_source": "none",
    }


def classify_topics(source_text: str) -> list[dict]:
    return [classify_macro_topic(source_text)]


def restore_protected_terms(phrase: str) -> str:
    phrase = phrase.strip(" -_/").lower()
    for normalized, display in sorted(PROTECTED_TERMS.items(), key=lambda item: len(item[0]), reverse=True):
        phrase = re.sub(rf"\b{re.escape(normalized)}\b", display, phrase, flags=re.IGNORECASE)
    return phrase


def readable_subtopic_name(phrases: list[str], representative_titles: list[str]) -> str:
    for phrase in phrases:
        clean_phrase = restore_protected_terms(phrase)
        if len(clean_phrase.split()) >= 2:
            return clean_phrase[:80]

    for title in representative_titles:
        title = re.sub(r"\s+", " ", str(title)).strip()
        if title:
            return title[:80]

    return restore_protected_terms(phrases[0])[:80] if phrases else "Conversacion especifica"


def keyword_set(value: str | list[str]) -> set[str]:
    if isinstance(value, list):
        tokens = value
    else:
        tokens = re.split(r"[,|]", value or "")
    return {normalize_text(token) for token in tokens if normalize_text(token)}


def jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def make_subtopic_id(macro_topic_id: str, keywords: list[str], name: str) -> str:
    signature = "|".join(sorted(keyword_set(keywords))) or normalize_text(name)
    digest = hashlib.sha1(signature.encode("utf-8")).hexdigest()[:8]
    return f"{macro_topic_id}__{slugify(name, 28)}__{digest}"


def reuse_or_create_subtopic_id(
    macro_topic_id: str,
    name: str,
    keywords: list[str],
    previous_subtopics: dict,
    used_ids: set[str],
    threshold: float = 0.45,
) -> str:
    current_keywords = keyword_set(keywords)
    best_id = None
    best_score = 0.0

    for candidate in previous_subtopics.get(macro_topic_id, []):
        if candidate["sub_topic_id"] in used_ids:
            continue
        score = jaccard_similarity(current_keywords, candidate["keywords"])
        if score > best_score:
            best_id = candidate["sub_topic_id"]
            best_score = score

    if best_id and best_score >= threshold:
        used_ids.add(best_id)
        return best_id

    new_id = make_subtopic_id(macro_topic_id, keywords, name)
    suffix = 2
    unique_id = new_id
    while unique_id in used_ids:
        unique_id = f"{new_id}_{suffix}"
        suffix += 1
    used_ids.add(unique_id)
    return unique_id


def choose_cluster_count(doc_count: int) -> int:
    if doc_count <= 2:
        return doc_count
    return max(2, min(5, int(math.ceil(math.sqrt(doc_count)))))


def extract_top_phrases(vectorizer: TfidfVectorizer, matrix, indices: list[int], limit: int = 8) -> list[str]:
    feature_names = vectorizer.get_feature_names_out()
    cluster_matrix = matrix[indices]
    scores = cluster_matrix.mean(axis=0).A1
    ordered_indices = scores.argsort()[::-1]
    phrases = []
    seen = set()
    for index in ordered_indices:
        phrase = feature_names[index].strip()
        normalized = normalize_text(phrase)
        if not normalized or normalized in seen:
            continue
        if len(normalized) <= 2:
            continue
        phrases.append(phrase)
        seen.add(normalized)
        if len(phrases) >= limit:
            break
    return phrases


def derive_subtopics_for_macro(macro_df: pd.DataFrame, previous_subtopics: dict, used_ids: set[str]) -> dict:
    macro_topic_id = str(macro_df.iloc[0]["macro_topic_id"])
    documents = macro_df["source_text"].fillna("").astype(str).tolist()
    doc_count = len(documents)

    if doc_count == 1:
        title = str(macro_df.iloc[0].get("title") or "")
        phrases = extract_phrases_from_small_group(documents[0], title)
        name = readable_subtopic_name(phrases, [title])
        sub_topic_id = reuse_or_create_subtopic_id(macro_topic_id, name, phrases, previous_subtopics, used_ids)
        return {macro_df.index[0]: build_subtopic_payload(sub_topic_id, name, phrases)}

    try:
        vectorizer = TfidfVectorizer(
            stop_words=STOP_WORDS,
            ngram_range=(1, 3),
            min_df=1,
            max_df=0.9,
            strip_accents="unicode",
            token_pattern=r"(?u)\b[a-zA-Z0-9_][\w]+\b",
        )
        matrix = vectorizer.fit_transform(documents)
    except ValueError:
        return derive_fallback_subtopics(macro_df, previous_subtopics, used_ids)

    cluster_count = choose_cluster_count(doc_count)
    if cluster_count <= 1 or matrix.shape[0] < cluster_count:
        labels = [0] * doc_count
    else:
        labels = KMeans(n_clusters=cluster_count, random_state=42, n_init=10).fit_predict(matrix)

    assignments = {}
    for cluster_label in sorted(set(labels)):
        positions = [pos for pos, label in enumerate(labels) if label == cluster_label]
        row_indices = [macro_df.index[pos] for pos in positions]
        phrases = extract_top_phrases(vectorizer, matrix, positions)
        representative_titles = (
            macro_df.iloc[positions]
            .sort_values(["comment_count", "view_count"], ascending=False, na_position="last")
            ["title"]
            .fillna("")
            .astype(str)
            .head(3)
            .tolist()
        )
        name = readable_subtopic_name(phrases, representative_titles)
        sub_topic_id = reuse_or_create_subtopic_id(macro_topic_id, name, phrases, previous_subtopics, used_ids)

        for row_index in row_indices:
            assignments[row_index] = build_subtopic_payload(sub_topic_id, name, phrases)

    return assignments


def extract_phrases_from_small_group(text: str, title: str = "") -> list[str]:
    normalized = normalize_text(f"{title} {text}")
    words = [word for word in normalized.split() if word not in STOP_WORDS and len(word) > 2]
    phrases = []
    for size in [3, 2]:
        phrases.extend(" ".join(words[index:index + size]) for index in range(max(0, len(words) - size + 1)))
    phrases.extend(words)
    counts = Counter(phrases)
    return [phrase for phrase, _ in counts.most_common(8)] or ["conversacion especifica"]


def derive_fallback_subtopics(macro_df: pd.DataFrame, previous_subtopics: dict, used_ids: set[str]) -> dict:
    assignments = {}
    macro_topic_id = str(macro_df.iloc[0]["macro_topic_id"])
    for row_index, row in macro_df.iterrows():
        phrases = extract_phrases_from_small_group(row.get("source_text", ""), row.get("title", ""))
        name = readable_subtopic_name(phrases, [row.get("title", "")])
        sub_topic_id = reuse_or_create_subtopic_id(macro_topic_id, name, phrases, previous_subtopics, used_ids)
        assignments[row_index] = build_subtopic_payload(sub_topic_id, name, phrases)
    return assignments


def build_subtopic_payload(sub_topic_id: str, sub_topic_name: str, keywords: list[str]) -> dict:
    return {
        "sub_topic_id": sub_topic_id,
        "sub_topic_name": sub_topic_name,
        "sub_topic_keywords": ", ".join(restore_protected_terms(keyword) for keyword in keywords[:8]),
    }


def fetch_previous_subtopics(client: bigquery.Client, run_date: str) -> dict:
    query = f"""
    SELECT
        macro_topic_id,
        sub_topic_id,
        sub_topic_name,
        keywords
    FROM `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
    WHERE run_date < DATE('{run_date}')
        AND sub_topic_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY macro_topic_id, sub_topic_id
        ORDER BY run_date DESC
    ) = 1
    """
    try:
        df = client.query(query).to_dataframe()
    except Exception as exc:
        logger.warning("Could not read previous sub-topics for ID stabilization: %s", exc)
        return {}

    previous = {}
    for _, row in df.iterrows():
        macro_topic_id = str(row.get("macro_topic_id") or "")
        previous.setdefault(macro_topic_id, []).append(
            {
                "sub_topic_id": str(row.get("sub_topic_id") or ""),
                "sub_topic_name": str(row.get("sub_topic_name") or ""),
                "keywords": keyword_set(str(row.get("keywords") or "")),
            }
        )
    return previous


def add_subtopics(video_documents_df: pd.DataFrame, previous_subtopics: dict) -> pd.DataFrame:
    if video_documents_df.empty:
        return video_documents_df

    enriched = video_documents_df.copy()
    used_ids = set()
    assignments = {}
    for macro_topic_id, macro_df in enriched.groupby("macro_topic_id", sort=False):
        if macro_topic_id == "other":
            for row_index in macro_df.index:
                assignments[row_index] = {
                    "sub_topic_id": "other__otros",
                    "sub_topic_name": "Otros",
                    "sub_topic_keywords": "",
                }
            continue
        assignments.update(derive_subtopics_for_macro(macro_df, previous_subtopics, used_ids))

    for field in ["sub_topic_id", "sub_topic_name", "sub_topic_keywords"]:
        enriched[field] = enriched.index.map(lambda idx: assignments[idx][field])

    return enriched


def create_gold_tables(client: bigquery.Client):
    video_topics_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{GOLD_DATASET}.video_topics_daily` (
        run_date DATE,
        video_id STRING,
        topic_id STRING,
        topic_name STRING,
        macro_topic_id STRING,
        macro_topic_name STRING,
        sub_topic_id STRING,
        sub_topic_name STRING,
        topic_keywords STRING,
        macro_topic_keywords STRING,
        sub_topic_keywords STRING,
        topic_confidence FLOAT64,
        topic_method STRING,
        matched_source STRING,
        source_text STRING,
        processed_at TIMESTAMP
    )
    PARTITION BY run_date
    CLUSTER BY macro_topic_id, sub_topic_id, video_id
    """

    topic_daily_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{GOLD_DATASET}.topic_daily` (
        run_date DATE,
        topic_id STRING,
        topic_name STRING,
        macro_topic_id STRING,
        macro_topic_name STRING,
        sub_topic_id STRING,
        sub_topic_name STRING,
        keywords STRING,
        representative_videos STRING,
        sample_comments STRING,
        videos_count INT64,
        video_count INT64,
        total_views INT64,
        view_count_total INT64,
        total_likes INT64,
        total_comments INT64,
        comment_count_total INT64,
        avg_views_per_video FLOAT64,
        avg_likes_per_video FLOAT64,
        avg_comments_per_video FLOAT64,
        sentiment_avg FLOAT64,
        relevance_score FLOAT64,
        processed_at TIMESTAMP
    )
    PARTITION BY run_date
    CLUSTER BY macro_topic_id, sub_topic_id
    """

    topic_trends_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{GOLD_DATASET}.topic_trends_daily` (
        run_date DATE,
        topic_id STRING,
        topic_name STRING,
        macro_topic_id STRING,
        macro_topic_name STRING,
        sub_topic_id STRING,
        sub_topic_name STRING,
        keywords STRING,
        videos_count INT64,
        video_count INT64,
        total_views INT64,
        view_count_total INT64,
        total_likes INT64,
        total_comments INT64,
        comment_count_total INT64,
        relevance_score FLOAT64,
        previous_videos_count INT64,
        previous_total_views INT64,
        previous_total_likes INT64,
        previous_total_comments INT64,
        videos_count_change INT64,
        views_change INT64,
        likes_change INT64,
        comments_change INT64,
        processed_at TIMESTAMP
    )
    PARTITION BY run_date
    CLUSTER BY macro_topic_id, sub_topic_id
    """

    channel_daily_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{GOLD_DATASET}.channel_daily` (
        run_date DATE,
        channel_id STRING,
        channel_title STRING,
        videos_count INT64,
        total_views INT64,
        total_likes INT64,
        total_comments INT64,
        subscriber_count INT64,
        total_channel_views INT64,
        total_channel_videos INT64,
        avg_views_per_video FLOAT64,
        avg_comments_per_video FLOAT64,
        processed_at TIMESTAMP
    )
    PARTITION BY run_date
    CLUSTER BY channel_id
    """

    dashboard_summary_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{GOLD_DATASET}.dashboard_summary_daily` (
        run_date DATE,
        videos_monitored INT64,
        channels_monitored INT64,
        topics_detected INT64,
        comments_available INT64,
        total_views INT64,
        total_likes INT64,
        total_comments INT64,
        top_topic_id STRING,
        top_topic_name STRING,
        top_topic_comments INT64,
        top_channel_id STRING,
        top_channel_title STRING,
        top_channel_views INT64,
        processed_at TIMESTAMP
    )
    PARTITION BY run_date
    """

    for ddl in [video_topics_ddl, topic_daily_ddl, topic_trends_ddl, channel_daily_ddl, dashboard_summary_ddl]:
        client.query(ddl).result()

    ensure_hierarchical_columns(client)
    logger.info("Gold tables created or updated.")


def ensure_hierarchical_columns(client: bigquery.Client):
    table_columns = {
        "video_topics_daily": {
            "macro_topic_id": "STRING",
            "macro_topic_name": "STRING",
            "sub_topic_id": "STRING",
            "sub_topic_name": "STRING",
            "macro_topic_keywords": "STRING",
            "sub_topic_keywords": "STRING",
        },
        "topic_daily": {
            "macro_topic_id": "STRING",
            "macro_topic_name": "STRING",
            "sub_topic_id": "STRING",
            "sub_topic_name": "STRING",
            "keywords": "STRING",
            "representative_videos": "STRING",
            "sample_comments": "STRING",
            "video_count": "INT64",
            "view_count_total": "INT64",
            "comment_count_total": "INT64",
            "sentiment_avg": "FLOAT64",
            "relevance_score": "FLOAT64",
        },
        "topic_trends_daily": {
            "macro_topic_id": "STRING",
            "macro_topic_name": "STRING",
            "sub_topic_id": "STRING",
            "sub_topic_name": "STRING",
            "keywords": "STRING",
            "video_count": "INT64",
            "view_count_total": "INT64",
            "comment_count_total": "INT64",
            "relevance_score": "FLOAT64",
        },
    }

    for table_name, columns in table_columns.items():
        for column_name, column_type in columns.items():
            query = f"""
            ALTER TABLE `{PROJECT_ID}.{GOLD_DATASET}.{table_name}`
            ADD COLUMN IF NOT EXISTS {column_name} {column_type}
            """
            client.query(query).result()


def get_dates_to_process(args) -> list[str]:
    if args.run_date:
        return [args.run_date]
    if args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        dates = []
        current = start
        while current <= end:
            dates.append(str(current))
            current += timedelta(days=1)
        return dates
    tz = ZoneInfo("America/Monterrey")
    return [str((datetime.now(tz) - timedelta(days=1)).date())]


def fetch_videos_for_date(client: bigquery.Client, run_date: str) -> pd.DataFrame:
    query = f"""
    SELECT
        run_date, video_id, video_url, title, description, channel_id,
        channel_title, published_at, search_query, category_id, tags,
        default_language, duration_seconds, view_count, like_count,
        comment_count, thumbnail_url, published_after_utc, published_before_utc,
        extracted_at, raw_search_json
    FROM `{PROJECT_ID}.{SILVER_DATASET}.videos_daily`
    WHERE run_date = DATE('{run_date}')
    """
    return client.query(query).to_dataframe()


def fetch_comments_for_date(client: bigquery.Client, run_date: str) -> pd.DataFrame:
    query = f"""
    SELECT
        run_date, video_id, comment_id, parent_id, author_channel_id,
        author_name, comment_text, like_count, published_at, updated_at,
        extracted_at
    FROM `{PROJECT_ID}.{SILVER_DATASET}.comments_current`
    WHERE run_date = DATE('{run_date}')
    """
    return client.query(query).to_dataframe()


def write_video_topics_for_date(client: bigquery.Client, run_date: str, df: pd.DataFrame) -> int:
    delete_query = f"""
    DELETE FROM `{PROJECT_ID}.{GOLD_DATASET}.video_topics_daily`
    WHERE run_date = DATE('{run_date}')
    """
    client.query(delete_query).result()
    logger.info("Deleted existing video_topics for %s.", run_date)

    if df.empty:
        logger.warning("No video topics to insert for %s.", run_date)
        return 0

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
        df,
        f"{PROJECT_ID}.{GOLD_DATASET}.video_topics_daily",
        job_config=job_config,
    )
    job.result()
    logger.info("Inserted %s video topic records for %s.", len(df), run_date)
    return len(df)


def build_topic_daily_for_date(client: bigquery.Client, run_date: str):
    delete_query = f"""
    DELETE FROM `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
    WHERE run_date = DATE('{run_date}')
    """
    client.query(delete_query).result()
    logger.info("Deleted existing topic_daily for %s.", run_date)

    insert_query = f"""
    INSERT INTO `{PROJECT_ID}.{GOLD_DATASET}.topic_daily` (
        run_date, topic_id, topic_name, macro_topic_id, macro_topic_name,
        sub_topic_id, sub_topic_name, keywords, representative_videos,
        sample_comments, videos_count, video_count, total_views, view_count_total,
        total_likes, total_comments, comment_count_total, avg_views_per_video,
        avg_likes_per_video, avg_comments_per_video, sentiment_avg,
        relevance_score, processed_at
    )
    WITH topic_video_metrics AS (
        SELECT
            t.run_date,
            t.topic_id,
            t.topic_name,
            COALESCE(t.macro_topic_id, t.topic_id) AS macro_topic_id,
            COALESCE(t.macro_topic_name, t.topic_name) AS macro_topic_name,
            COALESCE(t.sub_topic_id, CONCAT(t.topic_id, '__general')) AS sub_topic_id,
            COALESCE(t.sub_topic_name, t.topic_name) AS sub_topic_name,
            ANY_VALUE(t.sub_topic_keywords) AS keywords,
            v.video_id,
            ANY_VALUE(v.title) AS title,
            MAX(COALESCE(v.view_count, 0)) AS view_count,
            MAX(COALESCE(v.like_count, 0)) AS like_count,
            MAX(COALESCE(v.comment_count, 0)) AS comment_count
        FROM `{PROJECT_ID}.{GOLD_DATASET}.video_topics_daily` t
        JOIN `{PROJECT_ID}.{SILVER_DATASET}.videos_daily` v
            ON t.video_id = v.video_id AND t.run_date = v.run_date
        WHERE t.run_date = DATE('{run_date}')
        GROUP BY
            t.run_date, t.topic_id, t.topic_name, macro_topic_id, macro_topic_name,
            sub_topic_id, sub_topic_name, v.video_id
    ),
    aggregated AS (
        SELECT
            run_date,
            topic_id,
            topic_name,
            macro_topic_id,
            macro_topic_name,
            sub_topic_id,
            sub_topic_name,
            STRING_AGG(DISTINCT keywords, ', ') AS keywords,
            STRING_AGG(title, ' | ' ORDER BY comment_count DESC, view_count DESC LIMIT 5) AS representative_videos,
            COUNT(DISTINCT video_id) AS videos_count,
            SUM(view_count) AS total_views,
            SUM(like_count) AS total_likes,
            SUM(comment_count) AS total_comments,
            SAFE_DIVIDE(SUM(view_count), COUNT(DISTINCT video_id)) AS avg_views_per_video,
            SAFE_DIVIDE(SUM(like_count), COUNT(DISTINCT video_id)) AS avg_likes_per_video,
            SAFE_DIVIDE(SUM(comment_count), COUNT(DISTINCT video_id)) AS avg_comments_per_video
        FROM topic_video_metrics
        GROUP BY
            run_date, topic_id, topic_name, macro_topic_id, macro_topic_name,
            sub_topic_id, sub_topic_name
    ),
    comment_samples AS (
        SELECT
            COALESCE(t.sub_topic_id, CONCAT(t.topic_id, '__general')) AS sub_topic_id,
            STRING_AGG(c.comment_text, ' | ' ORDER BY c.like_count DESC LIMIT 5) AS sample_comments
        FROM `{PROJECT_ID}.{GOLD_DATASET}.video_topics_daily` t
        JOIN `{PROJECT_ID}.{SILVER_DATASET}.comments_current` c
            ON t.video_id = c.video_id AND t.run_date = c.run_date
        WHERE t.run_date = DATE('{run_date}')
            AND c.comment_text IS NOT NULL
        GROUP BY sub_topic_id
    )
    SELECT
        a.run_date,
        a.topic_id,
        a.topic_name,
        a.macro_topic_id,
        a.macro_topic_name,
        a.sub_topic_id,
        a.sub_topic_name,
        a.keywords,
        a.representative_videos,
        cs.sample_comments,
        a.videos_count,
        a.videos_count AS video_count,
        a.total_views,
        a.total_views AS view_count_total,
        a.total_likes,
        a.total_comments,
        a.total_comments AS comment_count_total,
        a.avg_views_per_video,
        a.avg_likes_per_video,
        a.avg_comments_per_video,
        CAST(NULL AS FLOAT64) AS sentiment_avg,
        CAST(a.total_comments + SAFE_DIVIDE(a.total_views, 1000) + a.videos_count * 5 AS FLOAT64) AS relevance_score,
        CURRENT_TIMESTAMP() AS processed_at
    FROM aggregated a
    LEFT JOIN comment_samples cs
        ON a.sub_topic_id = cs.sub_topic_id
    """
    client.query(insert_query).result()
    logger.info("Built hierarchical topic_daily aggregations for %s.", run_date)


def rebuild_topic_trends(client: bigquery.Client):
    truncate_query = f"""
    DELETE FROM `{PROJECT_ID}.{GOLD_DATASET}.topic_trends_daily`
    WHERE 1=1
    """
    client.query(truncate_query).result()
    logger.info("Truncated topic_trends_daily.")

    insert_query = f"""
    INSERT INTO `{PROJECT_ID}.{GOLD_DATASET}.topic_trends_daily` (
        run_date, topic_id, topic_name, macro_topic_id, macro_topic_name,
        sub_topic_id, sub_topic_name, keywords, videos_count, video_count,
        total_views, view_count_total, total_likes, total_comments,
        comment_count_total, relevance_score, previous_videos_count,
        previous_total_views, previous_total_likes, previous_total_comments,
        videos_count_change, views_change, likes_change, comments_change,
        processed_at
    )
    SELECT
        run_date,
        topic_id,
        topic_name,
        COALESCE(macro_topic_id, topic_id) AS macro_topic_id,
        COALESCE(macro_topic_name, topic_name) AS macro_topic_name,
        COALESCE(sub_topic_id, CONCAT(topic_id, '__general')) AS sub_topic_id,
        COALESCE(sub_topic_name, topic_name) AS sub_topic_name,
        keywords,
        videos_count,
        video_count,
        total_views,
        view_count_total,
        total_likes,
        total_comments,
        comment_count_total,
        relevance_score,
        LAG(videos_count) OVER (PARTITION BY COALESCE(sub_topic_id, CONCAT(topic_id, '__general')) ORDER BY run_date) AS previous_videos_count,
        LAG(total_views) OVER (PARTITION BY COALESCE(sub_topic_id, CONCAT(topic_id, '__general')) ORDER BY run_date) AS previous_total_views,
        LAG(total_likes) OVER (PARTITION BY COALESCE(sub_topic_id, CONCAT(topic_id, '__general')) ORDER BY run_date) AS previous_total_likes,
        LAG(total_comments) OVER (PARTITION BY COALESCE(sub_topic_id, CONCAT(topic_id, '__general')) ORDER BY run_date) AS previous_total_comments,
        videos_count - LAG(videos_count) OVER (PARTITION BY COALESCE(sub_topic_id, CONCAT(topic_id, '__general')) ORDER BY run_date) AS videos_count_change,
        total_views - LAG(total_views) OVER (PARTITION BY COALESCE(sub_topic_id, CONCAT(topic_id, '__general')) ORDER BY run_date) AS views_change,
        total_likes - LAG(total_likes) OVER (PARTITION BY COALESCE(sub_topic_id, CONCAT(topic_id, '__general')) ORDER BY run_date) AS likes_change,
        total_comments - LAG(total_comments) OVER (PARTITION BY COALESCE(sub_topic_id, CONCAT(topic_id, '__general')) ORDER BY run_date) AS comments_change,
        CURRENT_TIMESTAMP() AS processed_at
    FROM `{PROJECT_ID}.{GOLD_DATASET}.topic_daily`
    ORDER BY sub_topic_id, run_date
    """
    client.query(insert_query).result()
    logger.info("Rebuilt topic_trends_daily with sub-topic LAG calculations.")


def build_channel_daily_for_date(client: bigquery.Client, run_date: str):
    delete_query = f"""
    DELETE FROM `{PROJECT_ID}.{GOLD_DATASET}.channel_daily`
    WHERE run_date = DATE('{run_date}')
    """
    client.query(delete_query).result()
    logger.info("Deleted existing channel_daily for %s.", run_date)

    insert_query = f"""
    INSERT INTO `{PROJECT_ID}.{GOLD_DATASET}.channel_daily` (
        run_date, channel_id, channel_title,
        videos_count, total_views, total_likes, total_comments,
        subscriber_count, total_channel_views, total_channel_videos,
        avg_views_per_video, avg_comments_per_video,
        processed_at
    )
    SELECT
        v.run_date,
        v.channel_id,
        v.channel_title,
        COUNT(DISTINCT v.video_id) AS videos_count,
        SUM(v.view_count) AS total_views,
        SUM(v.like_count) AS total_likes,
        SUM(v.comment_count) AS total_comments,
        MAX(c.subscriber_count) AS subscriber_count,
        MAX(c.total_view_count) AS total_channel_views,
        MAX(c.total_video_count) AS total_channel_videos,
        SAFE_DIVIDE(SUM(v.view_count), COUNT(DISTINCT v.video_id)) AS avg_views_per_video,
        SAFE_DIVIDE(SUM(v.comment_count), COUNT(DISTINCT v.video_id)) AS avg_comments_per_video,
        CURRENT_TIMESTAMP() AS processed_at
    FROM `{PROJECT_ID}.{SILVER_DATASET}.videos_daily` v
    LEFT JOIN `{PROJECT_ID}.{SILVER_DATASET}.channels_daily` c
        ON v.channel_id = c.channel_id AND v.run_date = c.run_date
    WHERE v.run_date = DATE('{run_date}')
    GROUP BY v.run_date, v.channel_id, v.channel_title
    """
    client.query(insert_query).result()
    logger.info("Built channel_daily aggregations for %s.", run_date)


def build_dashboard_summary_for_date(client: bigquery.Client, run_date: str):
    delete_query = f"""
    DELETE FROM `{PROJECT_ID}.{GOLD_DATASET}.dashboard_summary_daily`
    WHERE run_date = DATE('{run_date}')
    """
    client.query(delete_query).result()
    logger.info("Deleted existing dashboard_summary for %s.", run_date)

    insert_query = f"""
    INSERT INTO `{PROJECT_ID}.{GOLD_DATASET}.dashboard_summary_daily` (
        run_date, videos_monitored, channels_monitored, topics_detected,
        comments_available, total_views, total_likes, total_comments,
        top_topic_id, top_topic_name, top_topic_comments,
        top_channel_id, top_channel_title, top_channel_views,
        processed_at
    )
    WITH video_stats AS (
        SELECT
            COUNT(DISTINCT v.video_id) AS videos_monitored,
            COUNT(DISTINCT v.channel_id) AS channels_monitored,
            COALESCE(SUM(v.view_count), 0) AS total_views,
            COALESCE(SUM(v.like_count), 0) AS total_likes,
            COALESCE(SUM(v.comment_count), 0) AS total_comments
        FROM `{PROJECT_ID}.{SILVER_DATASET}.videos_daily` v
        WHERE v.run_date = DATE('{run_date}')
    ),
    comment_stats AS (
        SELECT COUNT(DISTINCT c.comment_id) AS comments_available
        FROM `{PROJECT_ID}.{SILVER_DATASET}.comments_current` c
        WHERE c.run_date = DATE('{run_date}')
    ),
    topic_stats AS (
        SELECT COUNT(DISTINCT IF(macro_topic_id != 'other', macro_topic_id, NULL)) AS topics_detected
        FROM `{PROJECT_ID}.{GOLD_DATASET}.video_topics_daily` t
        WHERE t.run_date = DATE('{run_date}')
    ),
    top_topic AS (
        SELECT
            macro_topic_id AS topic_id,
            macro_topic_name AS topic_name,
            SUM(v.comment_count) AS topic_comments
        FROM `{PROJECT_ID}.{GOLD_DATASET}.video_topics_daily` t
        JOIN `{PROJECT_ID}.{SILVER_DATASET}.videos_daily` v
            ON t.video_id = v.video_id AND t.run_date = v.run_date
        WHERE t.run_date = DATE('{run_date}')
            AND macro_topic_id != 'other'
        GROUP BY macro_topic_id, macro_topic_name
        ORDER BY topic_comments DESC
        LIMIT 1
    ),
    top_channel AS (
        SELECT
            c.channel_id,
            c.channel_title,
            SUM(v.view_count) AS channel_views
        FROM `{PROJECT_ID}.{SILVER_DATASET}.videos_daily` v
        JOIN `{PROJECT_ID}.{SILVER_DATASET}.channels_daily` c
            ON v.channel_id = c.channel_id AND v.run_date = c.run_date
        WHERE v.run_date = DATE('{run_date}')
        GROUP BY c.channel_id, c.channel_title
        ORDER BY channel_views DESC
        LIMIT 1
    )
    SELECT
        DATE('{run_date}') AS run_date,
        vs.videos_monitored,
        vs.channels_monitored,
        COALESCE(ts.topics_detected, 0) AS topics_detected,
        COALESCE(cs.comments_available, 0) AS comments_available,
        vs.total_views,
        vs.total_likes,
        vs.total_comments,
        tt.topic_id AS top_topic_id,
        tt.topic_name AS top_topic_name,
        CAST(tt.topic_comments AS INT64) AS top_topic_comments,
        tc.channel_id AS top_channel_id,
        tc.channel_title AS top_channel_title,
        CAST(tc.channel_views AS INT64) AS top_channel_views,
        CURRENT_TIMESTAMP() AS processed_at
    FROM video_stats vs
    CROSS JOIN comment_stats cs
    CROSS JOIN topic_stats ts
    LEFT JOIN top_topic tt ON TRUE
    LEFT JOIN top_channel tc ON TRUE
    """
    client.query(insert_query).result()
    logger.info("Built dashboard_summary for %s.", run_date)


def process_date(client: bigquery.Client, run_date: str):
    logger.info("Processing run_date: %s", run_date)
    videos_df = fetch_videos_for_date(client, run_date)
    comments_df = fetch_comments_for_date(client, run_date)

    if videos_df.empty:
        logger.warning("No videos found for %s. Skipping.", run_date)
        return

    logger.info("Fetched %s videos and %s comments.", len(videos_df), len(comments_df))

    comments_by_video = {}
    if not comments_df.empty:
        for video_id, group in comments_df.groupby("video_id"):
            comments_by_video[video_id] = group.sort_values("like_count", ascending=False)["comment_text"].dropna().tolist()

    load_spacy_model()
    previous_subtopics = fetch_previous_subtopics(client, run_date)
    processed_at = datetime.now(timezone.utc).isoformat()
    video_documents = []

    for _, row in videos_df.iterrows():
        source_text = build_source_text(row, comments_by_video)
        macro_topic = classify_macro_topic(source_text)
        video_documents.append(
            {
                "run_date": row["run_date"],
                "video_id": row["video_id"],
                "title": row.get("title"),
                "view_count": row.get("view_count", 0),
                "comment_count": row.get("comment_count", 0),
                "topic_id": macro_topic["topic_id"],
                "topic_name": macro_topic["topic_name"],
                "macro_topic_id": macro_topic["topic_id"],
                "macro_topic_name": macro_topic["topic_name"],
                "macro_topic_keywords": macro_topic["topic_keywords"],
                "topic_confidence": macro_topic["topic_confidence"],
                "topic_method": macro_topic["topic_method"],
                "matched_source": macro_topic["matched_source"],
                "source_text": source_text,
            }
        )

    documents_df = add_subtopics(pd.DataFrame(video_documents), previous_subtopics)

    topic_records = []
    for _, row in documents_df.iterrows():
        topic_records.append(
            {
                "run_date": row["run_date"],
                "video_id": row["video_id"],
                "topic_id": row["macro_topic_id"],
                "topic_name": row["macro_topic_name"],
                "macro_topic_id": row["macro_topic_id"],
                "macro_topic_name": row["macro_topic_name"],
                "sub_topic_id": row["sub_topic_id"],
                "sub_topic_name": row["sub_topic_name"],
                "topic_keywords": row["sub_topic_keywords"],
                "macro_topic_keywords": row["macro_topic_keywords"],
                "sub_topic_keywords": row["sub_topic_keywords"],
                "topic_confidence": row["topic_confidence"],
                "topic_method": row["topic_method"],
                "matched_source": row["matched_source"],
                "source_text": str(row["source_text"])[:1200] if row["source_text"] else "",
                "processed_at": processed_at,
            }
        )

    topics_df = pd.DataFrame(topic_records)
    write_video_topics_for_date(client, run_date, topics_df)
    logger.info("Processed %s hierarchical topic assignments.", len(topic_records))

    build_topic_daily_for_date(client, run_date)
    build_channel_daily_for_date(client, run_date)
    build_dashboard_summary_for_date(client, run_date)

    logger.info("Completed processing for %s.", run_date)


def main():
    args = parse_args()
    if not PROJECT_ID:
        raise ValueError("Missing GCP_PROJECT_ID environment variable.")
    client = bigquery.Client(project=PROJECT_ID)
    create_gold_tables(client)

    dates = get_dates_to_process(args)
    logger.info("Processing %s date(s): %s", len(dates), dates)

    for run_date in dates:
        try:
            process_date(client, run_date)
        except Exception as exc:
            logger.error("Error processing %s: %s", run_date, exc, exc_info=True)
            raise

    logger.info("Rebuilding topic_trends_daily with LAG calculations...")
    rebuild_topic_trends(client)
    logger.info("Topic gold layer processing completed.")


if __name__ == "__main__":
    main()
