import json
import re
import sys
from typing import Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup, Tag

URL = "https://tarajm.com/people/10109"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
    )
}


# -------------------------
# Helpers
# -------------------------
def clean_key(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("：", ":")
    s = re.sub(r"\s+", " ", s)
    return s[:-1].strip() if s.endswith(":") else s


def text_clean(el: Optional[Tag]) -> str:
    if not el:
        return ""
    txt = el.get_text(" ", strip=True)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def abs_url(href: str) -> str:
    if not href:
        return href
    return "https://tarajm.com" + href if href.startswith("/") else href


def dedup_links(links: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for x in links:
        t = (x.get("text") or "").strip()
        h = (x.get("href") or "").strip()
        if not t or not h:
            continue
        key = (t, h)
        if key in seen:
            continue
        seen.add(key)
        out.append({"text": t, "href": h})
    return out


def dedup_paragraphs(txt: str) -> str:
    lines = [l.strip() for l in txt.split("\n") if l.strip()]
    out = []
    for l in lines:
        if not out or out[-1] != l:
            out.append(l)
    return "\n".join(out)


# -------------------------
# Extraction primitives
# -------------------------
def extract_links(container: Tag) -> List[Dict[str, str]]:
    """Return list of links (text+href) found inside container."""
    links = []
    if not container:
        return links
    for a in container.find_all("a", href=True):
        t = a.get_text(strip=True)
        href = abs_url(a.get("href", ""))
        if t and href:
            links.append({"text": t, "href": href})
    return dedup_links(links)


def extract_value_from_row(row: Tag, label_span: Tag) -> Dict[str, Any]:
    """
    Given a row container and its label span, return value:
    - list of links if any
    - else cleaned text of the value container
    """
    value_container = None

    # Prefer next sibling tag after label span
    sib = label_span.find_next_sibling()
    if isinstance(sib, Tag):
        value_container = sib

    # Fallback to parent row
    if value_container is None:
        value_container = row

    # Links
    links = extract_links(value_container)
    if links:
        raw_txt = text_clean(value_container)
        raw_txt = re.sub(r"عرض الكل\s*\(\d+\)", "", raw_txt).strip()
        return {"type": "links", "items": links, "text": raw_txt}

    # Text-only
    val_txt = text_clean(value_container)
    val_txt = re.sub(r"عرض الكل\s*\(\d+\)", "", val_txt).strip()
    return {"type": "text", "text": val_txt}


# -------------------------
# Page structure finders
# -------------------------
def get_main_container(soup: BeautifulSoup) -> Tag:
    """
    Restrict parsing to main content to avoid footer/header pollution.
    """
    main = soup.find("main")
    if main:
        return main
    # Next.js roots often use __next
    root = soup.select_one("#__next")
    return root if root else soup


def find_person_summary_section(root: Tag) -> Optional[Tag]:
    # Most pages have this stable anchor:
    sec = root.select_one('section[aria-labelledby="person-summary-heading"]')
    if sec:
        return sec

    # Fallback: find heading text and locate nearest section
    h2 = root.find(lambda t: isinstance(t, Tag) and t.name in ("h2", "h3") and text_clean(t) == "ملخص الشخصية")
    if not h2:
        return None
    return h2.find_parent("section") or h2.parent


def find_translation_section(root: Tag) -> Optional[Tag]:
    h2 = root.find(lambda t: isinstance(t, Tag) and t.name == "h2" and text_clean(t) == "الترجمة")
    if not h2:
        return None
    return h2.find_parent("section") or h2.parent


# -------------------------
# Main extractor: Summary fields
# -------------------------
def extract_summary_fields(summary_section: Tag) -> Dict[str, Any]:
    """
    Extract fields grouped by h3 headings.
    - Labeled rows: spans that end with ":"  -> key/value
    - Link-only groups (مثل: الصفات والتصنيفات): store as {"items": {"type":"links","items":[...]}}
    """
    if not summary_section:
        return {}

    result: Dict[str, Any] = {}

    # Collect all h3 headings inside summary section
    h3s = summary_section.find_all("h3")
    if not h3s:
        # Fallback: older layout - attempt label-based walk
        result.setdefault("general", {})
        for sp in summary_section.find_all("span"):
            label = text_clean(sp)
            if not label.endswith(":"):
                continue
            key = clean_key(label)
            row = sp.find_parent("div") or sp.find_parent("li") or sp.parent
            if not row:
                continue
            val = extract_value_from_row(row, sp)
            result["general"][key] = val
        return {k: v for k, v in result.items() if v}

    for idx, h3 in enumerate(h3s):
        group_name = text_clean(h3) or f"group_{idx+1}"
        bucket: Dict[str, Any] = {}

        # Collect sibling nodes after this h3 until the next h3
        nodes: List[Tag] = []
        for sib in h3.next_siblings:
            if isinstance(sib, Tag) and sib.name == "h3":
                break
            if isinstance(sib, Tag):
                nodes.append(sib)

        # 1) Labeled extraction in this group chunk
        for node in nodes:
            for sp in node.find_all("span"):
                label = text_clean(sp)
                if not label.endswith(":"):
                    continue
                key = clean_key(label)

                row = sp.find_parent("div") or sp.find_parent("li") or sp.parent
                if not row:
                    continue

                value = extract_value_from_row(row, sp)

                if key in bucket:
                    if not isinstance(bucket[key], list):
                        bucket[key] = [bucket[key]]
                    bucket[key].append(value)
                else:
                    bucket[key] = value

        # 2) If no labeled keys but has links -> store as items (for الصفات والتصنيفات)
        if not bucket:
            all_links: List[Dict[str, str]] = []
            for node in nodes:
                all_links.extend(extract_links(node))
            all_links = dedup_links(all_links)
            if all_links:
                bucket["items"] = {"type": "links", "items": all_links}

        if bucket:
            result[group_name] = bucket

    # Remove empties
    result = {g: v for g, v in result.items() if v}
    return result


# -------------------------
# Translation extractor
# -------------------------
def extract_translation(root: Tag) -> str:
    section = find_translation_section(root)
    if not section:
        return ""

    # The page often includes multiple variants (print/desktop).
    # Prefer visible prose blocks; collect and dedup.
    prose_blocks = section.select(".print\\:hidden .prose")
    if not prose_blocks:
        prose_blocks = section.select(".prose")

    texts = []
    for pb in prose_blocks:
        t = pb.get_text("\n", strip=True)
        t = dedup_paragraphs(t)
        if t:
            texts.append(t)

    # Deduplicate identical blocks
    uniq = []
    seen = set()
    for t in texts:
        if t in seen:
            continue
        seen.add(t)
        uniq.append(t)

    # If multiple remain, join with a separator
    return "\n\n".join(uniq).strip()


# -------------------------
# Optional: page sections list (debugging)
# -------------------------
def extract_page_sections(root: Tag) -> List[str]:
    titles = []
    for h in root.find_all(["h1", "h2", "h3"]):
        t = text_clean(h)
        if t:
            titles.append(t)
    # Dedup keep order
    seen = set()
    out = []
    for t in titles:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


# -------------------------
# Main
# -------------------------
def main():
    url = URL
    if len(sys.argv) > 1 and sys.argv[1].strip():
        url = sys.argv[1].strip()

    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    root = get_main_container(soup)

    # Core page info
    h1 = root.find("h1") or soup.find("h1")
    name = text_clean(h1)

    short_summary = ""
    if h1:
        p = h1.find_next("p")
        short_summary = text_clean(p)

    # Summary fields
    summary_sec = find_person_summary_section(root) or find_person_summary_section(soup)
    fields = extract_summary_fields(summary_sec) if summary_sec else {}

    # Translation
    translation = extract_translation(root) or extract_translation(soup)

    data = {
        "url": url,
        "name": name,
        "summary": short_summary,
        "fields": fields,
        "translation": translation,
        "page_sections": extract_page_sections(root),
    }

    # Save JSON (use person id from URL)
    person_id = url.rstrip("/").split("/")[-1]
    out_path = f"tarajm_{person_id}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("Saved:", out_path)
    print("Name:", name)
    print("Top groups:", list(fields.keys()))

    # Quick check for traits/categories group
    if "الصفات والتصنيفات" in fields:
        print("Traits/Categories captured ✅")
    else:
        print("Traits/Categories missing ❌")


if __name__ == "__main__":
    main()
