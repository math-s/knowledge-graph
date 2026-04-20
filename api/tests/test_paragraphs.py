def test_list_paragraphs(client):
    r = client.get("/paragraphs")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 2865
    assert len(body["paragraphs"]) == 50
    assert "id" in body["paragraphs"][0]
    assert "text" in body["paragraphs"][0]


def test_list_paragraphs_pagination(client):
    r = client.get("/paragraphs?page=2&limit=10")
    assert r.status_code == 200
    body = r.json()
    assert body["page"] == 2
    assert len(body["paragraphs"]) == 10


def test_list_paragraphs_theme_filter(client):
    # Get a real theme ID first
    themes = client.get("/graph/themes").json()
    theme_id = themes[0]["id"]
    r = client.get(f"/paragraphs?theme={theme_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0


def test_get_paragraph(client):
    r = client.get("/paragraphs/1")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == 1
    assert "en" in body["text"]
    assert "cross_references" in body
    assert "bible_citations" in body


def test_get_paragraph_not_found(client):
    r = client.get("/paragraphs/99999")
    assert r.status_code == 404


def test_get_paragraph_full(client):
    r = client.get("/paragraphs/29/full")
    assert r.status_code == 200
    body = r.json()
    assert "sources_bible" in body
    assert "sources_patristic" in body
    assert "sources_documents" in body
    assert "sources_authors" in body
    assert "cross_references" in body
    # paragraph 29 has graph bible-verse edges
    assert len(body["sources_bible"]) > 0
    verse = body["sources_bible"][0]
    assert "book_id" in verse
    assert "text" in verse
    assert "en" in verse["text"]


def test_get_paragraph_siblings_by_citation(client):
    r = client.get("/paragraphs/2/siblings?by=citation")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0
    sib = body["siblings"][0]
    assert "id" in sib
    assert sib["edge_type"] == "shared_citation"
    assert "overlap_count" in sib
    assert "text_en" in sib


def test_get_paragraph_siblings_by_entity(client):
    r = client.get("/paragraphs/11/siblings?by=entity&limit=5")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0
    assert body["siblings"][0]["edge_type"] == "shared_entity"


def test_get_paragraph_siblings_both(client):
    r = client.get("/paragraphs/2/siblings?by=both&limit=10")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0


def test_get_paragraph_siblings_invalid_by(client):
    r = client.get("/paragraphs/2/siblings?by=invalid")
    assert r.status_code == 422


def test_get_paragraph_siblings_not_found(client):
    r = client.get("/paragraphs/99999/siblings")
    assert r.status_code == 404


def test_get_paragraph_full_not_found(client):
    r = client.get("/paragraphs/99999/full")
    assert r.status_code == 404


def test_list_paragraph_parts(client):
    r = client.get("/paragraphs/parts")
    assert r.status_code == 200
    parts = r.json()
    assert len(parts) == 2865
    assert "id" in parts[0]
    assert "part" in parts[0]
