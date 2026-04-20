def test_list_documents(client):
    r = client.get("/documents")
    assert r.status_code == 200
    docs = r.json()
    assert len(docs) == 39
    assert "id" in docs[0]
    assert "available_langs" in docs[0]
    assert "citing_paragraphs" in docs[0]


def test_get_document(client):
    r = client.get("/documents/lumen-gentium")
    assert r.status_code == 200
    doc = r.json()
    assert doc["id"] == "lumen-gentium"
    assert isinstance(doc["fetchable"], bool)


def test_get_document_not_found(client):
    r = client.get("/documents/notadoc")
    assert r.status_code == 404


def test_get_document_sections(client):
    r = client.get("/documents/lumen-gentium/sections")
    assert r.status_code == 200
    body = r.json()
    assert body["section_count"] > 0
    assert "sections" in body


def test_get_document_sections_not_found(client):
    r = client.get("/documents/notadoc/sections")
    assert r.status_code == 404
