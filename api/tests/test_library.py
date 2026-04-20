def test_list_library(client):
    r = client.get("/library")
    assert r.status_code == 200
    docs = r.json()
    assert len(docs) == 196
    assert "id" in docs[0]
    assert "category" in docs[0]


def test_list_library_filter_docs(client):
    r = client.get("/library?category=docs")
    assert r.status_code == 200
    docs = r.json()
    assert len(docs) == 187
    assert all(d["category"] == "docs" for d in docs)


def test_list_library_filter_almanac(client):
    r = client.get("/library?category=almanac")
    assert r.status_code == 200
    docs = r.json()
    assert len(docs) == 9


def test_list_library_invalid_category(client):
    r = client.get("/library?category=notacategory")
    assert r.status_code == 422


def test_list_categories(client):
    r = client.get("/library/categories")
    assert r.status_code == 200
    cats = r.json()
    assert any(c["category"] == "docs" for c in cats)


def test_get_library_doc(client):
    r = client.get("/library/docs_bo08us")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == "docs_bo08us"
    assert body["text"]


def test_get_library_doc_not_found(client):
    r = client.get("/library/notadoc")
    assert r.status_code == 404
