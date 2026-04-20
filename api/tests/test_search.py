def test_search_ccc(client):
    r = client.get("/search?q=Trinity")
    assert r.status_code == 200
    body = r.json()
    assert body["query"] == "Trinity"
    assert body["count"] > 0
    assert "snippet" in body["results"][0]


def test_search_ccc_invalid_lang(client):
    r = client.get("/search?q=Trinity&lang=zz")
    assert r.status_code == 422


def test_search_ccc_bilingual(client):
    r = client.get("/search?q=Trinity&bilingual=true")
    assert r.status_code == 200
    body = r.json()
    assert "translations" in body["results"][0]


def test_search_bible(client):
    r = client.get("/search/bible?q=love")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0
    result = body["results"][0]
    assert "book_id" in result
    assert "chapter" in result
    assert "verse" in result


def test_search_bible_invalid_lang(client):
    r = client.get("/search/bible?q=love&lang=zz")
    assert r.status_code == 422


def test_search_patristic(client):
    r = client.get("/search/patristic?q=grace")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0
    assert "section_id" in body["results"][0]


def test_search_patristic_invalid_lang(client):
    r = client.get("/search/patristic?q=grace&lang=zz")
    assert r.status_code == 422


def test_search_too_short(client):
    r = client.get("/search?q=a")
    assert r.status_code == 422


def test_search_lemma(client):
    r = client.get("/search/lemma?q=love&lang=la")
    assert r.status_code == 200
    body = r.json()
    assert "lemmas_matched" in body
    assert "results" in body


def test_search_lemma_invalid_lang(client):
    r = client.get("/search/lemma?q=love&lang=zz")
    assert r.status_code == 422
