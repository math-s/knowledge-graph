def test_search_lexicon_latin(client):
    r = client.get("/lexicon/la?q=deus")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0
    assert "lemma" in body["results"][0]


def test_search_lexicon_greek(client):
    r = client.get("/lexicon/el?q=word")  # matches definition_en
    assert r.status_code == 200
    assert r.json()["count"] > 0


def test_search_lexicon_invalid_lang(client):
    r = client.get("/lexicon/zz?q=deus")
    assert r.status_code == 422


def test_get_entry_by_id(client):
    r = client.get("/lexicon/la/entry?id=deus")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == "deus"
    assert "occurrences" in body


def test_get_entry_by_lemma(client):
    # Lemmas are stored with diacritics (e.g. "dĕus") — use FTS to find a matchable lemma
    r = client.get("/lexicon/la?q=love&limit=1")
    assert r.status_code == 200
    result = r.json()["results"][0]
    r2 = client.get(f"/lexicon/la/entry?id={result['id']}")
    assert r2.status_code == 200


def test_get_entry_missing_params(client):
    r = client.get("/lexicon/la/entry")
    assert r.status_code == 400


def test_get_entry_not_found(client):
    r = client.get("/lexicon/la/entry?id=notaword999")
    assert r.status_code == 404


def test_hapax(client):
    r = client.get("/lexicon/el/hapax?corpus=nt&limit=5")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0
    assert len(body["results"]) == 5
    assert "lemma" in body["results"][0]


def test_hapax_invalid_lang(client):
    r = client.get("/lexicon/zz/hapax")
    assert r.status_code == 422


def test_vocab(client):
    r = client.get("/lexicon/la/vocab?corpus=all&limit=10")
    assert r.status_code == 200
    body = r.json()
    assert len(body["results"]) == 10
    assert body["results"][0]["count"] >= body["results"][-1]["count"]


def test_doctrine_chain(client):
    r = client.get("/lexicon/la/deus/doctrine")
    assert r.status_code == 200
    body = r.json()
    assert body["lemma_id"] == "deus"
    assert body["total_paragraphs"] > 0
    para = body["paragraphs"][0]
    assert "id" in para
    assert "source_count" in para
    assert para["source_count"] >= 1
    assert "source_ids" in para
    assert "text_en" in para


def test_doctrine_chain_invalid_lang(client):
    r = client.get("/lexicon/zz/deus/doctrine")
    assert r.status_code == 422


def test_doctrine_chain_not_found(client):
    r = client.get("/lexicon/la/notaword999/doctrine")
    assert r.status_code == 404


def test_doctrine_chain_source_filter(client):
    r = client.get("/lexicon/la/deus/doctrine?source_type=bible-verse&limit=5")
    assert r.status_code == 200
    body = r.json()
    assert body["total_paragraphs"] > 0
    for para in body["paragraphs"]:
        assert all("bible-verse" in sid for sid in para["source_ids"])


def test_occurrences(client):
    r = client.get("/lexicon/la/occurrences?id=deus&limit=5")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0
    assert len(body["results"]) <= 5
    assert "text" in body["results"][0]
