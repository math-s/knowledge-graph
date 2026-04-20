def test_search_encyclopedia(client):
    r = client.get("/encyclopedia?q=grace")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0
    assert "title" in body["results"][0]
    assert "snippet" in body["results"][0]


def test_search_encyclopedia_too_short(client):
    r = client.get("/encyclopedia?q=a")
    assert r.status_code == 422


def test_get_article(client):
    r = client.get("/encyclopedia/13407a")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == "13407a"
    assert body["title"] == "Salvation"
    assert body["text"]


def test_get_article_not_found(client):
    r = client.get("/encyclopedia/notarticle")
    assert r.status_code == 404


def test_get_related_articles(client):
    r = client.get("/encyclopedia/13407a/related")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0
    article = body["related"][0]
    assert "id" in article
    assert "direction" in article
    assert article["direction"] in ("inbound", "outbound")


def test_get_article_paragraphs(client):
    r = client.get("/encyclopedia/13407a/paragraphs")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0
    para = body["paragraphs"][0]
    assert "id" in para
    assert "text_en" in para


def test_get_paragraph_encyclopedia(client):
    # paragraph 169 has a discussed_in edge to ency:13407a
    r = client.get("/paragraphs/169/encyclopedia")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0
    assert any(a["id"] == "13407a" for a in body["articles"])


def test_get_paragraph_encyclopedia_not_found(client):
    r = client.get("/paragraphs/99999/encyclopedia")
    assert r.status_code == 404
