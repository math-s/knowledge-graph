def test_list_parts(client):
    r = client.get("/summa/parts")
    assert r.status_code == 200
    parts = r.json()
    assert len(parts) == 7
    assert parts[0]["name"] == "Prima Pars"
    assert parts[0]["question_count"] > 0


def test_get_part(client):
    r = client.get("/summa/parts/1")
    assert r.status_code == 200
    body = r.json()
    assert body["num"] == 1
    assert len(body["questions"]) > 0
    assert "title" in body["questions"][0]


def test_get_part_not_found(client):
    r = client.get("/summa/parts/99")
    assert r.status_code == 404


def test_get_question(client):
    r = client.get("/summa/questions/1001")
    assert r.status_code == 200
    body = r.json()
    assert str(body["id"]) == "1001"  # stored as TEXT in DB
    assert len(body["articles"]) > 0
    assert "title" in body["articles"][0]
    assert "text" in body["articles"][0]


def test_get_question_not_found(client):
    r = client.get("/summa/questions/99999")
    assert r.status_code == 404


def test_get_article(client):
    r = client.get("/summa/articles/1001:1")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == "1001:1"
    assert "title" in body
    assert "text" in body


def test_get_article_not_found(client):
    r = client.get("/summa/articles/9999:99")
    assert r.status_code == 404
