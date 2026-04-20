def test_list_authors(client):
    r = client.get("/authors")
    assert r.status_code == 200
    authors = r.json()
    assert len(authors) == 40
    assert "id" in authors[0]
    assert "citing_paragraphs" in authors[0]


def test_get_author(client):
    r = client.get("/authors/augustine")
    assert r.status_code == 200
    a = r.json()
    assert a["id"] == "augustine"
    assert a["work_count"] > 0


def test_get_author_not_found(client):
    r = client.get("/authors/nobody")
    assert r.status_code == 404


def test_get_author_works(client):
    r = client.get("/authors/augustine/works")
    assert r.status_code == 200
    body = r.json()
    assert body["author_id"] == "augustine"
    assert len(body["works"]) > 0
    work = body["works"][0]
    assert "chapters" in work
    if work["chapters"]:
        chapter = work["chapters"][0]
        assert "sections" in chapter
        if chapter["sections"]:
            section = chapter["sections"][0]
            assert "text" in section
            assert "id" in section


def test_get_author_works_not_found(client):
    r = client.get("/authors/nobody/works")
    assert r.status_code == 404
