def test_list_books(client):
    r = client.get("/bible/books")
    assert r.status_code == 200
    books = r.json()
    assert len(books) >= 66  # includes deuterocanonical books
    ids = [b["id"] for b in books]
    assert "genesis" in ids
    assert "testament" in books[0]
    assert "citing_paragraphs" in books[0]


def test_get_book(client):
    r = client.get("/bible/books/matthew")
    assert r.status_code == 200
    book = r.json()
    assert book["id"] == "matthew"
    assert book["testament"] == "new"


def test_get_book_not_found(client):
    r = client.get("/bible/books/notabook")
    assert r.status_code == 404


def test_get_chapter_verses(client):
    r = client.get("/bible/books/john/chapters/1")
    assert r.status_code == 200
    body = r.json()
    assert body["book_id"] == "john"
    assert body["chapter"] == 1
    assert body["verse_count"] > 0
    assert "text" in body["verses"][0]
    assert "en" in body["verses"][0]["text"]


def test_get_chapter_verses_invalid_lang(client):
    r = client.get("/bible/books/john/chapters/1?lang=zz")
    assert r.status_code == 422


def test_get_chapter_not_found(client):
    r = client.get("/bible/books/genesis/chapters/9999")
    assert r.status_code == 404
