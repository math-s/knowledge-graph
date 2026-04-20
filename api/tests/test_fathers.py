def test_list_fathers_pages(client):
    r = client.get("/fathers")
    assert r.status_code == 200
    pages = r.json()
    assert len(pages) == 424
    assert "id" in pages[0]
    assert "title" in pages[0]


def test_get_fathers_page_root(client):
    r = client.get("/fathers/0101")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == "0101"
    assert body["parent_id"] is None
    assert "text" in body
    assert "children" in body


def test_get_fathers_page_with_children(client):
    r = client.get("/fathers/0103")
    assert r.status_code == 200
    body = r.json()
    assert len(body["children"]) > 0
    assert "id" in body["children"][0]
    assert "title" in body["children"][0]


def test_get_fathers_page_not_found(client):
    r = client.get("/fathers/notapage")
    assert r.status_code == 404
