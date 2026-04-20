import pytest


def test_graph_stats(client):
    r = client.get("/graph/stats")
    assert r.status_code == 200
    body = r.json()
    assert body["total_nodes"] > 0
    assert body["total_edges"] > 0
    assert "nodes_by_type" in body


def test_list_themes(client):
    r = client.get("/graph/themes")
    assert r.status_code == 200
    themes = r.json()
    assert len(themes) == 15


def test_graph_by_theme(client):
    r = client.get("/graph/themes")
    theme_id = r.json()[0]["id"]
    r2 = client.get(f"/graph/theme/{theme_id}")
    assert r2.status_code == 200
    body = r2.json()
    assert body["total_seeds"] > 0
    assert "nodes" in body
    assert "edges" in body


def test_graph_by_paragraph(client):
    r = client.get("/graph/paragraph/1")
    assert r.status_code == 200
    body = r.json()
    assert body["paragraph"] == 1
    assert body["node_count"] > 0


def test_graph_by_paragraph_missing(client):
    r = client.get("/graph/paragraph/99999")
    assert r.status_code == 200
    assert r.json()["nodes"] == []


def test_graph_connect_too_few(client):
    r = client.get("/graph/connect?sources=p:1")
    assert r.status_code == 422


def test_graph_connect(client):
    r = client.get("/graph/connect?sources=p:1,p:2")
    assert r.status_code == 200
    body = r.json()
    assert body["seed_count"] == 2


def test_list_entities(client):
    r = client.get("/graph/entities")
    assert r.status_code == 200
    entities = r.json()
    assert len(entities) == 89


def test_graph_by_entity(client):
    r = client.get("/graph/entities")
    entity_id = r.json()[0]["id"]
    r2 = client.get(f"/graph/entity/{entity_id}")
    assert r2.status_code == 200
    body = r2.json()
    assert body["total_seeds"] > 0


def test_list_topics(client):
    r = client.get("/graph/topics")
    assert r.status_code == 200
    assert len(r.json()) > 0


def test_graph_filter_no_filters(client):
    r = client.get("/graph/filter")
    assert r.status_code == 200
    assert r.json()["total_seeds"] == 0


def test_graph_path_same_node(client):
    r = client.get("/graph/path?source=p:1&target=p:1")
    assert r.status_code == 422


def test_graph_path_not_found_node(client):
    r = client.get("/graph/path?source=p:1&target=p:99999999")
    assert r.status_code == 404


def test_graph_path_direct_neighbors(client):
    # p:1 and p:2 are adjacent CCC paragraphs and share citations/entities
    r = client.get("/graph/path?source=p:1&target=p:2")
    assert r.status_code == 200
    body = r.json()
    assert body["found"] is True
    assert body["hops"] >= 1
    assert body["path"][0]["id"] == "p:1"
    assert body["path"][-1]["id"] == "p:2"
    assert len(body["edges"]) == body["hops"]


def test_graph_path_cross_type(client):
    # p:1 → some non-paragraph node (e.g., a Bible verse it cites)
    r = client.get("/graph/path?source=p:1&target=p:100&max_hops=4")
    assert r.status_code == 200
    body = r.json()
    assert body["found"] is True
    assert body["path"][0]["id"] == "p:1"
    assert body["path"][-1]["id"] == "p:100"
