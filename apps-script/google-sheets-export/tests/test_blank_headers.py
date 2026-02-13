def test_blank_header_detection():
    headers = ["name", "", "price"]
    clean_headers = [h.strip() for h in headers]
    blank_cols = [i+1 for i,h in enumerate(clean_headers) if not h]

    assert blank_cols == [2]