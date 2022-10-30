-- super useful to have the log of past HTTP requests available as a queryable table!
-- we can do retry logic e.g. status code and last time of retrieval
-- also can put in support for API auth and stuff like bearer tokens that 
-- end up in the headers.
CREATE TABLE http_request(
    local_path_response_body varchar,
    -- local path that the body is written to. 
    -- XXX: there is anecdotal evidence of pathological behavior by the sqlite-http
    -- extension (or perhaps the SQLite query planner) unless the response_body is
    -- written out to a file? Not sure if this is because the 'blob' nature of the
    -- column is not visible to SQLite because of the way the temp table is created.
    response_body_bytes_written int,
    request_url TEXT,
    -- URL that is request
    request_method TEXT,
    -- HTTP method used in request
    request_headers TEXT,
    -- Request HTTP headers, in wire format
    request_cookies TEXT,
    -- Cookies sent in request (unstable)
    request_body BLOB,
    -- Body sent in request
    response_status TEXT,
    -- Status text of the response ("200 OK")
    response_status_code INT,
    -- HTTP status code of the response (100-999)
    response_headers TEXT,
    -- Response HTTP headers, in wire format
    response_cookies TEXT,
    -- Cookies received in response (unstable)
    response_body BLOB,
    -- Body received in response
    remote_address TEXT,
    -- IP address of responding server
    timings TEXT,
    -- JSON of various event timestamps
    meta TEXT, -- Metadata of request
    url_template_family varchar NULL,
    url_template_name varchar NULL    
);
