
-- Создание таблиц для SberAuto Analytics
CREATE TABLE IF NOT EXISTS sessions_historical (
    session_id VARCHAR,
    client_id VARCHAR,
    visit_date DATE,
    visit_time TIME,
    visit_number INTEGER,
    utm_source VARCHAR,
    utm_medium VARCHAR,
    utm_campaign VARCHAR,
    utm_adcontent VARCHAR,
    utm_keyword VARCHAR,
    device_category VARCHAR,
    device_os VARCHAR,
    device_brand VARCHAR,
    device_model VARCHAR,
    device_screen_resolution VARCHAR,
    device_browser VARCHAR,
    geo_country VARCHAR,
    geo_city VARCHAR
);

CREATE TABLE IF NOT EXISTS hits_historical (
    session_id VARCHAR,
    hit_date DATE,
    hit_time INTEGER,
    hit_number INTEGER,
    hit_type VARCHAR,
    hit_referer VARCHAR,
    hit_page_path VARCHAR,
    event_category VARCHAR,
    event_action VARCHAR,
    event_label VARCHAR,
    event_value INTEGER
);

CREATE TABLE IF NOT EXISTS sessions_incremental (
    session_id VARCHAR,
    client_id VARCHAR,
    visit_date DATE,
    visit_time TIME,
    visit_number INTEGER,
    utm_source VARCHAR,
    utm_medium VARCHAR,
    utm_campaign VARCHAR,
    utm_adcontent VARCHAR,
    utm_keyword VARCHAR,
    device_category VARCHAR,
    device_os VARCHAR,
    device_brand VARCHAR,
    device_model VARCHAR,
    device_screen_resolution VARCHAR,
    device_browser VARCHAR,
    geo_country VARCHAR,
    geo_city VARCHAR,
    source_file VARCHAR,
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hits_incremental (
    session_id VARCHAR,
    hit_date DATE,
    hit_time INTEGER,
    hit_number INTEGER,
    hit_type VARCHAR,
    hit_referer VARCHAR,
    hit_page_path VARCHAR,
    event_category VARCHAR,
    event_action VARCHAR,
    event_label VARCHAR,
    event_value INTEGER,
    source_file VARCHAR,
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_files (
    file_name VARCHAR PRIMARY KEY,
    file_size INTEGER,
    records_processed INTEGER,
    processed_at TIMESTAMP,
    md5_hash VARCHAR
);

-- Создание представлений
CREATE VIEW IF NOT EXISTS sessions_combined AS
SELECT 
    session_id, client_id, visit_date, visit_time, visit_number,
    utm_source, utm_medium, utm_campaign, utm_adcontent, utm_keyword,
    device_category, device_os, device_brand, device_model,
    device_screen_resolution, device_browser, geo_country, geo_city
FROM sessions_historical
UNION ALL
SELECT 
    session_id, client_id, visit_date, visit_time, visit_number,
    utm_source, utm_medium, utm_campaign, utm_adcontent, utm_keyword,
    device_category, device_os, device_brand, device_model,
    device_screen_resolution, device_browser, geo_country, geo_city
FROM sessions_incremental;

CREATE VIEW IF NOT EXISTS hits_combined AS
SELECT 
    session_id, hit_date, hit_time, hit_number, hit_type,
    hit_referer, hit_page_path, event_category, event_action,
    event_label, event_value
FROM hits_historical
UNION ALL
SELECT 
    session_id, hit_date, hit_time, hit_number, hit_type,
    hit_referer, hit_page_path, event_category, event_action,
    event_label, event_value
FROM hits_incremental;
