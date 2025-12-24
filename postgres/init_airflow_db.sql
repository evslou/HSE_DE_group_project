SELECT 'Airflow DB created';

CREATE TABLE IF NOT EXISTS processed_files (
    file_name VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);