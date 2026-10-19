CREATE TABLE IF NOT EXISTS asset_utilization_daily (
    snapshot_date DATE NOT NULL,
    department TEXT NOT NULL,
    total_assets BIGINT NOT NULL,
    allocated_assets BIGINT NOT NULL,
    available_assets BIGINT NOT NULL,
    utilization_rate NUMERIC(5, 2) NOT NULL,
    PRIMARY KEY (snapshot_date, department)
);

CREATE TABLE IF NOT EXISTS maintenance_summary_monthly (
    snapshot_month DATE NOT NULL,
    department TEXT NOT NULL,
    request_count BIGINT NOT NULL,
    completed_count BIGINT NOT NULL,
    total_cost NUMERIC(12, 2) NOT NULL,
    average_cost NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (snapshot_month, department)
);

