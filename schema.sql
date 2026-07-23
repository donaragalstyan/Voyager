-- ============================================================
-- AI Trip Planner — Postgres schema
-- Days 1-2: flight_catalog, flight_prices, hotel_catalog,
-- hotel_prices, tasks, worker_types
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto"; -- for gen_random_uuid()

-- ------------------------------------------------------------
-- ENUM types
-- ------------------------------------------------------------

CREATE TYPE task_status AS ENUM (
    'pending',
    'in_progress',
    'completed',
    'failed',
    'skipped'
);

CREATE TYPE cabin_class AS ENUM (
    'economy',
    'premium_economy',
    'business',
    'first'
);

-- ------------------------------------------------------------
-- worker_types
-- Registry of agent/worker types and the Kafka topic each
-- one consumes from. Referenced by tasks.worker_type.
-- ------------------------------------------------------------

CREATE TABLE worker_types (
    worker_type     TEXT PRIMARY KEY,           -- e.g. 'planner', 'flight_catalog', 'hotel_catalog',
                                                 -- 'interest_matching', 'logistics', 'supervisor'
    description     TEXT NOT NULL,
    kafka_topic     TEXT NOT NULL UNIQUE,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------
-- flight_catalog
-- Static route/flight data. Seeded once from Kaggle dataset,
-- rarely changes.
-- ------------------------------------------------------------

CREATE TABLE flight_catalog (
    flight_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    airline             TEXT NOT NULL,
    flight_number       TEXT NOT NULL,
    origin_airport      CHAR(3) NOT NULL,        -- IATA code
    destination_airport CHAR(3) NOT NULL,        -- IATA code
    departure_time      TIME NOT NULL,           -- scheduled local departure time
    arrival_time        TIME NOT NULL,           -- scheduled local arrival time
    duration_minutes    INTEGER NOT NULL CHECK (duration_minutes > 0),
    stops               SMALLINT NOT NULL DEFAULT 0 CHECK (stops >= 0),
    cabin_class         cabin_class NOT NULL DEFAULT 'economy',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (airline, flight_number, origin_airport, destination_airport, cabin_class)
);

CREATE INDEX idx_flight_catalog_route ON flight_catalog (origin_airport, destination_airport);

-- ------------------------------------------------------------
-- flight_prices
-- Dynamic price data, refreshed on a schedule by the
-- Lambda + EventBridge job. Kept separate from catalog so
-- price refreshes don't touch static route data.
-- ------------------------------------------------------------

CREATE TABLE flight_prices (
    flight_price_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flight_id        UUID NOT NULL REFERENCES flight_catalog(flight_id) ON DELETE CASCADE,
    travel_date      DATE NOT NULL,
    price            NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    currency         CHAR(3) NOT NULL DEFAULT 'USD',
    last_refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (flight_id, travel_date)
);

CREATE INDEX idx_flight_prices_flight_id ON flight_prices (flight_id);
CREATE INDEX idx_flight_prices_travel_date ON flight_prices (travel_date);

-- ------------------------------------------------------------
-- hotel_catalog
-- Static hotel/listing data. Seeded once (synthetic or from
-- a dataset), rarely changes.
-- ------------------------------------------------------------

CREATE TABLE hotel_catalog (
    hotel_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    city            TEXT NOT NULL,
    country         TEXT NOT NULL,
    address         TEXT,
    star_rating     SMALLINT CHECK (star_rating BETWEEN 1 AND 5),
    latitude        NUMERIC(9, 6),
    longitude       NUMERIC(9, 6),
    amenities       TEXT[] NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_hotel_catalog_city ON hotel_catalog (city);

-- ------------------------------------------------------------
-- hotel_prices
-- Dynamic price data, refreshed on a schedule by the
-- Lambda + EventBridge job.
-- ------------------------------------------------------------

CREATE TABLE hotel_prices (
    hotel_price_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hotel_id          UUID NOT NULL REFERENCES hotel_catalog(hotel_id) ON DELETE CASCADE,
    stay_date         DATE NOT NULL,              -- night of the stay
    price_per_night   NUMERIC(10, 2) NOT NULL CHECK (price_per_night >= 0),
    currency          CHAR(3) NOT NULL DEFAULT 'USD',
    last_refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (hotel_id, stay_date)
);

CREATE INDEX idx_hotel_prices_hotel_id ON hotel_prices (hotel_id);
CREATE INDEX idx_hotel_prices_stay_date ON hotel_prices (stay_date);

-- ------------------------------------------------------------
-- tasks
-- The orchestrator's task graph. Each row is one subtask
-- dispatched to a worker via Kafka. Dependencies are modeled
-- as a self-referencing array of task ids that must complete
-- first (simple + queryable without a join table).
-- ------------------------------------------------------------

CREATE TABLE tasks (
    task_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    goal_id         UUID NOT NULL,               -- groups all subtasks for one user request
    worker_type     TEXT NOT NULL REFERENCES worker_types(worker_type),
    status          task_status NOT NULL DEFAULT 'pending',
    depends_on      UUID[] NOT NULL DEFAULT '{}', -- task_ids that must complete first
    payload         JSONB NOT NULL DEFAULT '{}',  -- input for the worker
    result          JSONB,                        -- output once completed
    error_message   TEXT,                         -- populated on failure
    retry_count     SMALLINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_tasks_goal_id ON tasks (goal_id);
CREATE INDEX idx_tasks_status ON tasks (status);
CREATE INDEX idx_tasks_worker_type ON tasks (worker_type);

-- keep updated_at current on every row change
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_tasks_updated_at
BEFORE UPDATE ON tasks
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- ------------------------------------------------------------
-- Seed worker_types with the agents in the plan
-- ------------------------------------------------------------

INSERT INTO worker_types (worker_type, description, kafka_topic) VALUES
    ('planner',           'Decomposes a goal into a subtask graph',                 'planner-tasks'),
    ('flight_catalog',    'Reads cached flight routes/prices from Postgres',        'flight-catalog-tasks'),
    ('hotel_catalog',     'Reads cached hotel listings/prices from Postgres',       'hotel-catalog-tasks'),
    ('interest_matching', 'Matches stated preferences to activities',               'interest-matching-tasks'),
    ('logistics',         'Checks feasibility/conflicts in a day plan',             'logistics-tasks'),
    ('supervisor',        'Merges outputs into one budget-constrained plan',        'supervisor-tasks');
