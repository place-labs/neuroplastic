-- Create tables for neuroplastic test models

CREATE TABLE IF NOT EXISTS test_base (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owns TEXT NOT NULL DEFAULT 'all your bases',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS basic (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS goat (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT,
    teeth INTEGER NOT NULL DEFAULT 0,
    job TEXT NOT NULL DEFAULT 'being a goat',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS child_kid (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    age INTEGER NOT NULL DEFAULT 0,
    hoof_treatment TEXT NOT NULL DEFAULT 'oatmeal scrub',
    visits TEXT[] NOT NULL DEFAULT '{}',
    goat_id UUID REFERENCES goat(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
