-- =============================================================================
-- DAY 1: Source schema + seed data  (auto-run by the postgres container on
-- first boot via /docker-entrypoint-initdb.d/)
--
-- Mirrors the FinContract Corp story: contracts + the documents our AI
-- chatbot will later embed into Qdrant.
-- =============================================================================

-- The application user we will capture with Debezium (we use superuser 'demo'
-- in the lab; in real life you create a dedicated replication role).

-- ---------------------------------------------------------------------------
-- Table 1: customers
-- ---------------------------------------------------------------------------
CREATE TABLE customers (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT        NOT NULL,
    email       TEXT        NOT NULL UNIQUE,
    city        TEXT        DEFAULT 'Bangalore',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Table 2: contracts  (the money table - updates here MUST reach AI fast)
-- ---------------------------------------------------------------------------
CREATE TABLE contracts (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT      NOT NULL REFERENCES customers(id),
    title           TEXT        NOT NULL,
    monthly_price   NUMERIC(10,2) NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'active',   -- active | cancelled
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Table 3: contract_documents  (text chunks that become vectors later)
-- ---------------------------------------------------------------------------
CREATE TABLE contract_documents (
    id           BIGSERIAL PRIMARY KEY,
    contract_id  BIGINT      NOT NULL REFERENCES contracts(id),
    chunk_text   TEXT        NOT NULL,
    content_hash TEXT        NOT NULL,  -- Day 4 trick: skip re-embedding if hash unchanged
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Seed data: exactly the scenario from the problem statement
-- ---------------------------------------------------------------------------
INSERT INTO customers (name, email, city) VALUES
  ('Acme Manufacturing',  'ops@acme.example',   'Bangalore'),
  ('Zenith Health',       'billing@zenith.example', 'Bangalore');

INSERT INTO contracts (customer_id, title, monthly_price, status) VALUES
  (1, 'Acme - Premium Support Plan', 1200.00, 'active'),   -- id = 1  (remember this)
  (2, 'Zenith - Basic Analytics Plan', 450.00, 'active');

-- A realistic multi-document corpus so the Day-4 token-saving proof has real
-- numbers: full backfill costs the WHOLE corpus, an incremental change costs
-- only the touched chunk(s). Each row is one document the worker will chunk.
INSERT INTO contract_documents (contract_id, chunk_text, content_hash) VALUES
  (1, 'Acme Manufacturing pays $1200 per month for Premium Support with 24x7 SLA. This agreement governs the enterprise support relationship between Acme Manufacturing and the vendor, and it defines the response and resolution commitments that the vendor must meet for severity-one through severity-four incidents. Severity-one incidents are defined as complete service outages affecting production workloads, and they carry a fifteen minute response target with continuous engagement until restoration.', 'hash-acme-001'),
  (1, 'Acme contract renews on 1 Jan 2027. Price escalation cap is 5% per year. Renewal is automatic for successive twelve month terms unless either party provides written notice of non-renewal at least sixty days before the end of the then-current term. Any price increase applied at a renewal checkpoint shall not exceed five percent of the prior annual fee, and the vendor shall provide at least thirty days of advance written notice before the new rate takes effect.', 'hash-acme-002'),
  (1, 'Termination for cause permits either party to exit this agreement if the other party commits a material breach and fails to cure that breach within thirty days of written notice. Either party may also terminate immediately if the other party becomes insolvent, enters bankruptcy, or ceases normal business operations. Upon termination for cause by the customer for vendor breach, the vendor shall refund all prepaid fees for the remainder of the then-current term on a pro-rata basis.', 'hash-acme-003'),
  (1, 'Payment terms for this agreement are net thirty days from the date of invoice. Late payments accrue interest at one and one half percent per month or the maximum rate permitted by law, whichever is lower. The customer shall reimburse the vendor for reasonable costs of collection, including reasonable attorneys fees, for any overdue amounts that remain unpaid after written demand.', 'hash-acme-004'),
  (2, 'Zenith Health pays $450 per month for Basic Analytics with email support. Basic Analytics includes access to the standard dashboard, nightly data refresh, and up to five named user seats. Email support is provided during business hours, Monday through Friday, with a target first response within one business day. Additional seats and premium data retention are available as paid add-ons billed at the rates set out in the order form.', 'hash-zenith-001'),
  (2, 'Zenith Health data is classified as protected health information and is handled under a signed business associate agreement. The vendor shall implement administrative, physical, and technical safeguards consistent with HIPAA, encrypt data at rest and in transit, and limit internal access to personnel with a documented business need. The vendor shall notify the customer of any confirmed breach without undue delay and in no case later than seventy two hours after discovery.', 'hash-zenith-002'),
  (2, 'Zenith Health may terminate this agreement for convenience upon thirty days written notice, in which case the vendor shall refund prepaid fees for unused service on a pro-rata basis. Neither party shall be liable for indirect, incidental, or consequential damages, and each party aggregate liability is capped at the total fees paid or payable in the twelve months preceding the event giving rise to the claim.', 'hash-zenith-003'),
  (2, 'The vendor shall process customer data only on documented instructions from Zenith Health and shall not use customer data for any purpose other than providing the contracted service. Upon expiration or termination, the vendor shall, at the customer election, return customer data in a structured, commonly used format or securely delete it within ninety days, subject to any legal retention obligations that apply.', 'hash-zenith-004');

-- Keep updated_at fresh so the first UPDATE is easy to spot
UPDATE contracts          SET updated_at = now();
UPDATE contract_documents SET updated_at = now();

-- ---------------------------------------------------------------------------
-- REPLICA IDENTITY FULL  (learned the hard way on Day 1!)
-- With the DEFAULT replica identity, an UPDATE event carries only the new
-- row (before = null) and a DELETE carries only the primary key. Day 4's
-- vector worker NEEDS the full OLD row (the 'before' image) to know which
-- stale embedding to delete from Qdrant -> so we opt in per table.
-- Trade-off: larger WAL / more storage. Real systems set this only on the
-- tables downstream consumers actually need history for.
-- ---------------------------------------------------------------------------
ALTER TABLE customers          REPLICA IDENTITY FULL;
ALTER TABLE contracts          REPLICA IDENTITY FULL;
ALTER TABLE contract_documents REPLICA IDENTITY FULL;
