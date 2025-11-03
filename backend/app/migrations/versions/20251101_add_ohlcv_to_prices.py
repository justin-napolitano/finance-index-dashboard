from alembic import op

revision = "20251101_add_ohlcv_to_prices"
down_revision = "20251030_init"
branch_labels = None
depends_on = None

def upgrade():
    op.execute('ALTER TABLE prices ADD COLUMN IF NOT EXISTS "open" double precision;')
    op.execute('ALTER TABLE prices ADD COLUMN IF NOT EXISTS "high" double precision;')
    op.execute('ALTER TABLE prices ADD COLUMN IF NOT EXISTS "low"  double precision;')
    # NOTE: do not modify PK; existing pk_prices(ticker,date) stays

def downgrade():
    op.execute('ALTER TABLE prices DROP COLUMN IF EXISTS "low";')
    op.execute('ALTER TABLE prices DROP COLUMN IF EXISTS "high";')
    op.execute('ALTER TABLE prices DROP COLUMN IF EXISTS "open";')

