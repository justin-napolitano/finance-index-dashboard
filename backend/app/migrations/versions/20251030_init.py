from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

# Revision identifiers, used by Alembic.
revision = "20251030_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # --- core entities ---
    op.create_table(
        "tickers",
        sa.Column("ticker", sa.String(10), primary_key=True),
        sa.Column("name", sa.String(200), nullable=True),
        sa.Column("sector", sa.String(50), nullable=True),
        sa.Column("exchange", sa.String(20), nullable=True),
        sa.Column("market_cap", sa.BigInteger, nullable=True),
        sa.Column("country", sa.String(50), nullable=True),
    )

    op.create_table(
        "prices",
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("close", sa.Numeric(12, 4), nullable=True),
        sa.Column("volume", sa.BigInteger, nullable=True),
        sa.PrimaryKeyConstraint("ticker", "date", name="pk_prices"),
        sa.ForeignKeyConstraint(
            ["ticker"], ["tickers.ticker"], name="fk_prices_ticker", ondelete="CASCADE"
        ),
    )
    # PK covers (ticker,date); add a helper index for time-window scans
    op.create_index("ix_prices_date", "prices", ["date"], unique=False)

    op.create_table(
        "signals",
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("ret_1m", sa.Numeric(10, 6), nullable=True),
        sa.Column("ret_3m", sa.Numeric(10, 6), nullable=True),
        sa.Column("ret_6m", sa.Numeric(10, 6), nullable=True),
        sa.Column("rsi_14", sa.Numeric(10, 6), nullable=True),
        sa.Column("atr_14", sa.Numeric(10, 6), nullable=True),
        sa.Column("sma50", sa.Numeric(12, 6), nullable=True),
        sa.Column("sma200", sa.Numeric(12, 6), nullable=True),
        sa.Column("vol_surge", sa.Numeric(12, 6), nullable=True),
        sa.Column("beta_60", sa.Numeric(12, 6), nullable=True),
        sa.Column("m_score", sa.Numeric(12, 6), nullable=True),
        sa.Column("breakout", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.PrimaryKeyConstraint("ticker", "date", name="pk_signals"),
        sa.ForeignKeyConstraint(
            ["ticker"], ["tickers.ticker"], name="fk_signals_ticker", ondelete="CASCADE"
        ),
    )
    op.create_index("ix_signals_date", "signals", ["date"], unique=False)

    op.create_table(
        "index_definitions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("slug", sa.String(100), nullable=False, unique=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("rules", pg.JSONB, nullable=True),
        sa.Column("rebalance_freq", sa.String(20), nullable=True),
        sa.Column("reconst_freq", sa.String(20), nullable=True),
    )
    op.create_index(
        "ix_index_definitions_slug", "index_definitions", ["slug"], unique=True
    )

    op.create_table(
        "index_constituents",
        sa.Column("index_id", sa.Integer, nullable=False),
        sa.Column("asof", sa.Date, nullable=False),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("weight", sa.Numeric(10, 6), nullable=True),
        sa.PrimaryKeyConstraint("index_id", "asof", "ticker", name="pk_index_constituents"),
        sa.ForeignKeyConstraint(
            ["index_id"], ["index_definitions.id"],
            name="fk_index_constituents_index",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["ticker"], ["tickers.ticker"], name="fk_index_constituents_ticker", ondelete="CASCADE"
        ),
    )
    op.create_index(
        "ix_index_constituents_index_asof", "index_constituents", ["index_id", "asof"], unique=False
    )

    op.create_table(
        "index_history",
        sa.Column("index_id", sa.Integer, nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("level", sa.Numeric(14, 6), nullable=True),
        sa.Column("ret_daily", sa.Numeric(12, 6), nullable=True),
        sa.PrimaryKeyConstraint("index_id", "date", name="pk_index_history"),
        sa.ForeignKeyConstraint(
            ["index_id"], ["index_definitions.id"],
            name="fk_index_history_index",
            ondelete="CASCADE",
        ),
    )
    # PK already covers (index_id, date); add a date-only index if you query by date across indices
    op.create_index("ix_index_history_date", "index_history", ["date"], unique=False)


def downgrade():
    op.drop_index("ix_index_history_date", table_name="index_history")
    op.drop_table("index_history")
    op.drop_index("ix_index_constituents_index_asof", table_name="index_constituents")
    op.drop_table("index_constituents")
    op.drop_index("ix_index_definitions_slug", table_name="index_definitions")
    op.drop_table("index_definitions")
    op.drop_index("ix_signals_date", table_name="signals")
    op.drop_table("signals")
    op.drop_index("ix_prices_date", table_name="prices")
    op.drop_table("prices")
    op.drop_table("tickers")

