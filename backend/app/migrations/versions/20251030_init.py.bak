from alembic import op
import sqlalchemy as sa

revision = "20251030_init"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table("tickers",
        sa.Column("ticker", sa.String(10), primary_key=True),
        sa.Column("name", sa.String(100)),
        sa.Column("sector", sa.String(50)),
        sa.Column("exchange", sa.String(20)),
        sa.Column("market_cap", sa.BigInteger),
        sa.Column("country", sa.String(50))
    )
    op.create_table("prices",
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("close", sa.Numeric(12,4)),
        sa.Column("volume", sa.BigInteger),
        sa.PrimaryKeyConstraint("ticker","date"),
        sa.ForeignKeyConstraint(["ticker"], ["tickers.ticker"])
    )
    op.create_table("signals",
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("ret_1m", sa.Numeric(10,6)),
        sa.Column("ret_3m", sa.Numeric(10,6)),
        sa.Column("ret_6m", sa.Numeric(10,6)),
        sa.Column("rsi_14", sa.Numeric(10,6)),
        sa.Column("atr_14", sa.Numeric(10,6)),
        sa.Column("sma50", sa.Numeric(12,6)),
        sa.Column("sma200", sa.Numeric(12,6)),
        sa.Column("vol_surge", sa.Numeric(12,6)),
        sa.Column("beta_60", sa.Numeric(12,6)),
        sa.Column("m_score", sa.Numeric(12,6)),
        sa.Column("breakout", sa.Boolean()),
        sa.PrimaryKeyConstraint("ticker","date"),
        sa.ForeignKeyConstraint(["ticker"], ["tickers.ticker"])
    )
    op.create_table("index_definitions",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("slug", sa.String(100), unique=True),
        sa.Column("name", sa.String(100)),
        sa.Column("description", sa.Text()),
        sa.Column("rules", sa.JSON()),
        sa.Column("rebalance_freq", sa.String(20)),
        sa.Column("reconst_freq", sa.String(20))
    )
    op.create_table("index_constituents",
        sa.Column("index_id", sa.Integer, nullable=False),
        sa.Column("asof", sa.Date, nullable=False),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("weight", sa.Numeric(10,6)),
        sa.PrimaryKeyConstraint("index_id","asof","ticker"),
        sa.ForeignKeyConstraint(["index_id"], ["index_definitions.id"]),
        sa.ForeignKeyConstraint(["ticker"], ["tickers.ticker"])
    )
    op.create_table("index_history",
        sa.Column("index_id", sa.Integer, nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("level", sa.Numeric(14,6)),
        sa.Column("ret_daily", sa.Numeric(12,6)),
        sa.PrimaryKeyConstraint("index_id","date"),
        sa.ForeignKeyConstraint(["index_id"], ["index_definitions.id"])
    )

def downgrade():
    op.drop_table("index_history")
    op.drop_table("index_constituents")
    op.drop_table("index_definitions")
    op.drop_table("signals")
    op.drop_table("prices")
    op.drop_table("tickers")
