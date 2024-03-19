from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float
from sqlalchemy.orm import declarative_base


Base = declarative_base()

# Table Schemas
class Stg_user(Base):
    __tablename__ = 'stg_user'

    user_id = Column('user_id', Integer, primary_key=True)
    username = Column('username', String)
    residence_country_code = Column('residence_country_code', String)
    favorite_fan_token = Column('favorite_fan_token', String)
    updated_at = Column('updated_at', DateTime)

class Str_user_registration(Base):
    __tablename__ = 'str_user_registration'

    user_id = Column('user_id', Integer, primary_key=True)
    registered_at = Column('registered_at', DateTime)
    registration_ip = Column('registration_ip', String)
    affiliate_key = Column('affiliate_key', String)

class Stg_user_kyc(Base):
    __tablename__ = 'stg_user_kyc'

    id = Column('id', Integer, primary_key=True)
    user_id = Column('user_id', Integer)
    kyc_level = Column('kyc_level', String)
    updated_at = Column('updated_at', DateTime)

class Dim_country(Base):
    __tablename__ = 'dim_country'

    primary_key = Column('primary_key', Integer, primary_key=True, autoincrement=True)
    country_code = Column('country_code', String)
    country_name = Column('country_name', String)

class Fact_transaction(Base):
    __tablename__ = 'fact_transaction'

    id = Column('id', String, primary_key=True)
    transaction_timestamp = Column('transaction_timestamp', DateTime)
    user_id = Column('user_id', Integer)
    transaction_type = Column('transaction_type', String)
    token_code = Column('token_code', String)
    quantity = Column('quantity', Integer)
    unit_price_chz = Column('unit_price_chz', Float)

class Dim_conversion_rate(Base):
    __tablename__ = 'dim_conversion_rate'

    primary_key = Column('primary_key', Integer, primary_key=True, autoincrement=True)
    token_code = Column('token_code', String)
    valid_from = Column('valid_from', DateTime)
    valid_to = Column('valid_to', DateTime)
    usd_rate = Column('usd_rate', Float)


#Data Insertion Queries
sql_dim_user = """
CREATE TABLE IF NOT EXISTS dim_user (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR,
    residence_country_code VARCHAR,
    registration_date TIMESTAMP,
    favorite_club VARCHAR,
    current_kyc INTEGER,
    registration_affiliate VARCHAR,
    last_updated_at TIMESTAMP
);

WITH t_user as (
    SELECT
        stg_user.user_id, stg_user.username, stg_user.residence_country_code, stg_user.favorite_fan_token as favorite_club
    FROM
        stg_user
),

t_registration as (
    SELECT str_user_registration.user_id ,str_user_registration.registered_at as registration_date, str_user_registration.affiliate_key as registration_affiliate
    FROM
        str_user_registration
),

t_kyc as (
    SELECT user_id, CAST(kyc_level AS INTEGER) as current_kyc, updated_at as last_updated_at
    FROM
        (SELECT
            stg_user_kyc.user_id, stg_user_kyc.kyc_level, stg_user_kyc.updated_at,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY kyc_level DESC, COALESCE(updated_at, '0001-01-01') DESC) as rank
        FROM
            stg_user_kyc
        )
    WHERE rank = 1
)

INSERT INTO dim_user (username, residence_country_code, registration_date, favorite_club, current_kyc, registration_affiliate, last_updated_at)
SELECT
    t_user.username, t_user.residence_country_code, t_registration.registration_date, t_user.favorite_club, t_kyc.current_kyc, t_registration.registration_affiliate, t_kyc.last_updated_at
FROM
    t_user

LEFT JOIN t_registration
ON t_user.user_id = t_registration.user_id

LEFT JOIN t_kyc
ON t_user.user_id = t_kyc.user_id
;
"""


sql_user_metrics = """
CREATE TABLE IF NOT EXISTS user_metrics (
    primary_key SERIAL PRIMARY KEY,
    username VARCHAR,
    residency_country VARCHAR,
    kyc INTEGER,
    affiliate VARCHAR,
    conversion_date TIMESTAMP,
    conversion_token VARCHAR,
    conversion_amount_usd FLOAT,
    days_to_convert FLOAT,
    last_deposit_date TIMESTAMP,
    most_traded_fan_token VARCHAR,
    favorite_club VARCHAR,
    favorite_club_tokens INTEGER
);

INSERT INTO user_metrics (username, residency_country, kyc, affiliate, conversion_date, conversion_token, conversion_amount_usd, days_to_convert, last_deposit_date, most_traded_fan_token, favorite_club, favorite_club_tokens)
WITH A AS (
    SELECT
        dim_user.user_id,
        dim_user.username,
        dim_country.country_name as residence_country,
        dim_user.current_kyc as kyc,
        dim_user.registration_affiliate as affiliate,
        dim_user.registration_date,
        dim_user.favorite_club,
        favorite_club_tokens.favorite_club_tokens
    FROM
        dim_user
    INNER JOIN dim_country ON dim_user.residence_country_code = dim_country.country_code
    LEFT JOIN (
        SELECT
            user_id,
            favorite_club,
            total_deposits + total_buys - total_sells - total_withdrawals as favorite_club_tokens
        FROM (
            SELECT
                ft.user_id,
                du.favorite_club,
                SUM(CASE WHEN ft.transaction_type = 'D' THEN ft.quantity ELSE 0 END) AS total_deposits,
                SUM(CASE WHEN ft.transaction_type = 'B' THEN ft.quantity ELSE 0 END) AS total_buys,
                SUM(CASE WHEN ft.transaction_type = 'S' THEN ft.quantity ELSE 0 END) AS total_sells,
                SUM(CASE WHEN ft.transaction_type = 'W' THEN ft.quantity ELSE 0 END) AS total_withdrawals
            FROM
                fact_transaction ft
            INNER JOIN dim_user du ON ft.user_id = du.user_id AND ft.token_code = du.favorite_club
            GROUP BY
                ft.user_id,
                du.favorite_club
        )
    ) favorite_club_tokens ON dim_user.user_id = favorite_club_tokens.user_id
),
fan_tokens AS (
    WITH total_fan_tokens AS (
        SELECT
            ft.user_id,
            ft.token_code,
            SUM(CASE WHEN ft.transaction_type = 'B' THEN quantity ELSE 0 END) +
            SUM(CASE WHEN ft.transaction_type = 'S' THEN quantity ELSE 0 END) AS total
        FROM
            fact_transaction ft
        WHERE transaction_type IN ('B', 'S')
        GROUP BY
            ft.user_id, ft.token_code
    )
    SELECT
        user_id,
        token_code as most_traded_fan_token
    FROM (
        SELECT
            user_id,
            token_code,
            total,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY total DESC) AS rank
        FROM
            total_fan_tokens
    )
    WHERE rank = 1
),
first_transaction AS (
    WITH ranked_transactions AS (
        SELECT
            fact_transaction.*,
            ROW_NUMBER() OVER (PARTITION BY fact_transaction.user_id ORDER BY fact_transaction.transaction_timestamp) AS rank
        FROM
            fact_transaction
    )
    SELECT
        rt.user_id,
        rt.transaction_timestamp as first_transaction_timestamp,
        rt.token_code,
        rt.transaction_type,
        rt.quantity,
        rt.unit_price_chz,
        dcr.usd_rate
    FROM
        ranked_transactions rt
    JOIN dim_conversion_rate dcr ON rt.transaction_timestamp BETWEEN dcr.valid_from AND dcr.valid_to
    WHERE rank = 1
),
last_deposit AS (
    WITH ranked_transactions AS (
        SELECT
            fact_transaction.*,
            ROW_NUMBER() OVER (PARTITION BY fact_transaction.user_id ORDER BY fact_transaction.transaction_timestamp DESC) AS rank
        FROM
            fact_transaction
        WHERE transaction_type = 'D'
    )
    SELECT
        user_id,
        transaction_timestamp as last_deposit_timestamp,
        token_code,
        transaction_type,
        quantity,
        unit_price_chz
    FROM
        ranked_transactions
    WHERE
        rank = 1
),
B AS (
    SELECT
        ft.*,
        ld.last_deposit_timestamp
    FROM
        first_transaction AS ft
    JOIN
        last_deposit AS ld ON ft.user_id = ld.user_id
),
C AS (
    SELECT
        dim_conversion_rate.token_code,
        dim_conversion_rate.valid_from,
        dim_conversion_rate.valid_to,
        dim_conversion_rate.usd_rate
    FROM
        dim_conversion_rate
)
SELECT
    A.username,
    A.residence_country,
    A.kyc,
    A.affiliate,
    DATE(B.first_transaction_timestamp) as conversion_date,
    B.token_code as conversion_token,
    ROUND((quantity * unit_price_chz * usd_rate)::numeric, 2) as conversion_amount_usd,
    EXTRACT(DAY FROM (B.first_transaction_timestamp - A.registration_date)) AS days_to_convert,
    DATE(b.last_deposit_timestamp) as last_deposit_date,
    ft.most_traded_fan_token,
    A.favorite_club,
    A.favorite_club_tokens
FROM
    A
LEFT JOIN B ON A.user_id = B.user_id
LEFT JOIN fan_tokens ft ON A.user_id = ft.user_id
ORDER BY A.user_id ASC;
"""
