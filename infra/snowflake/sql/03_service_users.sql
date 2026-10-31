-- 03: key-pair service users (no passwords). Public keys come from the environment.
-- Generate a key pair:
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256 -inform PEM -out svc_dbt.p8
--   openssl rsa -in svc_dbt.p8 -pubout -out svc_dbt.pub
--   grep -v '^-----' svc_dbt.pub | tr -d '\n'     # paste into DBT_RSA_PUBLIC_KEY
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS SVC_AIRBYTE TYPE = SERVICE DEFAULT_ROLE = LOADER      DEFAULT_WAREHOUSE = LOADING_WH   COMMENT = 'Airbyte destination';
CREATE USER IF NOT EXISTS SVC_DBT     TYPE = SERVICE DEFAULT_ROLE = TRANSFORMER DEFAULT_WAREHOUSE = TRANSFORM_WH COMMENT = 'dbt / Dagster';
CREATE USER IF NOT EXISTS SVC_BI      TYPE = SERVICE DEFAULT_ROLE = REPORTER    DEFAULT_WAREHOUSE = BI_WH        COMMENT = 'Streamlit / BI';

ALTER USER SVC_AIRBYTE SET RSA_PUBLIC_KEY = '${AIRBYTE_RSA_PUBLIC_KEY}';
ALTER USER SVC_DBT     SET RSA_PUBLIC_KEY = '${DBT_RSA_PUBLIC_KEY}';
ALTER USER SVC_BI      SET RSA_PUBLIC_KEY = '${BI_RSA_PUBLIC_KEY}';

GRANT ROLE LOADER      TO USER SVC_AIRBYTE;
GRANT ROLE TRANSFORMER TO USER SVC_DBT;
GRANT ROLE REPORTER    TO USER SVC_BI;
