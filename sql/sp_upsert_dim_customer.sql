-- SCD Type 2 upsert for dim_customer.
-- 1) Databricks SQL / Delta version (what a legacy stored procedure usually becomes)

MERGE INTO gold.dim_customer AS t
USING (
  -- rows that changed get two entries: one to close the old version, one to insert the new
  SELECT s.customer_id AS merge_key, s.* FROM silver.customers s
  UNION ALL
  SELECT NULL AS merge_key, s.* FROM silver.customers s
  JOIN gold.dim_customer d
    ON d.customer_id = s.customer_id AND d.is_current
   AND (d.email <> s.email OR d.country <> s.country)
) AS src
ON t.customer_id = src.merge_key AND t.is_current
WHEN MATCHED AND (t.email <> src.email OR t.country <> src.country) THEN
  UPDATE SET t.is_current = false, t.valid_to = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (customer_id, email, country, is_current, valid_from, valid_to)
  VALUES (src.customer_id, src.email, src.country, true, current_timestamp(), NULL);

-- 2) T-SQL stored procedure equivalent (Synapse / SQL Server), for comparison
/*
CREATE OR ALTER PROCEDURE dbo.sp_upsert_dim_customer AS
BEGIN
  SET NOCOUNT ON;
  BEGIN TRAN;
    UPDATE d SET is_current = 0, valid_to = SYSUTCDATETIME()
    FROM dbo.dim_customer d
    JOIN stg.customers s ON s.customer_id = d.customer_id
    WHERE d.is_current = 1 AND (d.email <> s.email OR d.country <> s.country);

    INSERT INTO dbo.dim_customer (customer_id, email, country, is_current, valid_from, valid_to)
    SELECT s.customer_id, s.email, s.country, 1, SYSUTCDATETIME(), NULL
    FROM stg.customers s
    LEFT JOIN dbo.dim_customer d ON d.customer_id = s.customer_id AND d.is_current = 1
    WHERE d.customer_id IS NULL;
  COMMIT;
END;
*/
