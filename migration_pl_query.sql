WITH
  /* Mappings */
  accounts_mapping AS (
    SELECT
      '0' AS "Id",
      NULL AS "MappedId"
    FROM
      SYS.DUMMY
  ),
  cost_centers_mapping AS (
    SELECT
      '0' AS "Id",
      NULL AS "MappedId"
    FROM
      SYS.DUMMY
  ),
  profit_centers_mapping AS (
    SELECT
      '0' AS "Id",
      NULL AS "MappedId"
    FROM
      SYS.DUMMY
  ),
  /* Entries */
  reconciliation_entries AS (
    SELECT
      'P291100000' AS "Account",
      'reconciliation' AS "AccountGroup",
      NULL AS "PrcCode",
      NULL AS "CostCenter",
      NULL AS "ProfitCenter",
      NULL AS "BrandCategory",
      NULL AS "ProductLine",
      NULL AS "CollectionType",
      NULL AS "MaterialClass",
      CAST(JDT1."Debit" - JDT1."Credit" AS BIGINT) * -1 AS "TotalAmount",
      CAST(JDT1."Debit" - JDT1."Credit" AS BIGINT) * -1 AS "SplitAmount",
      JDT1."TransId",
      JDT1."Line_ID",
      JDT1."Account" AS "ItemText",
      TO_VARCHAR (LAST_DAY (JDT1."RefDate"), 'YYYYMMDD') AS "PostingDate"
    FROM
      JDT1
      INNER JOIN OACT ON OACT."AcctCode" = JDT1."Account"
    WHERE
      (JDT1."RefDate" BETWEEN '2026-01-01' AND '2026-03-31') -- Filter by posting date
      AND JDT1."Debit" <> JDT1."Credit" -- Exclude zero-balance lines
      AND JDT1."TransType" NOT IN (-2, -3) -- Exclude opening/closing balance transactions
      AND OACT."GroupMask" IN (4, 5, 6, 7, 8) -- Keep only P&L accounts
  ),
  journal_entries AS (
    SELECT
      COALESCE(am."MappedId", 'NOT MAPPED') AS "Account",
      CASE
        WHEN OACT."GroupMask" = 1 THEN '01 assets'
        WHEN OACT."GroupMask" = 2 THEN '02 liabilities'
        WHEN OACT."GroupMask" = 3 THEN '03 equity'
        WHEN OACT."GroupMask" = 4 THEN '04 revenue'
        WHEN OACT."GroupMask" = 5 THEN '05 cost of goods sold'
        WHEN OACT."GroupMask" = 6 THEN '06 expenses'
        WHEN OACT."GroupMask" = 7 THEN '07 other income'
        WHEN OACT."GroupMask" = 8 THEN '08 other expenses'
      END AS "AccountGroup",
      OCR1."PrcCode",
      ccm."MappedId" AS "CostCenter",
      pcm."MappedId" AS "ProfitCenter",
      CASE
        WHEN pcm."MappedId" IS NOT NULL THEN 'YY'
      END AS "BrandCategory",
      CASE
        WHEN pcm."MappedId" IS NOT NULL THEN 'YY'
      END AS "ProductLine",
      CASE
        WHEN pcm."MappedId" IS NOT NULL THEN 'Y'
      END AS "CollectionType",
      CASE
        WHEN pcm."MappedId" IS NOT NULL THEN 'Y'
      END AS "MaterialClass",
      CAST(JDT1."Debit" - JDT1."Credit" AS BIGINT) AS "TotalAmount",
      ROUND(
        CAST(JDT1."Debit" - JDT1."Credit" AS BIGINT) * COALESCE((OCR1."PrcAmount" / OOCR."OcrTotal"), 1),
        0
      ) AS "SplitAmount",
      JDT1."TransId",
      JDT1."Line_ID",
      JDT1."Account" AS "ItemText",
      TO_VARCHAR (LAST_DAY (JDT1."RefDate"), 'YYYYMMDD') AS "PostingDate"
    FROM
      JDT1
      INNER JOIN OACT ON OACT."AcctCode" = JDT1."Account"
      LEFT JOIN OOCR ON OOCR."OcrCode" = JDT1."ProfitCode"
      LEFT JOIN OCR1 ON OCR1."OcrCode" = OOCR."OcrCode"
      LEFT JOIN accounts_mapping am ON am."Id" = JDT1."Account"
      LEFT JOIN cost_centers_mapping ccm ON ccm."Id" = OCR1."PrcCode"
      LEFT JOIN profit_centers_mapping pcm ON pcm."Id" = OCR1."PrcCode"
    WHERE
      (JDT1."RefDate" BETWEEN '2026-01-01' AND '2026-03-31') -- Filter by posting date
      AND JDT1."Debit" <> JDT1."Credit" -- Exclude zero-balance lines
      AND JDT1."TransType" NOT IN (-2, -3) -- Exclude opening/closing balance transactions
      AND OACT."GroupMask" IN (4, 5, 6, 7, 8) -- Keep only P&L accounts
  ),
  combined_entries AS (
    SELECT
      *,
      NULL AS "RowNumDesc",
      NULL AS "RestAmount"
    FROM
      reconciliation_entries
    UNION ALL
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY
          "TransId",
          "Line_ID"
        ORDER BY
          "PrcCode" DESC
      ) AS "RowNumDesc",
      COALESCE(
        SUM("SplitAmount") OVER (
          PARTITION BY
            "TransId",
            "Line_ID"
          ORDER BY
            "PrcCode" ROWS BETWEEN UNBOUNDED PRECEDING
            AND 1 PRECEDING
        ),
        0
      ) AS "RestAmount"
    FROM
      journal_entries
  ),
  grouped_entries AS (
    SELECT
      ce."PrcCode",
      ce."Account",
      ce."AccountGroup",
      ce."CostCenter",
      ce."ProfitCenter",
      ce."BrandCategory",
      ce."ProductLine",
      ce."CollectionType",
      ce."MaterialClass",
      ce."ItemText",
      ce."PostingDate",
      OADM."MainCurncy" AS "Currency",
      SUM(
        CASE
          WHEN ce."RowNumDesc" = 1 THEN (ce."TotalAmount" - ce."RestAmount")
          ELSE ce."SplitAmount"
        END
      ) AS "Amount"
    FROM
      combined_entries ce
      CROSS JOIN OADM
    GROUP BY
      ce."PrcCode",
      ce."Account",
      ce."AccountGroup",
      ce."CostCenter",
      ce."ProfitCenter",
      ce."BrandCategory",
      ce."ProductLine",
      ce."CollectionType",
      ce."MaterialClass",
      ce."ItemText",
      ce."PostingDate",
      OADM."MainCurncy"
    HAVING
      SUM(
        CASE
          WHEN ce."RowNumDesc" = 1 THEN (ce."TotalAmount" - ce."RestAmount")
          ELSE ce."SplitAmount"
        END
      ) <> 0 -- Exclude zero-balance amount
  )
  /* Main Query */
SELECT
  DENSE_RANK() OVER (
    ORDER BY
      "PostingDate",
      "ItemText"
  ) AS "1_grouping",
  'E930' AS "2_company_code",
  'ZS' AS "3_document_type",
  "PostingDate" AS "4_document_date",
  "PostingDate" AS "5_posting_date",
  NULL AS "6_reverse_date",
  NULL AS "7_currency_date",
  'PL-ACCTS' AS "8_reference",
  'PL-Migration' AS "9_doc_header_text",
  NULL AS "10_local_ledger",
  NULL AS "11_posting_key",
  'S' AS "12_item_type",
  "Account" AS "13_account",
  NULL AS "14_special_gl_indicator",
  "Currency" AS "15_currency",
  NULL AS "16_exchange_rate",
  "Amount" AS "17_amount",
  NULL AS "18_vat_code",
  NULL AS "19_base_amount",
  NULL AS "20_vat_aut_calculation",
  NULL AS "21_tax_aut_calc",
  NULL AS "22_vat_amount",
  NULL AS "23_balancing_acct",
  NULL AS "24_balancing_profit_center",
  NULL AS "25_assignment",
  "ItemText" AS "26_item_text",
  NULL AS "27_mov_type",
  "CostCenter" AS "28_cost_center",
  "ProfitCenter" AS "29_profit_center",
  NULL AS "30_internal_order",
  NULL AS "31_wbe_wbs_element",
  NULL AS "32_plant_site",
  NULL AS "33_material",
  NULL AS "34_quantity",
  NULL AS "35_uom",
  "BrandCategory" AS "36_brand_category",
  "ProductLine" AS "37_product_line",
  "CollectionType" AS "38_collection_type",
  "MaterialClass" AS "39_material_class",
  NULL AS "40_distribution_channel",
  NULL AS "41_geographical_area",
  NULL AS "42_country",
  NULL AS "43_ref_customer",
  NULL AS "44_trading_partner",
  NULL AS "45_reference_key",
  NULL AS "46_key_ref_1",
  NULL AS "47_payment_terms",
  NULL AS "48_baseline_date",
  NULL AS "49_payment_method",
  NULL AS "50_payment_block",
  NULL AS "51_segment",
  NULL AS "52_cross_company",
  NULL AS "53_gl_accnt_999",
  NULL AS "54_prctr_999",
  NULL AS "55_amount_di",
  NULL AS "56_amt_base_di",
  NULL AS "57_date_of_dunning_note",
  NULL AS "58_dunning_level",
  NULL AS "59_base_wt",
  NULL AS "60_base_wt_localc_curr",
  NULL AS "61_amount_wt",
  NULL AS "62_amount_wt_localc_curr",
  NULL AS "63_type_of_wt",
  NULL AS "64_code_of_wt",
  NULL AS "65_payment_reference",
  NULL AS "66_discount_base",
  NULL AS "67_reference_key_2",
  NULL AS "68_invoice_receipt_date",
  "AccountGroup" AS "CHECKAccountGroup",
  "PrcCode" AS "CHECKProfitCode"
FROM
  grouped_entries
ORDER BY
  "PostingDate",
  "ItemText",
  "AccountGroup",
  "PrcCode"
;