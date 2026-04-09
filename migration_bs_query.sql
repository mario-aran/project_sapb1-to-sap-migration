WITH
  /* Mappings */
  accounts_mapping AS (
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
      CAST(JDT1."Debit" - JDT1."Credit" AS BIGINT) * -1 AS "Amount",
      JDT1."RefDate",
      JDT1."Account" AS "ItemText"
    FROM
      JDT1
      INNER JOIN OACT ON OACT."AcctCode" = JDT1."Account"
      LEFT JOIN OCRD ON OCRD."CardCode" = JDT1."ShortName"
    WHERE
      JDT1."Debit" <> JDT1."Credit" -- Exclude zero-balance lines
      AND OACT."GroupMask" IN (1, 2, 3) -- Keep only BS accounts
      AND JDT1."Account" NOT LIKE '102%' -- Exclude asset accounts
      AND OCRD."CardCode" IS NULL -- Exclude business partner accounts
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
      CAST(JDT1."Debit" - JDT1."Credit" AS BIGINT) AS "Amount",
      JDT1."RefDate",
      JDT1."Account" AS "ItemText"
    FROM
      JDT1
      INNER JOIN OACT ON OACT."AcctCode" = JDT1."Account"
      LEFT JOIN OCRD ON OCRD."CardCode" = JDT1."ShortName"
      LEFT JOIN accounts_mapping am ON am."Id" = JDT1."Account"
    WHERE
      JDT1."Debit" <> JDT1."Credit" -- Exclude zero-balance lines
      AND OACT."GroupMask" IN (1, 2, 3) -- Keep only BS accounts
      AND JDT1."Account" NOT LIKE '102%' -- Exclude asset accounts
      AND OCRD."CardCode" IS NULL -- Exclude business partner accounts
  ),
  combined_entries AS (
    SELECT
      *
    FROM
      reconciliation_entries
    UNION ALL
    SELECT
      *
    FROM
      journal_entries
  ),
  grouped_entries AS (
    SELECT
      ce."Account",
      ce."ItemText",
      ce."AccountGroup",
      OADM."MainCurncy" AS "Currency",
      SUM(ce."Amount") AS "Amount"
    FROM
      combined_entries ce
      CROSS JOIN OADM
    WHERE
      ce."RefDate" <= '2026-03-31' -- Filter by posting date
    GROUP BY
      ce."Account",
      ce."ItemText",
      ce."AccountGroup",
      OADM."MainCurncy"
    HAVING
      SUM(ce."Amount") <> 0 -- Exclude zero-balance amount
  )
  /* Main Query */
SELECT
  DENSE_RANK() OVER (
    ORDER BY
      "ItemText"
  ) AS "1_grouping",
  'E930' AS "2_company_code",
  'ZS' AS "3_document_type",
  '20260331' AS "4_document_date",
  '20260331' AS "5_posting_date",
  NULL AS "6_reverse_date",
  NULL AS "7_currency_date",
  'BS-ACCTS' AS "8_reference",
  'BS-Migration' AS "9_doc_header_text",
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
  NULL AS "28_cost_center",
  NULL AS "29_profit_center",
  NULL AS "30_internal_order",
  NULL AS "31_wbe_wbs_element",
  NULL AS "32_plant_site",
  NULL AS "33_material",
  NULL AS "34_quantity",
  NULL AS "35_uom",
  NULL AS "36_brand_category",
  NULL AS "37_product_line",
  NULL AS "38_collection_type",
  NULL AS "39_material_class",
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
  "AccountGroup" AS "CHECKAccountGroup"
FROM
  grouped_entries
ORDER BY
  "ItemText",
  "AccountGroup"
;