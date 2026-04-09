WITH
  /* Mappings */
  payment_terms_mapping AS (
    SELECT
      '0' AS "Id",
      NULL AS "MappedId"
    FROM
      SYS.DUMMY
  ),
  /* Documents */
  marketing_documents AS (
    SELECT
      *
    FROM
      OPCH -- A/P Invoice
    UNION ALL
    SELECT
      *
    FROM
      ORPC -- A/P Credit Memo
  ),
  /* Entries */
  reconciliation_entries AS (
    SELECT
      'S' AS "ItemType",
      'P291100002' AS "Account",
      'reconciliation' AS "AccountGroup",
      NULL AS "BaselineDate",
      NULL AS "PaymentTerms",
      NULL AS "PaymentMethod",
      OCRD."CardCode" AS "ItemText",
      REPLACE (
        REPLACE (
          TO_VARCHAR (
            CAST(
              CASE
                WHEN JDT1."FCCurrency" IS NOT NULL THEN JDT1."BalFcDeb" - JDT1."BalFcCred"
                ELSE JDT1."BalDueDeb" - JDT1."BalDueCred"
              END AS DECIMAL(19, 2)
            ) * -1
          ),
          '.00',
          ''
        ),
        '.',
        ','
      ) AS "Amount",
      CAST(
        CASE
          WHEN JDT1."FCCurrency" IS NOT NULL THEN JDT1."BalDueDeb" - JDT1."BalDueCred"
        END AS BIGINT
      ) * -1 AS "AmountDI",
      CAST(JDT1."BalDueDeb" - JDT1."BalDueCred" AS BIGINT) * -1 AS "AmountCLP",
      JDT1."TransId",
      JDT1."Line_ID",
      OCRD."CardCode",
      TO_VARCHAR (JDT1."TaxDate", 'YYYYMMDD') AS "DocumentDate",
      COALESCE(JDT1."FCCurrency", OADM."MainCurncy") AS "Currency",
      CASE
        WHEN md."TransId" IS NOT NULL THEN md."FolioPref" || '-' || md."FolioNum"
        ELSE TO_VARCHAR (JDT1."TransId")
      END AS "Reference"
    FROM
      JDT1
      CROSS JOIN OADM
      INNER JOIN OACT ON OACT."AcctCode" = JDT1."Account"
      INNER JOIN OCRD ON OCRD."CardCode" = JDT1."ShortName"
      AND OCRD."CardType" = 'S' -- Keep only vendor lines
      LEFT JOIN marketing_documents md ON md."TransId" = JDT1."TransId"
    WHERE
      JDT1."RefDate" <= '2026-03-31' -- Filter by posting date
      AND JDT1."BalDueDeb" <> JDT1."BalDueCred" -- Keep only open lines
  ),
  journal_entries AS (
    SELECT
      'K' AS "ItemType",
      COALESCE(OCRD."U_ID_SAP_AFS1", 'NOT MAPPED') AS "Account",
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
      TO_VARCHAR (JDT1."DueDate", 'YYYYMMDD') AS "BaselineDate",
      COALESCE(ptm."MappedId", 'Z999') AS "PaymentTerms", -- Default dummy payment term
      'B' AS "PaymentMethod", -- Default vendor payment method
      COALESCE(LPAD (OCRD."U_ID_SAP_AFS1", 10, '0'), 'NOT MAPPED') || '-' || OCRD."CardCode" AS "ItemText",
      REPLACE (
        REPLACE (
          TO_VARCHAR (
            CAST(
              CASE
                WHEN JDT1."FCCurrency" IS NOT NULL THEN JDT1."BalFcDeb" - JDT1."BalFcCred"
                ELSE JDT1."BalDueDeb" - JDT1."BalDueCred"
              END AS DECIMAL(19, 2)
            )
          ),
          '.00',
          ''
        ),
        '.',
        ','
      ) AS "Amount",
      CAST(
        CASE
          WHEN JDT1."FCCurrency" IS NOT NULL THEN JDT1."BalDueDeb" - JDT1."BalDueCred"
        END AS BIGINT
      ) AS "AmountDI",
      CAST(JDT1."BalDueDeb" - JDT1."BalDueCred" AS BIGINT) AS "AmountCLP",
      JDT1."TransId",
      JDT1."Line_ID",
      OCRD."CardCode",
      TO_VARCHAR (JDT1."TaxDate", 'YYYYMMDD') AS "DocumentDate",
      COALESCE(JDT1."FCCurrency", OADM."MainCurncy") AS "Currency",
      CASE
        WHEN md."TransId" IS NOT NULL THEN md."FolioPref" || '-' || md."FolioNum"
        ELSE TO_VARCHAR (JDT1."TransId")
      END AS "Reference"
    FROM
      JDT1
      CROSS JOIN OADM
      INNER JOIN OACT ON OACT."AcctCode" = JDT1."Account"
      INNER JOIN OCRD ON OCRD."CardCode" = JDT1."ShortName"
      AND OCRD."CardType" = 'S' -- Keep only vendor lines
      LEFT JOIN marketing_documents md ON md."TransId" = JDT1."TransId"
      LEFT JOIN payment_terms_mapping ptm ON ptm."Id" = md."GroupNum"
    WHERE
      JDT1."RefDate" <= '2026-03-31' -- Filter by posting date
      AND JDT1."BalDueDeb" <> JDT1."BalDueCred" -- Keep only open entries
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
  )
  /* Main Query */
SELECT
  DENSE_RANK() OVER (
    ORDER BY
      "CardCode",
      "TransId"
  ) AS "1_grouping",
  'E930' AS "2_company_code",
  'ZK' AS "3_document_type",
  "DocumentDate" AS "4_document_date",
  '20260331' AS "5_posting_date", -- Change date based on filter
  NULL AS "6_reverse_date",
  NULL AS "7_currency_date",
  "Reference" AS "8_reference",
  'AP OI-Migration' AS "9_doc_header_text",
  NULL AS "10_local_ledger",
  NULL AS "11_posting_key",
  "ItemType" AS "12_item_type",
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
  "PaymentTerms" AS "47_payment_terms",
  "BaselineDate" AS "48_baseline_date",
  "PaymentMethod" AS "49_payment_method",
  NULL AS "50_payment_block",
  NULL AS "51_segment",
  NULL AS "52_cross_company",
  NULL AS "53_gl_accnt_999",
  NULL AS "54_prctr_999",
  "AmountDI" AS "55_amount_di",
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
  "CardCode" AS "CHECKBusinessPartner",
  "AmountCLP" AS "CHECKAmountCLP"
FROM
  combined_entries
ORDER BY
  "CardCode",
  "TransId",
  "Line_ID",
  "ItemType"
;