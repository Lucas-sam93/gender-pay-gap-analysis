
-- PRIMARY METRIC AND DISTRIBUTION - 2025 

WITH
  companies_2025 AS (
    SELECT *
    FROM `dab-project-2026.gender_pay_gap.gender_pay_gap`
    WHERE EXTRACT(YEAR FROM DateSubmitted) = 2025
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY EmployerId ORDER BY DateSubmitted DESC)
      = 1
  )
SELECT
  APPROX_QUANTILES(DiffMedianHourlyPercent, 100)[OFFSET(50)]
    AS median_pay_gap_2025,
  COUNT(*) AS companies_2025
FROM companies_2025;

WITH
  companies_2025 AS (
    SELECT *
    FROM `dab-project-2026.gender_pay_gap.gender_pay_gap`
    WHERE EXTRACT(YEAR FROM DateSubmitted) = 2025
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY EmployerId ORDER BY DateSubmitted DESC)
      = 1
  )
SELECT
  EmployerSize,
  COUNT(*) AS companies,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM companies_2025
GROUP BY EmployerSize
ORDER BY companies DESC;

SELECT
  COUNT(*) AS total_records,
  COUNT(DISTINCT EmployerId) AS unique_companies,
  MIN(EXTRACT(YEAR FROM DateSubmitted)) AS earliest_year,
  MAX(EXTRACT(YEAR FROM DateSubmitted)) AS latest_year
FROM `dab-project-2026.gender_pay_gap.gender_pay_gap`;


-- Glass Ceiling 
WITH
  companies_2025 AS (
    SELECT *
    FROM `dab-project-2026.gender_pay_gap.gender_pay_gap`
    WHERE EXTRACT(YEAR FROM DateSubmitted) = 2025
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY EmployerId ORDER BY DateSubmitted DESC)
      = 1
  ),
  glass_ceiling_2025 AS (
    SELECT
      (FemaleLowerQuartile - FemaleTopQuartile) AS glass_ceiling,
      FemaleLowerQuartile AS bottom_25,
      FemaleTopQuartile AS top_25
    FROM companies_2025
    WHERE FemaleLowerQuartile IS NOT NULL AND FemaleTopQuartile IS NOT NULL
  )
SELECT
  APPROX_QUANTILES(glass_ceiling, 100)[OFFSET(50)] AS median_glass_ceiling,
  APPROX_QUANTILES(bottom_25, 100)[OFFSET(50)] AS median_bottom_quartile,
  APPROX_QUANTILES(top_25, 100)[OFFSET(50)] AS median_top_quartile,
  APPROX_QUANTILES(bottom_25, 100)[OFFSET(50)]
    - APPROX_QUANTILES(top_25, 100)[OFFSET(50)]
    AS drop
FROM glass_ceiling_2025;


-- EXECUTIVE BARRIER - Drop at Each Stage

WITH
  companies_2025 AS (
    SELECT *
    FROM `dab-project-2026.gender_pay_gap.gender_pay_gap`
    WHERE EXTRACT(YEAR FROM DateSubmitted) = 2025
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY EmployerId ORDER BY DateSubmitted DESC)
      = 1
  ),
  quartile_data AS (
    SELECT
      FemaleLowerQuartile,
      FemaleLowerMiddleQuartile,
      FemaleUpperMiddleQuartile,
      FemaleTopQuartile
    FROM companies_2025
    WHERE
      FemaleLowerQuartile IS NOT NULL
      AND FemaleLowerMiddleQuartile IS NOT NULL
      AND FemaleUpperMiddleQuartile IS NOT NULL
      AND FemaleTopQuartile IS NOT NULL
  )
SELECT
  APPROX_QUANTILES(FemaleLowerQuartile, 100)[OFFSET(50)] AS bottom,
  APPROX_QUANTILES(FemaleLowerMiddleQuartile, 100)[OFFSET(50)] AS lower_mid,
  APPROX_QUANTILES(FemaleUpperMiddleQuartile, 100)[OFFSET(50)] AS upper_mid,
  APPROX_QUANTILES(FemaleTopQuartile, 100)[OFFSET(50)] AS top,
  ROUND(
    APPROX_QUANTILES(FemaleLowerQuartile, 100)[OFFSET(50)]
      - APPROX_QUANTILES(FemaleLowerMiddleQuartile, 100)[OFFSET(50)],
    1)
    AS drop_stage_1_bottom_to_lowermid,
  ROUND(
    APPROX_QUANTILES(FemaleLowerMiddleQuartile, 100)[OFFSET(50)]
      - APPROX_QUANTILES(FemaleUpperMiddleQuartile, 100)[OFFSET(50)],
    1)
    AS drop_stage_2_lowermid_to_uppermid,
  ROUND(
    APPROX_QUANTILES(FemaleUpperMiddleQuartile, 100)[OFFSET(50)]
      - APPROX_QUANTILES(FemaleTopQuartile, 100)[OFFSET(50)],
    1)
    AS drop_stage_3_uppermid_to_top
FROM quartile_data;


-- YEAR-OVER-YEAR TRENDS (2018–2025 SUBMISSIONS)

WITH
  base AS (
    SELECT
      EmployerId,
      EXTRACT(YEAR FROM DateSubmitted) AS year,
      DateSubmitted,
      DiffMedianHourlyPercent,
      (FemaleLowerQuartile - FemaleTopQuartile) AS glass_ceiling
    FROM `dab-project-2026.gender_pay_gap.gender_pay_gap`
    WHERE EXTRACT(YEAR FROM DateSubmitted) BETWEEN 2018 AND 2025
  ),
  dedup_year AS (
    SELECT *
    FROM base
    QUALIFY
      ROW_NUMBER()
        OVER (PARTITION BY EmployerId, year ORDER BY DateSubmitted DESC)
      = 1
  )
SELECT
  year,
  COUNT(*) AS companies,
  ROUND(APPROX_QUANTILES(DiffMedianHourlyPercent, 100)[OFFSET(50)], 1)
    AS median_pay_gap,
  ROUND(APPROX_QUANTILES(glass_ceiling, 100)[OFFSET(50)], 1)
    AS median_glass_ceiling
FROM dedup_year
GROUP BY year
ORDER BY year;


-- SECTOR CONTRAST (2025 SUBMISSIONS, SIC2 PARSING, DEDUPED, MEDIAN)

WITH
  companies_2025 AS (
    SELECT *
    FROM `dab-project-2026.gender_pay_gap.gender_pay_gap`
    WHERE EXTRACT(YEAR FROM DateSubmitted) = 2025
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY EmployerId ORDER BY DateSubmitted DESC)
      = 1
  ),
  parsed AS (
    SELECT
      SAFE_CAST(SUBSTR(REGEXP_EXTRACT(SicCodes, r'(\d+)'), 1, 2) AS INT64)
        AS sic2,
      DiffMedianHourlyPercent,
      (FemaleLowerQuartile - FemaleTopQuartile) AS glass_ceiling
    FROM companies_2025
  )
SELECT
  CASE
    WHEN sic2 = 87 THEN 'Care Homes'
    WHEN sic2 = 86 THEN 'Healthcare'
    WHEN sic2 = 85 THEN 'Education'
    WHEN sic2 IN (64, 65, 66) THEN 'Finance'
    ELSE 'Other'
    END
    AS sector,
  COUNT(*) AS employers,
  ROUND(APPROX_QUANTILES(glass_ceiling, 100)[OFFSET(50)], 1)
    AS mean_glass_ceiling,
  ROUND(APPROX_QUANTILES(DiffMedianHourlyPercent, 100)[OFFSET(50)], 1)
    AS mean_pay_gap
FROM parsed
WHERE sic2 IN (87, 86, 85, 64, 65, 66)
GROUP BY sector
ORDER BY sector


-- COMPLETE SECTOR RANKING (All 19 sectors)

WITH companies_2025 AS (
  SELECT *
  FROM `dab-project-2026.gender_pay_gap.gender_pay_gap`
  WHERE EXTRACT(YEAR FROM DateSubmitted) = 2025
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EmployerId ORDER BY DateSubmitted DESC) = 1
),

all_sectors AS (
  SELECT
    CASE
      WHEN REGEXP_CONTAINS(SicCodes, r'\b87|88') THEN 'Residential Care & Social Work'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b86') THEN 'Human Health Activities'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b55|56') THEN 'Accommodation & Food Services'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b84') THEN 'Public Administration & Defence'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b90|91|92|93') THEN 'Arts, Entertainment & Recreation'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b45|46|47') THEN 'Wholesale & Retail Trade'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b49|50|51|52|53') THEN 'Transportation & Storage'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b1[0-9]|2[0-9]|3[0-3]') THEN 'Manufacturing'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b77|78|79|80|81|82') THEN 'Administrative & Support Services'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b35|36|37|38|39') THEN 'Utilities'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b85') THEN 'Education'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b68') THEN 'Real Estate'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b69|70|71|72|73|74|75') THEN 'Professional, Scientific & Technical'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b58|59|60|61|62|63') THEN 'Information & Communication'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b41|42|43') THEN 'Construction'
      WHEN REGEXP_CONTAINS(SicCodes, r'\b64|65|66') THEN 'Finance & Insurance'
      ELSE 'Other'
    END AS sector,
    (FemaleLowerQuartile - FemaleTopQuartile) AS glass_ceiling
  FROM companies_2025
  WHERE FemaleLowerQuartile IS NOT NULL
    AND FemaleTopQuartile IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY APPROX_QUANTILES(glass_ceiling, 100)[OFFSET(50)]) AS rank,
  sector,
  COUNT(*) AS companies,
  ROUND(APPROX_QUANTILES(glass_ceiling, 100)[OFFSET(50)], 1) AS median_gci
FROM all_sectors
GROUP BY sector
HAVING COUNT(*) >= 10
ORDER BY median_gci ASC;

-- Best & Worst Sectors Only


WITH
  companies_2025 AS (
    SELECT *
    FROM `dab-project-2026.gender_pay_gap.gender_pay_gap`
    WHERE EXTRACT(YEAR FROM DateSubmitted) = 2025
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY EmployerId ORDER BY DateSubmitted DESC)
      = 1
  ),
  -- BEST SECTOR: Residential Care & Social Work
  best_sector AS (
    SELECT
      'Residential Care & Social Work' AS sector,
      COUNT(*) AS companies,
      ROUND(
        APPROX_QUANTILES((FemaleLowerQuartile - FemaleTopQuartile), 100)[
          OFFSET(50)],
        1)
        AS median_gci,
      ROUND(APPROX_QUANTILES(DiffMedianHourlyPercent, 100)[OFFSET(50)], 1)
        AS median_pay_gap
    FROM companies_2025
    WHERE
      (REGEXP_CONTAINS(SicCodes, r'\b87') OR REGEXP_CONTAINS(SicCodes, r'\b88'))
      AND FemaleLowerQuartile IS NOT NULL
      AND FemaleTopQuartile IS NOT NULL
  ),

  -- WORST SECTOR: Finance & Insurance
  worst_sector AS (
    SELECT
      'Finance & Insurance' AS sector,
      COUNT(*) AS companies,
      ROUND(
        APPROX_QUANTILES((FemaleLowerQuartile - FemaleTopQuartile), 100)[
          OFFSET(50)],
        1)
        AS median_gci,
      ROUND(APPROX_QUANTILES(DiffMedianHourlyPercent, 100)[OFFSET(50)], 1)
        AS median_pay_gap
    FROM companies_2025
    WHERE
      (REGEXP_CONTAINS(SicCodes, r'\b64|65|66'))
      AND FemaleLowerQuartile IS NOT NULL
      AND FemaleTopQuartile IS NOT NULL
  )
SELECT * FROM best_sector
UNION ALL
SELECT * FROM worst_sector;

-- THE "1% CLUB" (2025 SUBMISSIONS, COMPANY-LEVEL, INCLUSIVE)

WITH
  companies_2025 AS (
    SELECT *
    FROM `dab-project-2026.gender_pay_gap.gender_pay_gap`
    WHERE EXTRACT(YEAR FROM DateSubmitted) = 2025
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY EmployerId ORDER BY DateSubmitted DESC)
      = 1
  ),
  excellent AS (
    SELECT (FemaleLowerQuartile - FemaleTopQuartile) AS glass_ceiling
    FROM companies_2025
    WHERE
      ABS(DiffMedianHourlyPercent) <= 1
      AND FemaleLowerQuartile IS NOT NULL
      AND FemaleTopQuartile IS NOT NULL
  ),
  overall AS (
    SELECT
      APPROX_QUANTILES((FemaleLowerQuartile - FemaleTopQuartile), 100)[
        OFFSET(50)]
        AS overall_median_glass_ceiling
    FROM companies_2025
    WHERE FemaleLowerQuartile IS NOT NULL AND FemaleTopQuartile IS NOT NULL
  )
SELECT
  (SELECT COUNT(*) FROM excellent) AS club_1pct_count,
  ROUND(
    (SELECT COUNT(*) FROM excellent)
      * 100.0
      / (SELECT COUNT(*) FROM companies_2025),
    1)
    AS club_1pct_percentage,
  ROUND(APPROX_QUANTILES(glass_ceiling, 100)[OFFSET(50)], 1)
    AS their_median_glass_ceiling,
  ROUND(
    (SELECT overall_median_glass_ceiling FROM overall), 1)
    AS overall_median_glass_ceiling
FROM excellent;


-- All Key Metrics

WITH
  companies_2025 AS (
    SELECT *
    FROM `dab-project-2026.gender_pay_gap.gender_pay_gap`
    WHERE EXTRACT(YEAR FROM DateSubmitted) = 2025
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY EmployerId ORDER BY DateSubmitted DESC)
      = 1
  ),
  data_2025 AS (
    SELECT
      DiffMedianHourlyPercent,
      DiffMedianBonusPercent,
      (FemaleLowerQuartile - FemaleTopQuartile) AS glass_ceiling,
      FemaleLowerQuartile,
      FemaleTopQuartile
    FROM companies_2025
  ),
  high_gap_bonus AS (
    SELECT
      APPROX_QUANTILES(DiffMedianBonusPercent, 100)[OFFSET(50)]
        AS median_bonus_gap
    FROM data_2025
    WHERE DiffMedianHourlyPercent >= 20
  )
SELECT
  COUNT(*) AS total_companies_2025,
  ROUND(APPROX_QUANTILES(DiffMedianHourlyPercent, 100)[OFFSET(50)], 1)
    AS median_pay_gap,
  ROUND(APPROX_QUANTILES(glass_ceiling, 100)[OFFSET(50)], 1)
    AS median_glass_ceiling,
  ROUND(APPROX_QUANTILES(FemaleLowerQuartile, 100)[OFFSET(50)], 1)
    AS bottom_25_pct,
  ROUND(APPROX_QUANTILES(FemaleTopQuartile, 100)[OFFSET(50)], 1) AS top_25_pct,
  ROUND(
    (SELECT median_bonus_gap FROM high_gap_bonus), 1)
    AS median_bonus_gap_for_high_paygap
FROM data_2025;
