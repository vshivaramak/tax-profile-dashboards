%sql
-- =============================================================================
-- Tax Profile Upsell — Experiment 329005 (Test #4)
-- Control: 719072 (DIWM_COMPCHART)
-- Treatment A: 719069 (DIWM_STANDALONE)
-- Treatment B: 719070 (DIWM_LITE_STANDALONE)
-- Treatment C: 719071 (DIWM_LITE_COMPCHART)
-- Segment: New + Returning users, all ages (25U + 26O), TTO Free/Deluxe/Premier/SE
-- Assignment: IXP-based (ixp_dwh.ixp_first_assignment)
-- =============================================================================

WITH experiment_base AS (
    SELECT DISTINCT
        ixp.id AS auth_id,
        CASE
            WHEN ixp.treatment_id = 719072 THEN 'DIWM_COMPCHART'
            WHEN ixp.treatment_id = 719069 THEN 'DIWM_STANDALONE'
            WHEN ixp.treatment_id = 719070 THEN 'DIWM_LITE_STANDALONE'
            WHEN ixp.treatment_id = 719071 THEN 'DIWM_LITE_COMPCHART'
        END AS treatment_name,
        CASE WHEN pam.first_completed_datetime IS NOT NULL THEN 1 ELSE 0 END AS raw_complete_flag,
        pam.start_product_family_name AS product_family_name,
        CASE WHEN pam.age_nbr <= 25 THEN '25U' ELSE '26O' END AS age_segment,
        pam.account_tenure_l1_name AS tenure
    FROM ixp_dwh.ixp_first_assignment ixp
    INNER JOIN tax_rpt_ca.rpt_account_product_master pam
        ON pam.auth_id = ixp.id
        AND pam.tax_product_year_nbr = 2025
    WHERE ixp.experiment_id = 329005
      AND CAST(ixp.first_timestamp AS DATE) >= '2026-04-15'
      AND CAST(ixp.first_timestamp AS DATE) <= CURRENT_DATE() - 1
      AND ixp.treatment_id IN (719072, 719069, 719070, 719071)
      AND pam.account_tenure_l1_name IN ('New', 'Returning')
      AND pam.start_product_family_name IN ('TTO Free', 'TTO Deluxe', 'TTO Premier', 'TTO SE')
),

view_activity AS (
    SELECT
        dcua.auth_id,
        MAX(CASE WHEN fcce.page_parameter_name = 's_taxprofile_upsell_diwm_comp_chart'
             THEN 1 ELSE 0 END) AS view_flag_compchart,
        MAX(CASE WHEN fcce.page_parameter_name = 's_taxprofile_upsell_diwm_standalone'
             THEN 1 ELSE 0 END) AS view_flag_standalone,
        MAX(CASE WHEN fcce.page_parameter_name = 's_taxprofile_upsell_diwm_lite_standalone'
             THEN 1 ELSE 0 END) AS view_flag_lite_standalone,
        MAX(CASE WHEN fcce.page_parameter_name = 's_taxprofile_upsell_diwm_lite_comp_chart'
             THEN 1 ELSE 0 END) AS view_flag_lite_compchart
    FROM tax_stg_ca.stg_catax_clickstream stg
    INNER JOIN tax_dm_ca.fact_catax_clickstream_event fcce
        ON stg.event_header_event_id = fcce.clickstream_event_id
        AND stg.load_date = fcce.load_date
    INNER JOIN tax_dm_ca.dim_catax_user_account dcua
        ON dcua.catax_user_account_key = fcce.catax_user_account_key
    WHERE fcce.tax_product_year_nbr = 2025
      AND DATE(fcce.event_datetime) >= '2026-04-15'
      AND fcce.page_parameter_name IN (
          's_taxprofile_upsell_diwm_comp_chart',
          's_taxprofile_upsell_diwm_standalone',
          's_taxprofile_upsell_diwm_lite_standalone',
          's_taxprofile_upsell_diwm_lite_comp_chart'
      )
      AND (fcce.clickstream_event_name IS NULL OR fcce.clickstream_event_name = '' OR fcce.clickstream_event_name = 'content:viewed')
    GROUP BY dcua.auth_id
),

confirm_activity AS (
    SELECT
        dcua.auth_id,
        -- DIWM Compchart: DIWM only (no PP)
        MAX(CASE WHEN fcce.page_parameter_name = 's_taxprofile_upsell_diwm_comp_chart'
                  AND fcce.ui_object_detail_name = 'upgrade_to_expert_assist'
             THEN 1 ELSE 0 END) AS confirm_flag_compchart_diwm,
        -- DIWM Compchart: DIWM + PP
        MAX(CASE WHEN fcce.page_parameter_name = 's_taxprofile_upsell_diwm_comp_chart'
                  AND fcce.ui_object_detail_name = 'upgrade_to_expert_assist_premium'
             THEN 1 ELSE 0 END) AS confirm_flag_compchart_pp,
        -- DIWM Standalone confirm (start_upgrade)
        MAX(CASE WHEN fcce.page_parameter_name = 's_taxprofile_upsell_diwm_standalone'
                  AND fcce.ui_object_detail_name = 'start_upgrade'
             THEN 1 ELSE 0 END) AS confirm_flag_standalone,
        -- DIWM Lite Standalone confirm (start_upgrade)
        MAX(CASE WHEN fcce.page_parameter_name = 's_taxprofile_upsell_diwm_lite_standalone'
                  AND fcce.ui_object_detail_name = 'start_upgrade'
             THEN 1 ELSE 0 END) AS confirm_flag_lite_standalone,
        -- DIWM Lite Compchart: Lite only
        MAX(CASE WHEN fcce.page_parameter_name = 's_taxprofile_upsell_diwm_lite_comp_chart'
                  AND fcce.ui_object_detail_name = 'upgrade_to_expert_one_time_assist'
             THEN 1 ELSE 0 END) AS confirm_flag_lite_cc_lite,
        -- DIWM Lite Compchart: Full DIWM (upsold from Lite CC)
        MAX(CASE WHEN fcce.page_parameter_name = 's_taxprofile_upsell_diwm_lite_comp_chart'
                  AND fcce.ui_object_detail_name = 'upgrade_to_expert_assist'
             THEN 1 ELSE 0 END) AS confirm_flag_lite_cc_diwm
    FROM tax_stg_ca.stg_catax_clickstream stg
    INNER JOIN tax_dm_ca.fact_catax_clickstream_event fcce
        ON stg.event_header_event_id = fcce.clickstream_event_id
        AND stg.load_date = fcce.load_date
    INNER JOIN tax_dm_ca.dim_catax_user_account dcua
        ON dcua.catax_user_account_key = fcce.catax_user_account_key
    WHERE DATE(fcce.event_datetime) >= '2026-04-15'
      AND fcce.clickstream_event_name = 'content:engaged'
      AND fcce.page_parameter_name IN (
          's_taxprofile_upsell_diwm_comp_chart',
          's_taxprofile_upsell_diwm_standalone',
          's_taxprofile_upsell_diwm_lite_standalone',
          's_taxprofile_upsell_diwm_lite_comp_chart'
      )
    GROUP BY dcua.auth_id
),

revenue_activity AS (
    SELECT
        auth_id,
        SUM(total_sales_amt) AS total_revenue
    FROM tax_dm_ca.v_sales_direct
    WHERE fiscal_year_nbr = 2026
      AND DATE(calendar_date) >= '2026-04-15'
      AND (applied_segment_name NOT IN ('25U_segment', 'Switch&Save') OR applied_segment_name IS NULL)
      AND item_id IN (292852, 292815, 292776, 292769, 292792, 292820, 292803, 292777,
                      292834, 292802, 292805, 292784, 292836, 292794, 292829, 292797,
                      292787, 292817, 292766, 292842, 292858, 292786, 292835, 300768,
                      303769, 303770, 303771)
    GROUP BY 1
),

contact_activity AS (
    SELECT
        auth_id,
        MAX(CASE WHEN leg_queue IN ('cg-can_ta_ar_se_en', 'cg-can_ta_ar_en', 'cg-can_ta_ar_fr')
             THEN 1 ELSE 0 END) AS contact_review_expert,
        MAX(CASE WHEN leg_queue IN ('cg-can_ta_diwmexpert_fr', 'cg-can_ta_diwmexpert_se_en', 'cg-can_ta_diwmexpert_en')
             THEN 1 ELSE 0 END) AS contact_nonreview_expert
    FROM ent_care_dwh.rpt_cct_interactions
    WHERE (upper(agent_country_served) IN ('CAN', 'CA') OR upper(interaction_skill_country) IN ('CA', 'CAN'))
      AND DATE(contact_start_ts) >= '2026-04-15'
      AND bu = 'cg'
      AND customer_handle_flg = 1
      AND leg_queue IN (
          'cg-can_ta_ar_se_en', 'cg-can_ta_ar_en', 'cg-can_ta_ar_fr',
          'cg-can_ta_diwmexpert_fr', 'cg-can_ta_diwmexpert_se_en', 'cg-can_ta_diwmexpert_en'
      )
    GROUP BY auth_id
),

user_level_funnel AS (
    SELECT
        base.auth_id,
        base.treatment_name,
        base.product_family_name,
        base.age_segment,
        base.tenure,

        -- View flag (match treatment to its view flag)
        CASE
            WHEN base.treatment_name = 'DIWM_COMPCHART' AND v.view_flag_compchart = 1 THEN 1
            WHEN base.treatment_name = 'DIWM_STANDALONE' AND v.view_flag_standalone = 1 THEN 1
            WHEN base.treatment_name = 'DIWM_LITE_STANDALONE' AND v.view_flag_lite_standalone = 1 THEN 1
            WHEN base.treatment_name = 'DIWM_LITE_COMPCHART' AND v.view_flag_lite_compchart = 1 THEN 1
            ELSE 0
        END AS view_flag,

        -- Confirm flag (any confirm type for the user's treatment, gated on view)
        CASE
            WHEN base.treatment_name = 'DIWM_COMPCHART' AND v.view_flag_compchart = 1
                 AND (c.confirm_flag_compchart_diwm = 1 OR c.confirm_flag_compchart_pp = 1) THEN 1
            WHEN base.treatment_name = 'DIWM_STANDALONE' AND v.view_flag_standalone = 1
                 AND c.confirm_flag_standalone = 1 THEN 1
            WHEN base.treatment_name = 'DIWM_LITE_STANDALONE' AND v.view_flag_lite_standalone = 1
                 AND c.confirm_flag_lite_standalone = 1 THEN 1
            WHEN base.treatment_name = 'DIWM_LITE_COMPCHART' AND v.view_flag_lite_compchart = 1
                 AND (c.confirm_flag_lite_cc_lite = 1 OR c.confirm_flag_lite_cc_diwm = 1) THEN 1
            ELSE 0
        END AS confirm_flag,

        -- Confirm sub-type flags for weight/rev branching
        COALESCE(c.confirm_flag_compchart_pp, 0) AS is_pp_confirm,
        COALESCE(c.confirm_flag_lite_cc_lite, 0) AS is_lite_cc_lite_confirm,
        COALESCE(c.confirm_flag_lite_cc_diwm, 0) AS is_lite_cc_diwm_confirm,

        -- Base Product Price (Apr 16+ end-of-season pricing)
        CASE COALESCE(base.product_family_name, 'Unknown')
            WHEN 'TTO Deluxe' THEN 30
            WHEN 'TTO Premier' THEN 50
            WHEN 'TTO SE' THEN 70
            ELSE 0
        END AS base_product_price,

        -- Adjusted confirm weight (varies by confirm type)
        -- DIWM full: Deluxe/Premier=1.0, SE=1.1
        -- DIWM+PP: Deluxe/Premier=1.9, SE=2.0
        -- Lite: 0.6
        CASE
            -- Compchart: PP confirm (DIWM + Priority Pro)
            WHEN base.treatment_name = 'DIWM_COMPCHART' AND v.view_flag_compchart = 1
                 AND c.confirm_flag_compchart_pp = 1 THEN
                CASE COALESCE(base.product_family_name, 'Unknown')
                    WHEN 'TTO SE' THEN 2.0
                    ELSE 1.9
                END
            -- Compchart: DIWM only (no PP)
            WHEN base.treatment_name = 'DIWM_COMPCHART' AND v.view_flag_compchart = 1
                 AND c.confirm_flag_compchart_diwm = 1 THEN
                CASE COALESCE(base.product_family_name, 'Unknown')
                    WHEN 'TTO SE' THEN 1.1
                    ELSE 1.0
                END
            -- Standalone: always full DIWM
            WHEN base.treatment_name = 'DIWM_STANDALONE' AND v.view_flag_standalone = 1
                 AND c.confirm_flag_standalone = 1 THEN
                CASE COALESCE(base.product_family_name, 'Unknown')
                    WHEN 'TTO SE' THEN 1.1
                    ELSE 1.0
                END
            -- Lite Compchart: full DIWM upsold
            WHEN base.treatment_name = 'DIWM_LITE_COMPCHART' AND v.view_flag_lite_compchart = 1
                 AND c.confirm_flag_lite_cc_diwm = 1 THEN
                CASE COALESCE(base.product_family_name, 'Unknown')
                    WHEN 'TTO SE' THEN 1.1
                    ELSE 1.0
                END
            -- Lite Compchart: Lite only
            WHEN base.treatment_name = 'DIWM_LITE_COMPCHART' AND v.view_flag_lite_compchart = 1
                 AND c.confirm_flag_lite_cc_lite = 1 THEN 0.6
            -- Lite Standalone: always Lite
            WHEN base.treatment_name = 'DIWM_LITE_STANDALONE' AND v.view_flag_lite_standalone = 1
                 AND c.confirm_flag_lite_standalone = 1 THEN 0.6
            ELSE 0
        END AS adjusted_confirm_flag,

        -- PM Incremental Revenue per confirm (varies by confirm type, Apr 16+ pricing)
        CASE
            -- Compchart: PP confirm (DIWM + Priority Pro)
            WHEN base.treatment_name = 'DIWM_COMPCHART' AND v.view_flag_compchart = 1
                 AND c.confirm_flag_compchart_pp = 1 THEN
                CASE COALESCE(base.product_family_name, 'Unknown')
                    WHEN 'TTO SE' THEN 140
                    ELSE 130
                END
            -- Compchart: DIWM only (no PP)
            WHEN base.treatment_name = 'DIWM_COMPCHART' AND v.view_flag_compchart = 1
                 AND c.confirm_flag_compchart_diwm = 1 THEN
                CASE COALESCE(base.product_family_name, 'Unknown')
                    WHEN 'TTO SE' THEN 80
                    ELSE 70
                END
            -- Standalone: always full DIWM
            WHEN base.treatment_name = 'DIWM_STANDALONE' AND v.view_flag_standalone = 1
                 AND c.confirm_flag_standalone = 1 THEN
                CASE COALESCE(base.product_family_name, 'Unknown')
                    WHEN 'TTO SE' THEN 80
                    ELSE 70
                END
            -- Lite Compchart: full DIWM upsold
            WHEN base.treatment_name = 'DIWM_LITE_COMPCHART' AND v.view_flag_lite_compchart = 1
                 AND c.confirm_flag_lite_cc_diwm = 1 THEN
                CASE COALESCE(base.product_family_name, 'Unknown')
                    WHEN 'TTO SE' THEN 80
                    ELSE 70
                END
            -- Lite Compchart: Lite only
            WHEN base.treatment_name = 'DIWM_LITE_COMPCHART' AND v.view_flag_lite_compchart = 1
                 AND c.confirm_flag_lite_cc_lite = 1 THEN 40
            -- Lite Standalone: always Lite
            WHEN base.treatment_name = 'DIWM_LITE_STANDALONE' AND v.view_flag_lite_standalone = 1
                 AND c.confirm_flag_lite_standalone = 1 THEN 40
            ELSE 0
        END AS pm_inc_rev,

        -- Complete (gated on view + confirm)
        CASE WHEN confirm_flag = 1 AND base.raw_complete_flag = 1
            THEN 1 ELSE 0
        END AS complete_flag,

        -- Purchase (gated on view + confirm + complete + revenue)
        CASE WHEN confirm_flag = 1 AND base.raw_complete_flag = 1 AND r.total_revenue > 0
            THEN 1 ELSE 0
        END AS purchase_flag,

        -- Funnel Revenue
        CASE WHEN confirm_flag = 1 AND base.raw_complete_flag = 1
            THEN COALESCE(r.total_revenue, 0)
            ELSE 0
        END AS funnel_revenue,

        -- Contact Flags
        CASE WHEN ca.contact_review_expert = 1 THEN 1 ELSE 0 END AS contact_review_flag,
        CASE WHEN ca.contact_nonreview_expert = 1 THEN 1 ELSE 0 END AS contact_nonreview_flag,
        CASE WHEN ca.contact_review_expert = 1 OR ca.contact_nonreview_expert = 1 THEN 1 ELSE 0 END AS contact_any_flag

    FROM experiment_base base
    LEFT JOIN view_activity v ON base.auth_id = v.auth_id
    LEFT JOIN confirm_activity c ON base.auth_id = c.auth_id
    LEFT JOIN revenue_activity r ON base.auth_id = r.auth_id
    LEFT JOIN contact_activity ca ON CAST(base.auth_id AS VARCHAR(50)) = CAST(ca.auth_id AS VARCHAR(50))
)

-- FINAL AGGREGATION by treatment, SKU, age_segment, tenure
SELECT
    treatment_name, product_family_name, age_segment, tenure,
    COUNT(auth_id) AS total_users,

    SUM(view_flag) AS viewed,
    SUM(confirm_flag) AS confirmed,
    SUM(adjusted_confirm_flag) AS adj_confirmed,
    SUM(purchase_flag) AS purchased,

    SUM(CASE WHEN purchase_flag = 1 AND contact_review_flag = 1 THEN 1 ELSE 0 END) AS purchasers_review_expert,
    SUM(CASE WHEN purchase_flag = 1 AND contact_nonreview_flag = 1 THEN 1 ELSE 0 END) AS purchasers_nonreview_expert,
    SUM(CASE WHEN purchase_flag = 1 AND contact_any_flag = 1 THEN 1 ELSE 0 END) AS purchasers_any_contact,
    ROUND(SUM(CASE WHEN purchase_flag = 1 AND contact_review_flag = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(SUM(purchase_flag), 0), 4) AS review_expert_rate_of_purchasers,
    ROUND(SUM(CASE WHEN purchase_flag = 1 AND contact_nonreview_flag = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(SUM(purchase_flag), 0), 4) AS nonreview_expert_rate_of_purchasers,
    ROUND(SUM(CASE WHEN purchase_flag = 1 AND contact_any_flag = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(SUM(purchase_flag), 0), 4) AS any_contact_rate_of_purchasers,

    ROUND(SUM(CASE WHEN purchase_flag = 1 THEN base_product_price ELSE 0 END), 4) AS actual_base_revenue,
    ROUND(SUM(CASE WHEN purchase_flag = 1 THEN funnel_revenue ELSE 0 END), 4) AS actual_incremental_revenue,
    ROUND(SUM(CASE WHEN purchase_flag = 1 THEN funnel_revenue + base_product_price ELSE 0 END), 4) AS actual_total_revenue,

    ROUND(SUM(confirm_flag * base_product_price), 4) AS projected_base_revenue,
    ROUND(SUM(pm_inc_rev), 4) AS projected_incremental_revenue,
    ROUND(SUM(pm_inc_rev) + SUM(confirm_flag * base_product_price), 4) AS projected_total_revenue,

    ROUND(SUM(view_flag) / COUNT(auth_id), 4) AS reach_rate,
    ROUND(SUM(confirm_flag) / NULLIF(SUM(view_flag), 0), 4) AS take_rate,
    ROUND(SUM(adjusted_confirm_flag) / NULLIF(SUM(view_flag), 0), 4) AS adjusted_take_rate,
    ROUND(SUM(complete_flag) / NULLIF(SUM(view_flag), 0), 4) AS complete_rate_of_view,
    ROUND(SUM(complete_flag) / NULLIF(SUM(confirm_flag), 0), 4) AS complete_rate_of_confirm,
    ROUND(SUM(purchase_flag) / NULLIF(SUM(view_flag), 0), 4) AS purchase_rate_of_view,
    ROUND(SUM(purchase_flag) / NULLIF(SUM(confirm_flag), 0), 4) AS purchase_rate_of_confirm,

    ROUND(SUM(funnel_revenue) / NULLIF(SUM(view_flag), 0), 4) AS avg_rev_per_view,
    ROUND(SUM(funnel_revenue) / NULLIF(SUM(confirm_flag), 0), 4) AS avg_rev_per_confirm

FROM user_level_funnel
GROUP BY 1, 2, 3, 4
ORDER BY 4, 3, 1, 2;
