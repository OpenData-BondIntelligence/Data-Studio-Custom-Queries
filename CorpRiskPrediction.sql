WITH risk_predicted_corp AS (SELECT * FROM ML.PREDICT(MODEL `bi-model-development.looker_FINAL.risk_corp_model`,
            (
            SELECT
            OFFERING_DATE, OFFERING_AMT, OFFERING_PRICE, PRINCIPAL_AMT, MATURITY,
            COUPON, DATED_DATE, FIRST_INTEREST_DATE, LAST_INTEREST_DATE,
            NCOUPS, T_DVolume, T_Yld_Pt,
            
              ((CASE WHEN SAFE_CAST(@yield as float64) IS NULL THEN ((YIELD * POWER(4.16508023e+01, 0.5)) + 5.25625364e+00)
            ELSE SAFE_CAST(@yield as float64)
            END) - 5.25625364e+00)/POWER(4.16508023e+01, 0.5) AS YIELD,
            
              ((CASE WHEN SAFE_CAST(@price_eom as float64) IS NULL THEN ((PRICE_EOM * POWER(1.76568368e+02, 0.5)) + 1.04157195e+02)
            ELSE SAFE_CAST(@price_eom as float64)
            END) - 1.04157195e+02)/POWER(1.76568368e+02, 0.5) AS PRICE_EOM,
            
              ((CASE WHEN SAFE_CAST(@trade_volume as float64) IS NULL THEN ((T_Volume * POWER(1.46914264e+16, 0.5)) + 4.04001929e+07)
            ELSE SAFE_CAST(@trade_volume as float64)
            END) - 4.04001929e+07)/POWER(1.46914264e+16, 0.5) AS T_Volume,
            
              ((CASE WHEN SAFE_CAST(@duration as float64) IS NULL THEN ((DURATION * POWER(1.75036897e+01, 0.5)) + 5.69104413e+00)
            ELSE SAFE_CAST(@duration as float64)
            END) - 5.69104413e+00)/POWER(1.75036897e+01, 0.5) AS DURATION,
            
              ((CASE WHEN SAFE_CAST(@amount_outstanding as float64) IS NULL THEN ((AMOUNT_OUTSTANDING * POWER(4.53987950e+09, 0.5)) + 3.88462634e+03)
            ELSE SAFE_CAST(@amount_outstanding as float64)
            END) - 3.88462634e+03)/POWER(4.53987950e+09, 0.5) AS AMOUNT_OUTSTANDING,
            GAP, COUPMONTH, COUPAMT, COUPACC, MULTICOUPS, RET_EOM, TMT, REMCOUPS, CUSIP
            FROM `bi-model-development.looker_FINAL.risk_corp_dataset`
            --WHERE CUSIP = @cusip
            ORDER BY ((DATED_DATE * POWER(4.82836142e+09, 0.5)) + 2.00584948e+07) DESC
            LIMIT 10000
            )
            )
         )
SELECT
    ROUND(((risk_predicted_corp.AMOUNT_OUTSTANDING * POWER(4.53987950e+09, 0.5)) + 3.88462634e+03), 10)  AS amount_outstanding,
    ROUND(((risk_predicted_corp.DURATION * POWER(1.75036897e+01, 0.5)) + 5.69104413e+00), 4)  AS duration,
    CASE WHEN  risk_predicted_corp.predicted_R_FR   = 4 THEN "Very Low"
          WHEN  risk_predicted_corp.predicted_R_FR   = 3 THEN "Low"
          WHEN  risk_predicted_corp.predicted_R_FR   = 2 THEN "Medium"
          WHEN  risk_predicted_corp.predicted_R_FR   = 1 THEN "High"
          WHEN  risk_predicted_corp.predicted_R_FR   = 0 THEN "Very High"
          ELSE NULL END
           AS risk_predicted_corp_evaluated_risk,
    ROUND(((risk_predicted_corp.PRICE_EOM  * POWER(1.76568368e+02, 0.5)) + 1.04157195e+02), 4)  AS price_eom,
    ROUND(((risk_predicted_corp.T_Volume * POWER(1.46914264e+16, 0.5)) + 4.04001929e+07), 4)  AS trade_volume,
    ROUND(((risk_predicted_corp.YIELD * POWER(4.16508023e+01, 0.5)) + 5.25625364e+00), 4)  AS yield,
	risk_predicted_corp.CUSIP AS CUSIP
FROM risk_predicted_corp
LIMIT 10000
