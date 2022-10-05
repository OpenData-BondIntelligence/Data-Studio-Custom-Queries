WITH pricemodel AS (SELECT model.*
          FROM ML.PREDICT(MODEL `bi-model-development.looker_FINAL.price_muni_boosted_model`,
            (
            SELECT
            (cols.Income_Ratio - 4.78707613)/POWER(0.416192260, 0.5) AS Income_Ratio,
            (cols._80th_Percentile_Income - 1.10834510e+05)/POWER(5.46152171e+08, 0.5) AS _80th_Percentile_Income,
            (cols.FIPS - 2.17677827e+04)/POWER(3.15575966e+08, 0.5) AS FIPS,
            (cols.Africa_dem - 1.04641487e+04)/POWER(2.05322957e+08, 0.5) AS Africa_dem,
            (cols.Trade_Date - 7.94418234e+04)/POWER(9.70011616e+04, 0.5) AS Trade_Date,
            
              
              ((CASE WHEN SAFE_CAST(@ten_year_rate as float64) IS NULL THEN cols._10_Year_Treasury_Constant_Maturity_Rate_Percent_Daily_Not_Seasonally_Adjusted
            ELSE SAFE_CAST(@ten_year_rate as float64)
            END) - 2.38129247e+00)/POWER(2.26253201e-01, 0.5) AS _10_Year_Treasury_Constant_Maturity_Rate_Percent_Daily_Not_Seasonally_Adjusted,
            
              
              ((CASE WHEN SAFE_CAST(@interest_rate as float64) IS NULL THEN cols.Interest_rate_of_the_issue_traded
            ELSE SAFE_CAST(@interest_rate as float64)
            END) - 4.21839171e+00)/POWER(1.65615697e+00, 0.5) AS Interest_rate_of_the_issue_traded,
            
              
              ((CASE WHEN SAFE_CAST(@days_elapsed as float64) IS NULL THEN cols.Days_between_maturity_date_and_trade_date
            ELSE SAFE_CAST(@days_elapsed as float64)
            END) - 4.42480705e+03)/POWER(8.97276123e+06, 0.5) AS Days_between_maturity_date_and_trade_date,
            
              
              ((CASE WHEN SAFE_CAST(@yield as float64) IS NULL THEN cols.The_yield_of_the_trade
            ELSE SAFE_CAST(@yield as float64)
            END) - 2.37509641e+00)/POWER(7.67943277e+00, 0.5) AS The_yield_of_the_trade,
            (cols.Issuer_Industry - 1.05359266e+02)/POWER(5.65820163e+00, 0.5) AS Issuer_Industry,
            (cols.Issue_Size - 1.60207594e+08)/POWER(4.60561611e+16, 0.5) AS Issue_Size,
            
              
              ((CASE WHEN SAFE_CAST(@maturity_size as float64) IS NULL THEN cols.MaturitySize
            ELSE SAFE_CAST(@maturity_size as float64)
            END) - 1.88481748e+07)/POWER(1.76399804e+15, 0.5) AS MaturitySize,
            (cols.Price_At_Issue - 1.04254845e+02)/POWER(2.82825359e+02, 0.5) AS Price_At_Issue,
            (cols.Yield_at_Issue - 3.41463484e+00)/POWER(2.02232741e+00, 0.5) AS Yield_at_Issue,
            (cols._85_MAge2 - 2.87041104e+04)/POWER(1.59689889e+09, 0.5) AS _85_MAge2,
            (cols._20_24_MAge2 - 1.16123435e+05)/POWER(2.88425738e+10, 0.5) AS _20_24_MAge2,
            (cols._Proficient - 9.58538532e+01)/POWER(1.50505168e+01, 0.5) AS _Proficient,
            (cols.__Non_Hispanic_White - 5.96917660e+01)/POWER(3.68286386e+02, 0.5) AS __Non_Hispanic_White,
            (cols.Ratings1 - 8.01936341e+01)/POWER(6.23614973e+02, 0.5) AS Ratings1,
            (cols.Ratings2 - 8.02749197e+01)/POWER(6.31857179e+02, 0.5) AS Ratings2,
            (cols.Ratings3 - 8.10222250e+01)/POWER(6.27553773e+02, 0.5) AS Ratings3,
            (cols.Dollar_Price_of_the_Trade) AS Dollar_Price_of_the_Trade,
            (cols.CUSIP) AS CUSIP,
            Issuer_Industry_String, Trade_Date_Time, Maturity_date_of_the_issue_traded
            
              FROM `bi-model-development.looker_FINAL.price_muni_boosted_training` AS cols
            -- WHERE CUSIP = @cusip
            ORDER BY Trade_Date DESC
            )
            ) AS model
            LIMIT 10000
      )
SELECT
    ((pricemodel.Days_between_maturity_date_and_trade_date * POWER(8.97276123e+06, 0.5)) + 4.42480705e+03) AS days_elapsed,
    ((pricemodel._10_Year_Treasury_Constant_Maturity_Rate_Percent_Daily_Not_Seasonally_Adjusted * POWER(2.26253201e-01, 0.5)) + 2.38129247e+00) AS ten_year_rate,
    ((pricemodel.Interest_rate_of_the_issue_traded * POWER(1.65615697e+00, 0.5)) + 4.21839171e+00) AS interest_rate,
    ((pricemodel.MaturitySize * POWER(1.76399804e+15, 0.5)) + 1.88481748e+07) AS maturity_size,
    
	ROUND(pricemodel.predicted_Dollar_Price_of_the_trade, 2)  AS predicted_price,
	ROUND(pricemodel.predicted_Dollar_Price_of_the_trade  +5, 2)  AS pricemodel_max_predicted_dollar_price_of_the_trade_1,
    ROUND(pricemodel.predicted_Dollar_Price_of_the_trade  -5, 2)  AS pricemodel_min_predicted_dollar_price_of_the_trade_1,
    ((pricemodel.The_yield_of_the_trade * POWER(7.67943277e+00, 0.5)) + 2.37509641e+00) AS yield,
    pricemodel.CUSIP AS CUSIP
FROM pricemodel
LIMIT 10000
