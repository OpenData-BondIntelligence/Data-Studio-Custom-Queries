WITH risk_predicted_muni AS (SELECT * FROM ML.PREDICT(MODEL `bi-model-development.risk_regression.model9_substitute_v2`,
              (
              SELECT int64_field_0, Year, Population, Number_of_Social_Associations, AR___10000, AR_Quartile,
              _80th_Percentile_Income,
              
                ((CASE WHEN SAFE_CAST(@_20th_percentile_income as float64) IS NULL THEN ((_20th_Percentile_Income * POWER(4.03997641e+07, 0.5)) + 2.36754978e+04)
              ELSE SAFE_CAST(@_20th_percentile_income as float64)
              END) - 2.36754978e+04)/POWER(4.03997641e+07, 0.5) AS _20th_Percentile_Income,
              Top_Performers_s2, IR_Quartile, FIPS,
              __Hispanic, _not_Hispanic, __Non_Hispanic_White, Not_Proficient_in_English, _Proficient,
              __Rural, _Non_Rural, Segregation_index____black_white, Segregation_Index___non_white_white,
              Average_Daily_PM2_5, ADP_Quartile, POV_Quartile_poll, Total_households_dem,
              Average_household_size_dem, Average_family_size_dem, Elementary_school_Enrollment_grades_1_8_dem,
              High_school_dem, College_or_graduate_school_dem, Percent_high_school_graduate_or_higher_dem,
              Percent_bachelor_s_degree_or_higher_dem, Europe_dem, Asia_dem, Africa_dem, Oceania_dem,
              South_America_dem, Language_other_than_English_dem, Percent_Language_other_than_English_dem,
              Trade_Date, Trade_Type_Indicator_1, Trade_Type_Indicator_2, Trade_Type_Indicator_3,
              Interest_rate_of_the_issue_traded,
              
                ((CASE WHEN SAFE_CAST(@days_elapsed as float64) IS NULL THEN ((Days_between_maturity_date_and_trade_date * POWER(9.02023371e+06, 0.5)) +4.49058080e+03)
              ELSE SAFE_CAST(@days_elapsed as float64)
              END) - 4.49058080e+03 )/POWER(9.02023371e+06, 0.5) AS Days_between_maturity_date_and_trade_date,
              The_par_value_of_the_trade,
              
                ((CASE WHEN SAFE_CAST(@dollar_price as float64) IS NULL THEN ((Dollar_Price_of_the_trade * POWER(2.97842972e-02, 0.5)) + 1.05344438e+02 )
              ELSE SAFE_CAST(@dollar_price as float64)
              END) - 1.05344438e+02 )/POWER(1.27098879e+02, 0.5) AS Dollar_Price_of_the_trade,
              Broker_s_Broker_Indicator_1,
              Broker_s_Broker_Indicator_2, Broker_s_Broker_Indicator_3, List_Offering_Price_Takedown_Indicator_1,
              List_Offering_Price_Takedown_Indicator_2, Version_Number, Alternative_Trading_System_ATS_Indicator_1,
              Alternative_Trading_System_ATS_Indicator_2, Non_Transaction_Based_Compensation_Arrangement_NTBC_Indicator_1,
              Non_Transaction_Based_Compensation_Arrangement_NTBC_Indicator_2, White_MR2, Black_or_African_American_MR2,
              American_Indian_and_Alaska_Native_MR2, Asian_MR2, Native_Hawaiian_and_Other_Pacific_Islander_MR2,
              Some_other_race_MR2, Two_or_more_races_MR2, Total_Population_MAge2, Male_MAge2, Female_MAge2, _0_5_MAge2,
              _5_9_MAge2, _10_14_MAge2, _15_19_MAge2, _20_24_MAge2, _25_29_MAge2, _30_34_MAge2, _35_39_MAge2, _40_44_MAge2,
              _45_49_MAge2, _50_54_MAge2, _55_59_MAge2, _60_64_MAge2, _70_74_MAge2, _75_79_MAge2, _80th_Percentile_Income_1,
              Median_age_MAge2, Percent_Vacant_housing_units_MH, Owner_occupied_MH, Renter_occupied_MH, Percent_Occupied_housing_units_MH,
              Less_than_50_000_MH, _50_000_to_99_999_MH, _100_000_to_149_999_MH, _150_000_to_199_999_MH, _200_000_to_299_999_MH,
              _300_000_to_499_999_MH, _500_000_to_999_999_MH, _1_000_000_or_more_MH, Median_Property_Value_dollars_MH,
              
                ((CASE WHEN SAFE_CAST(@median_rent as float64) IS NULL THEN ((Median_Gross_Rent_dollars_MH * POWER(8.41262659e+04, 0.5)) + 1.01046345e+03)
              ELSE SAFE_CAST(@median_rent as float64)
              END) - 1.01046345e+03)/POWER(8.41262659e+04, 0.5) AS Median_Gross_Rent_dollars_MH,
              
                ((CASE WHEN SAFE_CAST(@unemployment_rate as float64) IS NULL THEN ((Unemployment_Rate_eco * POWER(4.17357343e+00 , 0.5)) +  5.57808766e+00 )
              ELSE SAFE_CAST(@unemployment_rate as float64)
              END) -  5.57808766e+00 )/POWER(4.17357343e+00 , 0.5) AS Unemployment_Rate_eco,
              
                
                ((CASE WHEN SAFE_CAST(@poverty_rate as float64) IS NULL THEN ((Poverty_Rate_eco * POWER(2.65171930e+01 , 0.5)) + 1.34988353e+01 )
              ELSE SAFE_CAST(@poverty_rate as float64)
              END) - 1.34988353e+01 )/POWER(2.65171930e+01 , 0.5) AS Poverty_Rate_eco,
              High_school_graduate_edu,
              Some_college_no_degree_edu, Associate_s_degree_edu, Bachelor_s_degree_edu, Graduate_or_professional_degree_edu,
              Coupon_Frequency, Issuer_Industry_1, Issuer_Industry_2, Issuer_Industry_3, Issuer_Industry_4, Issuer_Industry_5,
              Issuer_Industry_6, Issuer_Industry_7, Issuer_Industry_8, Issuer_Industry_9, Issuer_Industry_10,
              Issuer_Industry_11, Issuer_Industry_12, Issuer_Industry_13, Issuer_Industry_14, Issuer_Industry_15,
              Issuer_Industry_16, Issuer_Industry_17, Issuer_Industry_18, Issuer_Industry_19, Issuer_Industry_20,
              Issuer_Industry_22, Issuer_Type_1, Issuer_Type_2, Issuer_Type_3, Issuer_Type_4, Issuer_Type_5, Issuer_Type_6,
              Issuer_Type_7, Issue_Size, Price_At_Issue, Yield_at_Issue, TaxProvision_1, TaxProvision_2, TaxProvision_3,
              TaxProvision_4, TaxProvision_5, TaxProvision_6, TaxProvision_7, TaxProvision_8, TaxProvision_9,
              TaxProvision_10, TaxProvision_11, Ratings1, Ratings2, Ratings3, CUSIP
              FROM `bi-model-development.risk_regression.muni_new_July13`
              -- WHERE CUSIP = @cusip
              ORDER BY Trade_Date DESC
              LIMIT 10000
              )
              )
         )
--SELECT
--    CASE WHEN (((risk_predicted_muni.predicted_Yield_Treasury * POWER(8.27610513, 0.5)) - 0.01111986 + 1)/2) > 1 THEN 1.00
--    WHEN (((risk_predicted_muni.predicted_Yield_Treasury * POWER(8.27610513, 0.5)) - 0.01111986 + 1)/2) < 0 THEN 0.00
--    ELSE (((risk_predicted_muni.predicted_Yield_Treasury * POWER(8.27610513, 0.5)) - 0.01111986 + 1)/2) END AS --risk_predicted_muni_evaluated_risk,
	
--	risk_predicted_muni._20th_Percentile_Income AS _20th_percentile_income,

--	risk_predicted_muni.Days_between_maturity_date_and_trade_date AS days_elapsed,

--	risk_predicted_muni.Dollar_Price_of_the_trade AS dollar_price,

--	risk_predicted_muni.Median_Gross_Rent_dollars_MH AS median_rent,

--	risk_predicted_muni.Unemployment_Rate_eco AS unemployment_rate,

--	risk_predicted_muni.Poverty_Rate_eco AS poverty_rate,

--	risk_predicted_muni.CUSIP AS CUSIP
SELECT
    ROUND(((risk_predicted_muni._20th_Percentile_Income * POWER(4.03997641e+07, 0.5)) + 2.36754978e+04), 4) AS _20th_percentile_income,
    ROUND(((risk_predicted_muni.Days_between_maturity_date_and_trade_date * POWER(9.02023371e+06, 0.5)) + 4.49058080e+03), 4)  AS days_elapsed,
    ROUND(((risk_predicted_muni.Dollar_Price_of_the_trade * POWER(2.97842972e-02, 0.5)) + 1.05344438e+02), 4)  AS dollar_price,
    CASE WHEN (((risk_predicted_muni.predicted_Yield_Treasury * POWER(8.27610513, 0.5)) - 0.01111986 + 1)/2) > 1 THEN 1.00
    WHEN (((risk_predicted_muni.predicted_Yield_Treasury * POWER(8.27610513, 0.5)) - 0.01111986 + 1)/2) < 0 THEN 0.00
    ELSE ROUND((((risk_predicted_muni.predicted_Yield_Treasury * POWER(8.27610513, 0.5)) - 0.01111986 + 1)/2), 6) END AS risk_predicted_muni_evaluated_risk,
    ROUND(((risk_predicted_muni.Median_Gross_Rent_dollars_MH * POWER(8.41262659e+04, 0.5)) + 1.01046345e+03), 4)  AS median_rent,
    ROUND(((risk_predicted_muni.Poverty_Rate_eco  * POWER(2.65171930e+01 , 0.5)) + 1.34988353e+01), 4)  AS poverty_rate,
    ROUND(((risk_predicted_muni.Unemployment_Rate_eco * POWER(4.17357343e+00, 0.5)) +  5.57808766e+00), 4)  AS unemployment_rate,

	risk_predicted_muni.CUSIP AS CUSIP

FROM risk_predicted_muni
LIMIT 10000
