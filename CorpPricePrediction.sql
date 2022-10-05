WITH corp_pricemodel AS (SELECT * FROM (
          SELECT model.dense_65 AS predicted_Dollar_Price_of_the_trade, model.dense_60_input[OFFSET(13)] AS T_Volume,
          model.dense_60_input[OFFSET(21)] AS DURATION, "0_Coupon" AS Used_Model, CUSIP
          FROM ML.PREDICT(MODEL `bi-model-development.corp_price_models.price_corp_coupon_0`,(
          SELECT[
          (cols.BOND_TYPE_1 - 9.40359555e-01)/POWER(5.60834625e-02, 0.5),
          (cols.BOND_TYPE_2 - 4.49919148e-02)/POWER(4.29676424e-02, 0.5),
          (cols.BOND_TYPE_3 - 1.46485304e-02)/POWER(1.44339509e-02, 0.5),
          (cols.SECURITY_LEVEL_1 - 9.86492914e-01)/POWER(1.33246451e-02, 0.5),
          (cols.SECURITY_LEVEL_2 - 1.31266052e-02)/POWER(1.29542974e-02, 0.5),
          (cols.SECURITY_LEVEL_4 - 2.85360982e-04)/POWER(2.85279551e-04, 0.5),
          (cols.CONV - 2.56824883e-03)/POWER(2.56165293e-03, 0.5),
          (cols.OFFERING_AMT - 4.58271057e+04)/POWER(5.05928431e+10, 0.5),
          (cols.PRINCIPAL_AMT - 8.97955969e+02)/POWER(1.46344628e+05, 0.5),
          (cols.COUPON - 7.10311043e-03)/POWER(4.07679326e-02, 0.5),
          (cols.NCOUPS - 0.00000000e+00),
          (cols.RATING_CLASS_1 - 9.96385428e-01)/POWER(3.60150730e-03, 0.5),
          (cols.RATING_CLASS_2 - 3.61457243e-03)/POWER(3.60150730e-03, 0.5),
          ((CASE WHEN SAFE_CAST(@trade_volume as float64) IS NULL THEN cols.T_Volume
            ELSE SAFE_CAST(@trade_volume as float64)
            END) - 7.10775199e+06)/POWER(2.46348681e+15, 0.5),
          (cols.COUPMONTH - 3.04991439e+01)/POWER(9.94538024e+02, 0.5),
          (cols.COUPAMT - 0.00000000e+00),
          (cols.COUPACC - 0.00000000e+00),
          (cols.MULTICOUPS - 1.00000000e+00),
          (cols.RET_EOM - 3.39245770e+00)/POWER(4.24137340e+01, 0.5),
          (cols.TMT - 1.13152022e+00)/POWER(4.35193746e-01, 0.5),
          (cols.REMCOUPS - 1.00000000e+00),
          ((CASE WHEN SAFE_CAST(@duration as float64) IS NULL THEN cols.DURATION
            ELSE SAFE_CAST(@duration as float64)
            END) - 3.26492581e+00)/POWER(2.41067510e+01, 0.5),
          (cols.DEFAULTED_1 - 9.95481784e-01)/POWER(4.49780127e-03, 0.5),
          (cols.DEFAULTED_2 - 4.51821554e-03)/POWER(4.49780127e-03, 0.5)] AS dense_60_input, CUSIP
          FROM `bi-model-development.corp_price_models.Price_corp_coupon_0_dataset` AS cols
            --WHERE CUSIP = @cusip
          )) AS model
          UNION ALL
          SELECT model_2.dense_4 AS predicted_Dollar_Price_of_the_trade, model_2.dense_input[OFFSET(16)] AS T_Volume,
          model_2.dense_input[OFFSET(24)] AS DURATION, "Positive_Coupon" AS Used_Model, CUSIP
          FROM ML.PREDICT(MODEL `bi-model-development.corp_price_models.price_corp_positive_coupon` ,(
          SELECT[
          (cols_2.BOND_TYPE_1 - 8.71200102e-01)/POWER(1.12210484e-01, 0.5),
          (cols_2.BOND_TYPE_2 - 1.28766350e-01)/POWER(1.12185577e-01, 0.5),
          (cols_2.SECURITY_LEVEL_1 - 8.76920131e-01)/POWER(1.07931215e-01, 0.5),
          (cols_2.SECURITY_LEVEL_2 - 6.66162506e-02)/POWER(6.21785258e-02, 0.5),
          (cols_2.SECURITY_LEVEL_3 - 3.68307238e-02)/POWER(3.54742216e-02, 0.5),
          (cols_2.SECURITY_LEVEL_4 - 1.90136445e-02)/POWER(1.86521258e-02, 0.5),
          (cols_2.SECURITY_LEVEL_5 - 1.38387796e-04)/POWER(1.38368645e-04, 0.5),
          (cols_2.SECURITY_LEVEL_6 - 4.80862645e-04)/POWER(4.80631416e-04, 0.5),
          (cols_2.CONV - 8.89036751e-04)/POWER(8.88246365e-04, 0.5),
          (cols_2.PRINCIPAL_AMT - 1.14364056e+03)/POWER(1.92992571e+07, 0.5),
          (cols_2.COUPON - 5.86444853e+00)/POWER(3.44041240e+00, 0.5),
          (cols_2.NCOUPS - 2.13784962e+00)/POWER(1.29649090e+00, 0.5),
          (cols_2.N_FR - 7.91656474e+00)/POWER(9.21493850e+00, 0.5),
          (cols_2.RATING_NUM - 8.19544271e+00)/POWER(9.04790312e+00, 0.5),
          (cols_2.RATING_CLASS_1 - 8.50078490e-01)/POWER(1.27445051e-01, 0.5),
          (cols_2.RATING_CLASS_2 - 1.49921510e-01)/POWER(1.27445051e-01, 0.5),
          ((CASE WHEN SAFE_CAST(@trade_volume as float64) IS NULL THEN cols_2.T_Volume
            ELSE SAFE_CAST(@trade_volume as float64)
            END) - 4.10469721e+07)/POWER(1.50954044e+16, 0.5),
          (cols_2.T_Yld_Pt - 4.87351403e+00)/POWER(1.33382536e+02, 0.5),
          (cols_2.COUPMONTH - 6.71115853e+01)/POWER(3.66969656e+03, 0.5),
          (cols_2.COUPAMT - 4.88843386e-01)/POWER(1.31262653e+00, 0.5),
          (cols_2.COUPACC - 1.51541008e+00)/POWER(1.03854277e+00, 0.5),
          (cols_2.MULTICOUPS - 1.01171544e+00)/POWER(6.52446919e-02, 0.5),
          (cols_2.TMT - 9.49545737e+00)/POWER(1.01405062e+02, 0.5),
          (cols_2.REMCOUPS - 2.06242338e+01)/POWER(6.32301828e+02, 0.5),
          ((CASE WHEN SAFE_CAST(@duration as float64) IS NULL THEN cols_2.DURATION
            ELSE SAFE_CAST(@duration as float64)
            END) - 5.85010920e+00)/POWER(1.82794824e+01, 0.5),
          (cols_2.DEFAULTED_1 - 9.95269654e-01)/POWER(4.70797031e-03, 0.5),
          (cols_2.DEFAULTED_2 - 4.73034649e-03)/POWER(4.70797031e-03, 0.5)
          ] AS dense_input, CUSIP FROM `bi-model-development.corp_price_models.Price_corp_positive_coupon_dataset` AS cols_2
           --WHERE CUSIP = @cusip
          )) AS model_2)
      )
SELECT
    ROUND(corp_pricemodel.predicted_Dollar_Price_of_the_trade, 7)  AS corp_pricemodel_predicted_dollar_price_of_the_trade_1,
    CASE WHEN (corp_pricemodel.Used_Model = "0_Coupon") THEN ((corp_pricemodel.T_Volume * POWER(2.46348681e+15, 0.5)) + 7.10775199e+06)
    ELSE ROUND(((corp_pricemodel.DURATION * POWER(1.50954044e+16, 0.5)) + 4.10469721e+07), 4) END  AS corp_pricemodel_t_volume_1,
    CASE WHEN (corp_pricemodel.Used_Model = "0_Coupon") THEN ((corp_pricemodel.DURATION * POWER(2.41067510e+01, 0.5)) + 3.26492581e+00)
    ELSE ROUND(((corp_pricemodel.DURATION * POWER(1.82794824e+01, 0.5)) + 5.85010920e+00), 4) END  AS corp_pricemodel_duration_1,

	corp_pricemodel.CUSIP AS CUSIP


FROM corp_pricemodel
