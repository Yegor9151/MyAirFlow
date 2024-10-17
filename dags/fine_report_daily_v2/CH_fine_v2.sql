WITH
    toDate('<DS>') AS DS,
    toDate('<DE>') AS DE,
    transactions AS (
        SELECT
            pio.CreateDateTimeUtc + INTERVAL 3 HOUR AS CreateDateTime
            , dct.country       AS Country
            , row_number() OVER (PARTITION BY oio.AccountId ORDER BY oio.CreateDateTimeUtc DESC) AS rn
            , laaco.Name        AS City
            , pio.VendorId      AS VendorId
            , osm.vendor_id     AS vendor_id
            , osm.company_name  AS Company
            , coalesce(pio.DebitBonus_Culture, pio.CreditBonus_Culture) AS Currency
            , pio.Comment       AS Comment
            , pio.id            AS TransactionsId
            , pio.AccountId     AS AccountId
            , ifNull(pio.BonusBalanceBefore_Value, 0)  AS BonusBalanceBefore_Value
            , ifNull(pio.DebitBonus_Value, 0) + ifNull(pio.DebitFiat_Value, 0)      AS Debit_Value
            , ifNull(pio.CreditBonus_Value, 0) + ifNull(pio.CreditFiat_Value, 0)    AS Credit_Value
        FROM
            payments pio
            JOIN orgs osm ON osm.VendorId = pio.VendorId
            LEFT JOIN currency dcr ON dcr.currency_culture = Currency
            LEFT JOIN country dct ON dct.country_code = dcr.country_code
            JOIN orders oio
                ON oio.AccountId = pio.AccountId
                AND oio.CreateDateTimeUtc < DE
            JOIN locations laaco ON laaco.id = oio.CityId
        WHERE
            lower(pio.Comment) LIKE '%штраф%'
            AND pio.CreateDateTimeUtc + INTERVAL 3 HOUR >= DS
            AND pio.CreateDateTimeUtc + INTERVAL 3 HOUR < DE
            AND oio.CreateDateTimeUtc < pio.CreateDateTimeUtc
    )
SELECT
    toString(toDate(t.CreateDateTime)) AS Date
    , t.Country AS Country 
    , t.City    AS City
    , toString(t.VendorId)  AS Vendor
    , toString(t.vendor_id) AS VendorId
    , t.Company     AS Company 
    , t.Currency    AS Currency
    , t.Comment     AS Comment
    , uniqExact(t.TransactionsId)   AS Transactions
    , uniqExact(t.AccountId)        AS Users
    , sum(t.Debit_Value)    AS Fine
    , sum(
        if(t.BonusBalanceBefore_Value <= 0, 0, 
        if(t.BonusBalanceBefore_Value <= t.Debit_Value, t.BonusBalanceBefore_Value, t.Debit_Value))) AS GMV
    , sum(t.Credit_Value)   AS Compensation
FROM
    transactions t
WHERE
    rn = 1
GROUP BY
    Date
    , Country
    , City
    , Vendor
    , VendorId
    , Company
    , Currency
    , Comment
