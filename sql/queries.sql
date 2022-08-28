with temp as(
    SELECT fromAddress as 'user', sum(value) as retiredNCT
    FROM toucan_nct nct
    WHERE toAddress = '0x0000000000000000000000000000000000000000'
    GROUP BY fromAddress
)
SELECT COALESCE(ens, "user") as 'user', retiredNCT
FROM temp nct
LEFT JOIN toucan_ens ens ON nct."user" = ens.address
ORDER BY retiredNCT DESC

SELECT contractAddress as 'contract', sum(value) as retiredNCT
FROM [dbo].[toucan_nct]
WHERE toAddress = '0x0000000000000000000000000000000000000000'
GROUP BY contractAddress
ORDER BY retiredNCT DESC;

SELECT DISTINCT fromAddress 
FROM toucan_nct
WHERE toAddress = '0x0000000000000000000000000000000000000000'