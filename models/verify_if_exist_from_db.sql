SELECT
    *
FROM
    PUBLIC.nba_info
WHERE
    rang =%s
    AND ligue_ses =%s
    AND conference =%s
    AND equipe =%s
    AND ligue =%s
    AND saison =%s
    AND pays =%s
    AND pays_code =%s
    AND match_joue =%s
    AND match_win =%s
    AND match_percentage_win =%s
    AND match_lose =%s
    AND match_percentage_lose =%s

