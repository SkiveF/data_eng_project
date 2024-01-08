"INSERT INTO public.nba_info (rang,"
                                "ligue_ses,conference,equipe, ligue ,saison ,"
                                "pays , pays_code ,match_joue,match_win, "
                                "match_percentage_win,match_lose,"
                                "match_percentage_lose)VALUES (%s, %s, %s,%s,"
                                "%s, %s, %s, %s, %s, %s, %s,  %s, %s)",
                                (row['rang'], row['ligue_ses'],
                                 row['conference'],
                                 row['equipe'], row['ligue'],
                                 row['saison'], row['pays'],
                                 row['pays_code'],
                                 row['match_joue'],
                                 row['match_win'],
                                 row['match_percentage_win'],
                                 row['match_lose'],
                                 row['match_percentage_lose'])