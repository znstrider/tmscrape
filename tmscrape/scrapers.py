import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
import requests
import os
import pandas as pd
import numpy as np
import re
import time
from datetime import datetime


HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
DELAY = 2


POSITION_NAMES = ['Torwart',
                  'Innenverteidiger',
                  'Linker Verteidiger',
                  'Rechter Verteidiger',
                  'Linkes Mittelfeld',
                  'Rechtes Mittelfeld',
                  'Defensives Mittelfeld',
                  'Zentrales Mittelfeld',
                  'Offensives Mittelfeld',
                  'Hängende Spitze',
                  'Linksaußen',
                  'Rechtsaußen',
                  'Mittelstürmer']

POSITION_NAMES_ENG = ['Goalkeeper',
                      'Centre-Back',
                      'Left-Back',
                      'Right-Back',
                      'Left Midfield', 
                      'Right Midfield',
                      'Defensive Midfield',
                      'Central Midfield',
                      'Attacking Midfield',
                      'Second Striker',
                      'Left Winger',
                      'Right Winger',
                      'Centre-Forward']

POSITION_ABBREV = ['TW',
                   'IV',
                   'LV',
                   'RV',
                   'LM',
                   'RM',
                   'DM',
                   'ZM',
                   'OM',
                   'HS',
                   'LA',
                   'RA',
                   'ST']

POSITION_ABBREV_ENG = ['GK',
                       'CB',
                       'LB',
                       'RB',
                       'LM',
                       'RM',
                       'DM',
                       'CM',
                       'AM',
                       'SS',
                       'LW',
                       'RW',
                       'ST']


def get_page_tree_and_soup(url, headers=HEADERS):
    pageTree = requests.get(url, headers=headers)
    soup = BeautifulSoup(pageTree.content, 'html.parser')
    return pageTree, soup


def get_table_columns(thead):
    '''
    parses an html tablehead (thead) into a list of column names
    '''
    return [t.text for t in thead.find_all('th')]


def get_table_from_tbody(tbody,
                         columns=None,
                         strip=False,
                         rid_empty=True):
    '''
    parses an html table into a DataFrame
    
    Parameters:
    ----------
    tbody: an html tbody element

    Returns:
    ----------
    a DataFrame
    '''
    data = []
    rows = tbody.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [element.text for element in cols]
        if strip:
            cols = [element.strip() for element in cols]
        if rid_empty:
            data.append([element for element in cols if element]) # Get rid of empty values
        else:
            data.append([element for element in cols])
        
    if columns is not None:
        return pd.DataFrame(data, columns = columns)
    else:
        return pd.DataFrame(data)


def clean_market_vals(series):
    series = (series
                .str.replace('ablösefrei', '0')
                .str.replace('-', '0'))
    mvdf = (series.str.split(' ', expand=True))
    mvdf[0] = mvdf[0].str.replace('-', '0')
    if mvdf.shape[1] > 1:
        mvdf[1] = (mvdf[1].str.replace('Tsd.', '1000')
                          .str.replace('Mio.', '1000000')
                          .str.replace('Mrd.', '1000000000').astype('float'))
        series = (mvdf[0].str.replace(',', '.').astype('float') * mvdf[1].fillna(0)).astype('int')
    return series


def get_competition_list(competition_string):
    """
    get all competitions listed on transfermarkt

    str competition_string: one of ['europa', 'asien', 'afrika', 'amerika', 'europaJugend']

    Returns:
    --------
    DataFrame with columns:
        ['League_Name', 'n_clubs', 'n_players', 'avg_age', 'pct_legionary',
         'Market_Value', 'League_Type', 'competition_string', 'Country']

        League_Type: ie 1st Division, Cup ...

    """
    assert competition_string in ['europa', 'asien', 'afrika', 'amerika', 'europaJugend'], "competition_string must be in ['europa', 'asien', 'afrika', 'amerika', 'europaJugend']"

    base_url = 'https://www.transfermarkt.de'
    url = f'https://www.transfermarkt.de/wettbewerbe/{competition_string}'

    pageTree, soup = get_page_tree_and_soup(url)

    avail_pages = soup.find_all('div', {'class': 'pager'})

    try:
        affix, last_page = avail_pages[0].find_all('li')[-1].find('a')['href'].split('=')
        affix += '='

        pages = [base_url + affix + str(nr) for nr in range(2, int(last_page)+1)]
    except:
        pages = []

    def get_competition_list(soup):
        tbody = soup.find_all('tbody')[0]

        table = get_table_from_tbody(tbody, strip=True)

        mask = pd.isna(table.iloc[:, 1:]).all(axis=1)

        ligen = pd.Series(index=table.index)
        ligen.loc[mask & (table[0] != table[0].shift(1))] = table.loc[mask & (table[0] != table[0].shift(1)), 0]

        ligen = ligen.fillna(method='ffill')
        table['League_Type'] = ligen

        table = table.iloc[:, 1:]

        table = table.loc[~mask]

        comp_links = [a['href'].split('/')[-1] for a in tbody.find_all('a')
                      if 'startseite/wettbewerb' in a['href']][1::2]

        table.columns = ['League_Name', 'n_clubs', 'n_players', 'avg_age', 'pct_legionary', 'Market_Value', 'League_Type']

        table = table.reset_index(drop=True)
        table['competition_string'] = comp_links

        countries = [img['title'] for img in tbody.find_all('img', {'class': "flaggenrahmen"})]
        table['Country'] = countries

        return table

    table = get_competition_list(soup)

    dfs = [table]

    for page in pages:
        _, soup = get_page_tree_and_soup(page)
        table = get_competition_list(soup)
        dfs.append(table)

    competitions = pd.concat(dfs).reset_index(drop=True)

    competitions.dtypes

    competitions['n_clubs'] = competitions['n_clubs'].astype('int')
    competitions['n_players'] = competitions['n_players'].str.replace('.', '').astype('int')

    competitions['avg_age'] = competitions['avg_age'].str.replace(',', '.').astype('float')
    competitions['pct_legionary'] = pd.to_numeric(competitions['pct_legionary']
                                                    .str.replace(',', '.')
                                                    .str.replace(' %', '')
                                                    .str.replace('%', ''), errors='coerce')

    def clean_market_val(series):
        base, mmm, _ = series.str.split(' ', expand=True).values.T

        base = np.array([float(val.replace('-', '0')
                               .replace(',', '.')
                               .replace('ablösefrei', '0')) for val in base])

        mmm = np.array(mmm)
        mmm = np.array([1000000000 if 'Mrd.' in str(m)
                            else 1000000 if 'Mio.' in str(m)
                            else 1000 if 'Tsd.' in str(m)
                            else 0 for m in mmm])

        return pd.Series(base * mmm, index=series.index)

    competitions['Market_Value'] = clean_market_val(competitions['Market_Value'])

    return competitions


def get_clubnames_league(league_abbrev,
                         league_name=None,
                         season_id=None,
                         headers={'User-Agent':
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
                        ):
    """

    get the tm specific club names and club ids for one league
    Parameters:
    -----------

    league_abbrev:    ie: for the premier league: GB1
    league_name: tm league name ie.: premier-league
    season_id: season, ie.: 2018 for 2018/19

    Returns:
    -----------
    an N x 2 array of club names and club ids 
    [['manchester-city', '281'],
     ...
    ]

    """

    if (league_name is None) and (season_id is None):
        pageTree = requests.get(f'https://www.transfermarkt.de/jumplist/startseite/wettbewerb/{league_abbrev}',
                                headers=headers)
    else:
        if season_id is None:
            pageTree = requests.get(f'https://www.transfermarkt.de/{league_name}/startseite/wettbewerb/{league_abbrev}/',
                                    headers=headers)
        else:    
            pageTree = requests.get(f'https://www.transfermarkt.de/{league_name}/startseite/wettbewerb/{league_abbrev}/plus/?saison_id={season_id}',
                                    headers=headers)

    soup = BeautifulSoup(pageTree.content, 'html.parser')

    links = soup.find_all('a',  {"class": "vereinprofil_tooltip"})
    links = [link['href'] for link in links]

    #clean_links = pd.Series(links).value_counts().index[18:36]
    mask = pd.Series(links).value_counts().index.str.contains('startseite')
    clean_links = pd.Series(links).value_counts().index[mask]    

    club_names_ids = np.array([[link.split('/')[1], link.split('/')[4]] for link in clean_links])
    return club_names_ids


def get_club_colors(club,
                    club_id,
                    headers={'User-Agent': 
           'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
                   ):
    '''
    scrapes the club colors from the "Daten&Fakten - Vereinsportrait" Transfermarkt page

    Parameters:
    -----------
    club_id: the transfermarkt club specific id, ie 16
    club: the transfermarkt club name, ie. borussia-dortmund

    Returns:
    -----------
    farben: a list of club colors
    '''
    vereinsfarben_link = f'https://www.transfermarkt.de/{club}/datenfakten/verein/{club_id}'
    pageTree = requests.get(vereinsfarben_link, headers=headers)
    soup = BeautifulSoup(pageTree.content, 'html.parser')
    farben = soup.find_all("p", {"class": "vereinsfarbe"})

    # There are teams for which there are no club colors specified
    if len(farben) > 0:
        farben = farben[0].find_all('span')
        farben = [f['style'].split(':')[1].replace(';', '') for f in farben]
        if '' in farben:
            farben.remove('')
    else:
        farben = ['w', 'k']

    return farben


def get_club_emblem(club_id):
    '''
    Parameters:
    -----------
    club_id: the transfermarkt club specific id

    Returns:
    -----------
    img: an image array
    '''
    img = plt.imread(f'https://tmssl.akamaized.net//images/wappen/big/{club_id}.png')
    return img


def scrape_team_league_placements(club_name,
                                  club_id,
                                  save = False,
                                  headers={'User-Agent': 
               'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}):
    '''
    scrape the 'Historische Platzierungen' page from tm to attain league, placement, coach, points etc.

    Parameters:
    -----------
    club_id: the transfermarkt club specific id, ie 16
    club: the transfermarkt club name, ie. borussia-dortmund
    save: whether to save the DataFrame to a 'league_placements/' folder
    headers: requests.get headers

    Returns:
    -----------
    df: a DataFrame of historic league placement data
    '''

    url = f'https://www.transfermarkt.de/{club_name}/platzierungen/verein/{club_id}'
    print('scraping ', url)
    pageTree = requests.get(url, headers=headers)
    soup = BeautifulSoup(pageTree.content, 'html.parser')
    table_body = soup.find_all('tbody')[1]
    platzierungen_columns = ['Saison', 'Liga', 'Ligahöhe', 'W', 'D', 'L', 'Tore', 'GD', 'Punkte', 'Platz', 'Trainer']
    df = get_table_from_tbody(table_body, columns = platzierungen_columns)
    df[['GF', 'GA']] = pd.DataFrame(df['Tore'].str.split(':').tolist(), columns = ['GF', 'GA'])
    df[['W', 'D', 'L', 'GD', 'Platz', 'GF', 'GA']] =\
        pd.DataFrame([df[col].astype('int')
                      for col in ['W', 'D', 'L', 'GD', 'Platz', 'GF', 'GA']]).T
    df['Pts'] = df['W']*3+df['D']
    df['Games Played'] = df[['W', 'L', 'D']].sum(1)

    liga_img_links = table_body.find_all('img', {'class': ''})
    liga_img_links = [t['src'].replace('verysmall', 'medium') for t in liga_img_links]

    df['Liga Image Links'] = liga_img_links

    if save:
        if os.path.isdir('league_placements') == False:
            os.makedirs('league_placements')
        df.to_csv(f'league_placements/{club}_league_placements.csv')    

    return df


def scrape_gameweek_placements(club_name,
                               club_id,
                               save=False,
                               max_spieltage=38,
                               sleep=DELAY,
                               headers= {'User-Agent': 
               'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}):
    '''
    scrape the 'Historische Platzierungen' page from tm to attain league, placement, coach, points etc
    - for each gameweek.

    Parameters:
    -----------
    club_id: the transfermarkt club specific id, ie 16
    club: the transfermarkt club name, ie. borussia-dortmund
    save: whether to save the DataFrame to a 'gameweek_placements/' folder
    max_spieltage = 38: some leagues and teams may have more gameweeks. That would require checking the scraped
                        table whether it contains info for a given gameweek. Something to be improved.
    sleep = 5: time in seconds to wait between requests
    headers: requests.get headers

    Returns:
    -----------
    df: a DataFrame of historic league placement data
    '''
    link = 'https://www.transfermarkt.de/{}/platzierungen/verein/{}/spieltag/{}'

    columns = ['Saison', 'Liga', 'Ligahöhe',
                    'W', 'D', 'L', 'GF', 'GA', 'GD', 'Punkte',
                    'Platz', 'Trainer', 'img_link']

    all_data = pd.DataFrame(columns=columns)

    for spieltag in np.arange(1, max_spieltage+1):
        # scrape the page
        page = link.format(club_name, club_id, spieltag)

        pageTree = requests.get(page, headers=headers)
        soup = BeautifulSoup(pageTree.content, 'html.parser')
        body = soup.find_all('tbody')[1]
        trs = body.find_all('tr')

        rows = []

        for tr in trs:
            row_text = []
            tds = tr.find_all('td')

            for i, td in enumerate(tds):
                if i == 1:
                    pass
                    # row_text.append(td.find_next('img')['title'])
                elif i == 7:
                    gf, ga = str.split(td.text, ':')
                    row_text.append(gf)
                    row_text.append(ga)
                elif i == 9:
                    row_text.append(str.split(td.text, ':')[0])
                else:
                    row_text.append(td.text)

            img_link = tr.find_next('img')['src'].replace('verysmall', 'medium')
            row_text.append(img_link)
            rows.append(row_text)

        data = pd.DataFrame(np.array(rows), columns=columns)

        data['club'] = club_name.replace('-', ' ').title()
        data['club_href'] = club_href
        data['club_id'] = club_id
        data['Spieltag'] = spieltag

        all_data = pd.concat([all_data, data])

        time.sleep(np.random.rand(1)*(sleep))

    all_data[['W','D','L','GF','GA','GD','Punkte','Platz', 'Spieltag']] = \
                    all_data[['W','D','L','GF','GA','GD','Punkte','Platz', 'Spieltag']].astype('int')

    all_data['Jahr'] = all_data['Saison'].apply(lambda x: '19'+x.split('/')[0] if int(x.split('/')[0]) >= 20 else '20'+x.split('/')[0])
    all_data['Jahr'] = all_data['Jahr'].astype('int')
    all_data = all_data.rename(columns = {'club-href': 'club_href', 'Club' : 'club'})

    if save:
        if os.path.isdir('gameweek_placements') == False:
            os.makedirs('gameweek_placements')
        df.to_csv(f'gameweek_placements/{club}_gameweek_placements.csv')

    return all_data    


def get_club_data(club,
                  club_id,
                  season,
                  league_abbrev=None,
                  save=False):
    '''
    scrapes the 'Kaderdaten' and 'Leistungsdaten' from Transfermarkt for one team and one season

    Parameters:
    -----------
    club: the transfermarkt club name, ie. borussia-dortmund
    club_id: the transfermarkt club specific id
    season: the year the season begins, ie: 2019
    league_abbrev = None: the transfermarkt specific league abbreviation
                          ie: L1 for the Bundesliga, L2 for 2. Bundesliga, GB_ for England, ES_ for Spain etc
            if None, scrapes data for all matches, if not none only for the league
    save = False: whether to save the returned dataframe

    Returns:
    -----------
    kader, leistungsdaten: 'Kaderdaten' and 'Leistungsdaten' DataFrames
    '''

    kader = scrape_kaderdaten(club=club,
                              club_id=club_id,
                              season=season,
                              save=True)

    leistungsdaten = scrape_leistungsdaten(club=club,
                                           club_id=club_id,
                                           season=season,
                                           league_abbrev=league_abbrev,
                                           save=True)
    return kader, leistungsdaten


def scrape_leistungsdaten(club,
                          club_id,
                          season,
                          league_abbrev=None,
                          save=False,
                          headers={'User-Agent': 
           'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
                   ):
    '''
    scrapes the 'Leistungsdaten' from Transfermarkt for one team and one season
    
    Parameters:
    -----------
    club: the transfermarkt club name, ie. borussia-dortmund
    club_id: the transfermarkt club specific id
    season: the year the season begins, ie: 2019
    league_abbrev = None: the transfermarkt specific league abbreviation
                          ie: L1 for the Bundesliga, L2 for 2. Bundesliga, GB_ for England, ES_ for Spain etc
            if None, scrapes data for all matches, if not none only for the league
    save = False: whether to save the returned dataframe
    headers: headers for requests.get 
    
    Returns:
    -----------
    a DataFrame with the scraped data
    '''
    if league_abbrev is not None:
        team_leistungsdaten_link = f'https://www.transfermarkt.de/{club}/leistungsdaten/verein/{club_id}/plus/1?reldata={league_abbrev}%26{season}'
    else:
        team_leistungsdaten_link = f'https://www.transfermarkt.de/{club}/leistungsdaten/verein/{club_id}/reldata/%26{season}/plus/1'

    print('scraping ', team_leistungsdaten_link)

    url = team_leistungsdaten_link
    pageTree = requests.get(url, headers=headers)
    soup = BeautifulSoup(pageTree.content, 'html.parser')
    tables = soup.find_all("div", {"class": "responsive-table"})

    data = []
    table = soup.find_all("div", {"class": "responsive-table"})[0]
    table_body = table.find('tbody')

    rows = table_body.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [element.text for element in cols]
        try:
            player_id = row.find_next('span').find_next('a', {'class': "spielprofil_tooltip"})['id']
            player_string = row.find_next('span').find_next('a', {'class': "spielprofil_tooltip"})['href'].split('/')[1]
        except:
            player_id = ''
            player_string = ''
        
        cols = cols+[player_id, player_string]
        
        data.append([element for element in cols if element]) # Get rid of empty values

    leistungsdaten_columns = ['Shirt Number', 'Name', 'Last Name', 'Position', 'Age',
                                    'In Squad', 'Games Played', 'Goals', 'Assists', 'Yellow', 'Second Yellow',
                                    'Red', 'Substituted On', 'Substituted Off', 'PPM', 'Minutes Played',
                                    'player_id', 'player_string']
    df = pd.DataFrame(data[::3])
    df = (df.iloc[:, :len(leistungsdaten_columns)]
            .rename(columns = dict(zip(df.iloc[:, :len(leistungsdaten_columns)], leistungsdaten_columns))))

    df.loc[(~df['Last Name'].str.contains('.', regex=False))&
        (df['Last Name'].str.contains(' ', regex=False)), 'Name'] =\
        (df.loc[(~df['Last Name'].str.contains('.', regex=False))&
            (df['Last Name'].str.contains(' ', regex=False)), 'Last Name']
            .apply(lambda x: x.split(' ')[0] + ' ' + x.split(' ')[-1])
        )

    df.loc[(~df['Last Name'].str.contains('.', regex=False))&
        (~df['Last Name'].str.contains(' ', regex=False)), 'Name'] =\
    (df.loc[(~df['Last Name'].str.contains('.', regex=False))&
        (~df['Last Name'].str.contains(' ', regex=False)), 'Last Name']
        .str.replace(r"([A-Z])", r" \1")
        .apply(lambda x: x.split(' ')[-1])
    )

    df.loc[df['Last Name'].str.contains('.', regex=False), 'Name'] =\
        df.loc[df['Last Name'].str.contains('.', regex=False), 'Last Name'].apply(lambda x: x.split('.')[0][:-1])

    df['Name'] = df['Name'].str.strip()

    df['Last Name'] = (df['Last Name']
                    .str.replace('.', '')
                    .str.strip()
                    .apply(lambda x: x.split(' ')[-1]
                    .strip()))
    df['Age'] = df.Age.str.replace('†', '').str.replace('-', '25').astype('int')
    for column in df.columns[5:-4]:
        df.loc[:, column] = pd.to_numeric(df.loc[:, column], errors = 'coerce').fillna(0).astype('int')
    df['PPM'] = df['PPM'].str.replace(',', '.').str.replace('-', 'NaN').astype('float')
    df['Minutes Played'] = (df['Minutes Played']
                            .str.replace("'", "")
                            .str.replace('.', '')
                            .str.replace('-', '0')
                            .astype('int'))
    df['Scorer'] = df.Goals + df.Assists
    df['Minutes per Appearance'] = (df['Minutes Played'] / df['Games Played']).fillna(0).astype('int')

    if save:
        if os.path.isdir('Kader-Leistungsdaten') == False:
            os.makedirs('Kader-Leistungsdaten')

        df.to_csv(f'Kader-Leistungsdaten/{club}_Leistungsdaten_{season}.csv')
        print(f'Leistungsdaten {club}-{season} - saved to Kader-Leistungsdaten/{club}_Leistungsdaten_{season}.csv')
    else:   
        print(f'Leistungsdaten {club}-{season} - retrieved')
        
    return df


def scrape_kaderdaten(club,
                      club_id,
                      season,
                      save = False,
                      headers={'User-Agent': 
           'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
                    ):   
    '''
    scrapes the 'Kaderdaten' containing contract duration etc from Transfermarkt for one team and one season
    
    Parameters:
    -----------
    club: the transfermarkt club name, ie. borussia-dortmund
    club_id: the transfermarkt club specific id
    season: the year the season begins, ie: 2019  
    save = False: whether to save the returned dataframe
    headers: headers for requests.get 
    
    Returns:
    -----------
    a DataFrame with the scraped data
    '''
        
    def clean_df(df):
        df.loc[(~df['Last Name'].str.contains('.', regex=False))&
            (df['Last Name'].str.contains(' ', regex=False)), 'Name'] =\
            (df.loc[(~df['Last Name'].str.contains('.', regex=False))&
                (df['Last Name'].str.contains(' ', regex=False)), 'Last Name']
                .apply(lambda x: x.split(' ')[0] + ' ' + x.split(' ')[-1])
            )

        df.loc[(~df['Last Name'].str.contains('.', regex=False))&
            (~df['Last Name'].str.contains(' ', regex=False)), 'Name'] =\
        (df.loc[(~df['Last Name'].str.contains('.', regex=False))&
            (~df['Last Name'].str.contains(' ', regex=False)), 'Last Name']
            .str.replace(r"([A-Z])", r" \1")
            .apply(lambda x: x.split(' ')[-1])
        )

        df.loc[df['Last Name'].str.contains('.', regex=False), 'Name'] =\
            df.loc[df['Last Name'].str.contains('.', regex=False), 'Last Name'].apply(lambda x: x.split('.')[0][:-1])

        df['Name'] = df['Name'].str.strip()
        df['Last Name'] = df['Name'].apply(lambda x: x.split(' ')[-1].strip())

        df['Age'] = df['Date of Birth'].str.replace('†', '').apply(lambda x: x.split('(')[1].split(')')[0])


        df['Date of Birth'] = pd.to_datetime(df['Date of Birth'].apply(lambda x: x.split('(')[0].strip()),
                                                errors = 'coerce', dayfirst=True)

        df['Height'] = (df['Height'].str.replace('k. A.', '')
                        .apply(lambda x: (x.split('m')[0].strip().replace(',', '.')))
                        .replace('', np.nan)
                        .astype('float'))

        df['At Club Since'] = pd.to_datetime(df['At Club Since'].replace('-', np.nan), dayfirst=True, errors = 'coerce')
        df['Contract Expires'] = pd.to_datetime(df['Contract Expires'].replace('-', np.nan), dayfirst=True, errors = 'coerce')
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
        df['Shirt Number'] = df['Shirt Number'].replace('-', 0).astype('int')
        df['Days at Club'] = (datetime.now() - df['At Club Since']).dt.days
        mvdf = (df['Market Value'].str.split(' ', expand = True))
        mvdf[0] = mvdf[0].str.replace('-', '0')
        if mvdf.shape[1] > 1:
            mvdf[1] = mvdf[1].str.replace('Tsd.', '1000').replace('Mio.', '1000000').astype('float')
            df['Market Value'] = (mvdf[0].str.replace(',', '.').astype('float') * mvdf[1].fillna(0)).astype('int')
        return df

    kader_link = f'https://www.transfermarkt.de/{club}/kader/verein/{club_id}/saison_id/{season}/plus/1'
    print('scraping ', kader_link)

    pageTree = requests.get(kader_link, headers=HEADERS)
    soup = BeautifulSoup(pageTree.content, 'html.parser')
    tables = soup.find_all("div", {"class": "responsive-table"})

    ##### Read the HTML Table into lists
    data = []
    table = soup.find_all("div", {"class": "responsive-table"})[0]
    table_body = table.find('tbody')

    rows = table_body.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [element.text for element in cols]
        try:
            player_id = row.find_next('span').find_next('a', {'class': "spielprofil_tooltip"})['id']
            player_string = row.find_next('span').find_next('a', {'class': "spielprofil_tooltip"})['href'].split('/')[1]
        except:
            player_id = ''
            player_string = ''
        img_link = row.find_next('img', {"class": "bilderrahmen-fixed"})
        if img_link is not None:
            img_link = img_link['src'].replace('small', 'big')
        cols = cols+[img_link]+[player_id, player_string]
        data.append([element for element in cols if element]) # Get rid of empty values

    kader_columns = ['Shirt Number', 'Name', 'Last Name', 'Position', 'Date of Birth',
                        'Height', 'Footedness', 'At Club Since', 'Contract Expires', 'Market Value',
                    'Image Link', 'player_id', 'player_string']
    df = pd.DataFrame(data[::3])
    df = (df.iloc[:, :len(kader_columns)].
            rename(columns = dict(zip(df.iloc[:, :len(kader_columns)], kader_columns))))

    df = clean_df(df)
            

    if save:
        if os.path.isdir('Kader-Leistungsdaten') == False:
            os.makedirs('Kader-Leistungsdaten') 
        df.to_csv(f'Kader-Leistungsdaten/{club}_Kader_{season}.csv')
        print(f'Kaderdaten {club}-{season} - saved to Kader-Leistungsdaten/{club}_Kader_{season}.csv')
    else:
        print(f'Kaderdaten {club}-{season} - retrieved')
        
    return df


def get_player_mv_history(player_id, player_string=None):
    """
    get the market value history including:
    date of market value, club and age at the time
    for a specific transfermarkt player id.
    
    Parameters:
    ___________
    int player_id: transfermarkt player specific id
    
    
    Returns:
    –––––––
    a DataFrame with datetime index and ['Market Value', 'Club', 'Age'] Columns
    """

    if player_string is None:
        player_string = 'player'

    url = f'https://www.transfermarkt.de/{player_string}/marktwertverlauf/spieler/{player_id}'
    
    pageTree = requests.get(url, headers=HEADERS)
    
    # we need to decode to get rid of unicode and hexcode character strings like \x20
    soup = BeautifulSoup(pageTree.content.decode('unicode-escape'), 'html.parser')
    
    # search the page content for the content we need
    result = re.search(r"series(.*?)]}", str(soup))
    
    if result is None:
        return pd.DataFrame()
    
    else:
        result = result.group(1)[3:]
    
        # search for all strings enclosed in {} that make up each entry
        entries = re.findall(r"{(.*?)}", result)

        mvs = []
        clubs = []
        ages = []
        dates_of_mv = []
        columns = [mvs, clubs, ages, dates_of_mv]
        column_names = ['Market Value', 'Club', 'Age', 'Date']

        # iterate through all entries
        for entry in entries:
            mv = re.search(r"y\':(.*?),", entry).group(1)
            club = re.search(r"verein\':(.*?),", entry).group(1)
            age = re.search(r"age\':(.*?),", entry).group(1)
            date_of_mv = re.search(r"datum_mw\':(.*?),", entry).group(1)

            for val, list_ in zip([mv, club, age, date_of_mv], columns):
                list_.append(val)

        df = pd.DataFrame(columns, index=column_names).T.set_index('Date')
        df.index = pd.to_datetime(df.index, dayfirst=True)

        df['Club'] = df.Club.str.replace("'", "")

        df['Age'] = df['Age'].astype('int')
        df['Market Value'] = df['Market Value'].astype('int')

        return df


def get_transfer_history(player_id,
                         player_string=None):
    """
    Parameters:
    ___________
    
    int player_id: transfermarkt player specific id
    str player_string=None:  transfermarkt player string
    
    
    Returns:
    –––––––
    a DataFrame with Columns ['Season', 'Date', 'Old_Club', 'New_Club', 'MV', 'Transferfee',
                               'old_club_string', 'old_club_id', 'new_club_sring', 'new_club_id'] 
    
    
    """
    if player_string is None:
        player_string = 'player'
    
    transfer_history_url = f'https://www.transfermarkt.de/{player_string}/transfers/spieler/{player_id}'

    _, soup = get_page_tree_and_soup(transfer_history_url)

    try:
        tbody = soup.find_all('tbody')[0]
        thead = soup.find_all('thead')[0]

        table_columns = get_table_columns(thead)

        table = get_table_from_tbody(tbody, rid_empty=False, strip=True).dropna()

        table.columns = ['Season', 'Date', '', '', '', 'Old_Club', '', '', '', 'New_Club', 'MV', 'Transferfee', '']

        table = table.drop('', axis=1)

        def clean_market_vals(series):
            series = (series
                        .str.replace('ablösefrei', '0')
                        .str.replace('-', '0'))
            mvdf = (series.str.split(' ', expand = True))
            mvdf[0] = mvdf[0].str.replace('-', '0')
            if mvdf.shape[1] > 1:
                mvdf[1] = mvdf[1].str.replace('Tsd.', '1000').replace('Mio.', '1000000').astype('float')
                series = (mvdf[0].str.replace(',', '.').astype('float') * mvdf[1].fillna(0)).astype('int')
            return series

        table['MV'] = clean_market_vals(table['MV'])
        
        table['Leihende'] = table['Transferfee'].str.contains('Leih-Ende')
        table['Leihe'] = table['Transferfee'].astype('str').str.contains('Leihe')|table['Transferfee'].astype('str').str.contains('Leihgeb')
        table['fee_unknown'] = table['Transferfee'].str.contains('?', regex=False)
        
        table['Transferfee'] = (table['Transferfee'].str.replace('Leih-Ende', '0')
                                                    .str.replace('Leih0Ende', '0')
                                                    .str.replace('Leihe', '0')
                                                    .str.replace('Leihgebühr:', '0')
                                                    .str.replace('?', '0'))
        table['Transferfee'] = table['Transferfee'].str.replace('Ã¶', 'ö').str.replace('â‚¬', '€')
        table['Transferfee'] = clean_market_vals(table['Transferfee'])

        table = table.reset_index(drop=True)

        #tooltips = tbody.find_all('a', {'class': 'vereinprofil_tooltip'})
        #if karriereende_tooltips != []:
        #    tooltips = [tooltips[::3][0]] + karriereende_tooltips + tooltips[::3][1:]
        #else:
        #    tooltips = tooltips[::3]

        tooltips = []
        for a in tbody.find_all('a'):
            try:
                if (a['title'] in ["Karriereende", "KarriereendeKarriereende",
                                   "Vereinslos", "VereinslosVereinslos",
                                   "Unbekannt", "UnbekanntUnbekannt",
                                   "pausiertpausiert", "pausiert"]):
                    tooltips.append(a)
            except:
                pass
            try:
                if (a['class'] == ['vereinprofil_tooltip']):
                    tooltips.append(a)
            except:
                pass
        tooltips = tooltips[::3]    

        club_hrefs = [a['href'] for a in tooltips]

        clubs_df = pd.DataFrame(
            np.concatenate([np.take(href.split('/'), [1, 4]) for href in club_hrefs]).reshape(-1, 4),
            columns=['old_club_string', 'old_club_id', 'new_club_string', 'new_club_id'])

        table = pd.concat([table, clubs_df], axis=1)

        return table
    
    except:
        return pd.DataFrame()


def get_spieler_verletzungshistorie(player_id,
                                    player_string=None):

    if player_string is None:
        player_string = 'player'
    
    url = f'https://www.transfermarkt.de/{player_string}/verletzungen/spieler/{player_id}'

    pageTree, soup = get_page_tree_and_soup(url)

    theads = soup.find_all('thead')
    tbodies = soup.find_all('tbody')

    try:
        columns = get_table_columns(theads[0])
        table = get_table_from_tbody(tbodies[0], columns=columns)

        table['von'] = pd.to_datetime(table['von'], dayfirst=True)
        table['bis'] = pd.to_datetime(table['bis'], dayfirst=True)
        table['Tage'] = table['Tage'].str.replace(' Tage', '').astype('int')
        table['Verpasste Spiele'] = table['Verpasste Spiele'].str.replace('-', '0').astype('int')

        return table
    
    except:
        return pd.DataFrame()


def get_league_table(league_abbrev,
                     season,
                     headers={'User-Agent': 
               'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
                   ):
    '''
    
    scrapes the table from Transfermarkt for one league one season
    
    Parameters:
    -----------
    league_abbrev: the transfermarkt specific league abbreviation
                  ie: L1 for the Bundesliga, L2 for 2. Bundesliga, GB_ for England, ES_ for Spain etc
    season: the year the season begins, ie: 2020
    
    Returns:
    -----------
    table: DataFrame
    
    '''

    table_url = f'https://www.transfermarkt.de/superligaen/tabelle/wettbewerb/{league_abbrev}/saison_id/{season}'
    pageTree = requests.get(table_url, headers=headers)

    tables = pd.read_html(pageTree.content)
    table = tables[3].drop('Verein', axis=1).rename(columns={'#': 'Rank',
                                                             'Verein.1': 'Club',
                                                             'SpieleS': 'Played',
                                                             'G': 'Wins',
                                                             'U': 'Draw',
                                                             'V': 'Losses',
                                                             'ToreT': 'Goals',
                                                             '+/-': 'GD',
                                                             'Pkt.P': 'Pts'})
    
    soup = BeautifulSoup(pageTree.content, 'html.parser')
    tds = soup.find_all('td', {'class': "zentriert no-border-rechts"})

    club_names = [item for td in tds for i, item in enumerate(td.find_next('a', {'class': "vereinprofil_tooltip"})['href'].split('/')) if i in [1]]
    club_ids = [item for td in tds for i, item in enumerate(td.find_next('a', {'class': "vereinprofil_tooltip"})['href'].split('/')) if i in [4]]

    table['GF'] = table['Goals'].apply(lambda x: x.split(':')[0]).astype('int')
    table['GA'] = table['Goals'].apply(lambda x: x.split(':')[1]).astype('int')
    
    table['club_name'] = club_names[:len(table)]
    table['club_id'] = club_ids[:len(table)]

    return table

def get_gameweek_table(league_abbrev,
                       season,
                       gameweek,
                       headers={'User-Agent': 
               'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
                   ):
    '''
    
    scrapes the table from Transfermarkt for one gameweek of one season for a given league
    
    Parameters:
    -----------
    league_abbrev: the transfermarkt specific league abbreviation
                  ie: L1 for the Bundesliga, L2 for 2. Bundesliga, GB_ for England, ES_ for Spain etc
    season: the year the season begins, ie: 2020
    gameweek: gameweek to scrape the table for
    headers: for response.get
    
    Returns:
    -----------
    table: DataFrame
    
    '''

    table_url = f'https://www.transfermarkt.de/league-name/spieltagtabelle/wettbewerb/{league_abbrev}?saison_id={season}&spieltag={gameweek}'
    pageTree = requests.get(table_url, headers=HEADERS)

    tables = pd.read_html(pageTree.content)

    table = tables[4].drop('Verein', axis=1).rename(columns={'#': 'Rank',
                                                             'Verein.1': 'Club',
                                                             'Unnamed: 3': 'Played',
                                                             'G': 'Wins',
                                                             'U': 'Draw',
                                                             'V': 'Losses',
                                                             'Tore': 'Goals',
                                                             '+/-': 'GD',
                                                             'Pkt.': 'Pts'})

    soup = BeautifulSoup(pageTree.content, 'html.parser')
    tds = soup.find_all('td', {'class': "zentriert no-border-rechts"})

    table['GF'] = table['Goals'].apply(lambda x: x.split(':')[0]).astype('int')
    table['GA'] = table['Goals'].apply(lambda x: x.split(':')[1]).astype('int')

    club_names = [item for td in tds for i, item in enumerate(td.find_next('a', {'class': "vereinprofil_tooltip"})['href'].split('/')) if i in [1]]
    club_ids = [item for td in tds for i, item in enumerate(td.find_next('a', {'class': "vereinprofil_tooltip"})['href'].split('/')) if i in [4]]

    table['club_name'] = club_names[:len(table)]
    table['club_id'] = club_ids[:len(table)]

    return table

def scrape_league_games(url,
                        year=None):
    """
    scrapes all the league games from the transfermarket page, ie:
    https://www.transfermarkt.de/1-bundesliga/gesamtspielplan/wettbewerb/L1
    
    Parameters:
    -----------
    url: the base url for the league games
    year=None: year
    
    Returns:
    --------
    
    DataFrame:
    columns: 'Home', 'Result', 'Away', 'Home_Rank', 'Away_Rank', 'Date', 'Spieltag',
             'Report_Link', 'Home_Link', 'Away_Link', 'Home_Goals', 'Away_Goals'
    
    """
    if year is not None:
        url = "".join([url, f'?saison_id={year}'])
    
    pageTree, soup = get_page_tree_and_soup(url)
    tbodies = soup.find_all('tbody')

    spieltage = [el.text for el in soup.find_all('div', {'class': 'table-header'})]

    gameday_dfs = []
    team_hrefs = []
    reports = []

    for spieltag, table in zip(spieltage, tbodies[1:]):
        spieltag_team_hrefs = [el['href'] for el in table.find_all('a', {'class': 'vereinprofil_tooltip'})[::2]]
        #report = table.find('a', {'class': "ergebnis-link"})['href']
        n_spiele = len(spieltag_team_hrefs[::2])
        reports = np.full(n_spiele, np.nan).astype('<U64')
        try:
            reports[:n_spiele] = [el['href'] for el in table.find_all('a', {'class': "ergebnis-link"})]
        except:
            pass

        team_hrefs.append(spieltag_team_hrefs)

        table = get_table_from_tbody(table)

        idx = table.dropna().index
        table = table.loc[idx]

        days = []
        for idx, row in (table[0]
                            .replace(r'\n',' ', regex=True)
                            .replace(r'\t',' ', regex=True)
                            .str.strip()
                            .str.split(' ', expand=True)
                        ).iterrows():
            current_day = row.loc[row != ''].iloc[1]    
            days.append(current_day)

        dates = pd.to_datetime(pd.Series(days).fillna(method='ffill'))

        table = table.loc[:, 2:].rename(columns = {2: 'Home', 3: 'Result', 4: 'Away'})
        try:
            table['Away_Rank'] = table['Away'].str.split('(', expand=True).iloc[:, 1].str.replace(')', '')
            table['Home_Rank'] = table['Home'].str.split(')', expand=True).iloc[:, 0].str.replace('(', '')
            table['Away'] = table['Away'].str.split('(', expand=True).iloc[:, 0].str.strip()
            table['Home'] = table['Home'].str.split(')', expand=True).iloc[:, 1].str.strip()
        except:
            pass
        table = table.reset_index(drop=True)
        table['Date'] = dates
        table['Spieltag'] = spieltag
        table['Report_Link'] = reports
        gameday_dfs.append(table)

    gameday_df = pd.concat(gameday_dfs)

    home_links, away_links = np.concatenate(team_hrefs).reshape(-1, 2).T

    gameday_df['Home_Link'] = home_links
    gameday_df['Away_Link'] = away_links
    
    gameday_df['Home_Goals'], gameday_df['Away_Goals'] = gameday_df['Result'].str.split(':', expand=True)
    
    return gameday_df

def scrape_cup_games(url,
                     year=None):
    """
    scrapes all the cup games from the transfermarket page, ie:
    https://www.transfermarkt.de/fa-cup/startseite/pokalwettbewerb/FAC for the FA Cup
    
    Parameters:
    -----------
    url: the base url for the cup
    year=None: year
    
    Returns:
    --------
    
    DataFrame:
    columns: Round, Date, Home, Away, Result, Home_Link, Away_Link, Report_Link,
             Period (ie overtime or penalty shootout), Home_Goals, Away_Goals
    
    """
    if year is not None:
        url = "".join([url, f'?saison_id={year}'])
    pageTree, soup = get_page_tree_and_soup(url)

    table = soup.find_all('tbody')[1]

    entries = table.find_all('tr', {'class': ['rundenzeile', 'begegnungZeile']})

    teams = []
    team_names = []
    rounds = []
    results = []
    spielberichte = []
    dates = []

    for entry in entries:
        if entry['class'] == ['rundenzeile']:
            current_round = entries[0].find_next('td', {'class': 'zeit ac'}).text
        elif entry['class'] == ['begegnungZeile']:
            rounds.append(current_round)
            team_entries = entry.find_all('a', {'class': "vereinprofil_tooltip"})
            team_name_entries = entry.find_all('img')
            for team_entry in team_entries[::2]:
                teams.append(team_entry['href'])
            for team_name_entry in team_name_entries:
                team_names.append(team_name_entry['alt'])
            try:
                if "matchresult finished" in str(entry):
                    result = entry.find_next('span', {'class': "matchresult finished"}).text
                    results.append(result)
                else:
                    results.append(np.nan)
            except:
                pass
            try:
                if 'Spielbericht' in str(entry):
                    spielbericht = entry.find_next('a', {'title': 'Spielbericht'})['href']
                    spielberichte.append(spielbericht)
                else:
                    spielberichte.append(np.nan)
            except:
                pass

            if (entry.find_next('a').text.strip() != '')&(('Datum' in entry.find_next('a')['href'])
                                                          |('datum' in entry.find_next('a')['href'])):
                current_date = entry.find_next('a').text.strip()

            dates.append(current_date)

    df = pd.concat([pd.Series(rounds),
                pd.Series(dates),
                pd.DataFrame(np.array(team_names).reshape(-1, 2)),
                pd.Series(results),
                pd.DataFrame(np.array(teams).reshape(-1, 2)),
                pd.Series(spielberichte)], axis=1)
    df.columns = ['Round', 'Date', 'Home', 'Away', 'Result', 'Home_Link', 'Away_Link', 'Report_Link']

    df['Period'] = df['Result'].str.split(' ', expand=True)[1]
    df['Result'] = df['Result'].str.split(' ', expand=True)[0]
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)

    df['Home_Goals'] = df['Result'].str.split(':', expand=True)[0].astype('float')
    df['Away_Goals'] = df['Result'].str.split(':', expand=True)[1].astype('float')

    return df


def get_player_leistungsdaten(player_id,
                              player_string=None):
    
    def get_detailed_table(soup):    
        tbodies = soup.find_all('tbody')
        
        try:
            tbody = tbodies[1]

            table = get_table_from_tbody(tbody, rid_empty=False)
            columns = ['Season', '', 'Competition', '', 'In Squad', 'Games Played', 'PPG', 'Goals', 'Assists', 'Own Goals', 
                       'Subbed In', 'Subbed Out', 'Yellow', '2nd Yellow', 'Red', 'Penalty Goals', 'Minutes per Goal', 'Minutes']
            table.columns = columns
            table = table.drop('', axis=1)

            for col in table.columns[2:]:
                table[col] = pd.to_numeric(table[col]
                                .str.replace('.', '')
                                .str.replace('-', '0')
                                .str.replace("'", '')
                                .str.replace(',', '.'))

            competition_strings = [a['href'].split('/')[-3] for a in tbody.find_all('a')][::3]    
            table.insert(2, 'competition_string', competition_strings)

            club_names = [img['alt'] for img in tbody.find_all('img')][1::2]
            table.insert(3, 'Club', club_names)

            club_strings = [a['href'].split('/')[-3] for a in tbody.find_all('a')][1::3]
            table.insert(4, 'club_string', club_strings)
            
        except:
            # when there is no performance history return empty DataFrame
            table = pd.DataFrame(columns = ['Season', 'Competition', 'competition_string', 'Club', 'club_string',
                                            'In Squad', 'Games Played', 'PPG', 'Goals', 'Assists', 'Own Goals', 
                                            'Subbed In', 'Subbed Out', 'Yellow', '2nd Yellow', 'Red', 'Penalty Goals',
                                            'Minutes per Goal', 'Minutes'])

        return table

    if player_string is None:
        player_string = 'player'
        
    url = f'https://www.transfermarkt.de/{player_string}/leistungsdatendetails/spieler/{player_id}/saison//verein/0/liga/0/wettbewerb//pos/0/trainer_id/0/plus/1'
    _, soup = get_page_tree_and_soup(url)
    
    table = get_detailed_table(soup)
    table['player_id'] = player_id
    
    return table


def get_national_team_history(player_id,
                              player_string=None,
                              domain='de'):
    """
    Get the history of national team games played for a player.

    Parameters:
    -----------
    player_id: the transfermarkt player specific player_id
    domain = 'de': domain on which to scrape. Currently supports ['de', 'com', 'co.uk']
    
    Returns:
    --------
    
    DataFrame with columns:
            ['Date', 'Ground', 'Team', 'Opponent', 'Result', 'game_outcome',
             'competition', 'game_id', 'team_id', 'team_year', 'opponent_id',
             'opponent_year', 'Position', 'Goals', 'Assists', 'Yellow', '2ndYellow',
             'Red', 'Minutes']

    """
    assert domain in ['de', 'com', 'co.uk'], 'Choose a domain of ["de", "com", "co.uk"]'
    
    if player_string is None:
        player_string = 'player-name'
    url = f'https://www.transfermarkt.{domain}/{player_string}/nationalmannschaft/spieler/{player_id}'

    pageTree, soup = get_page_tree_and_soup(url)

    if domain == 'de':
        data_placeholder = 'Nationalteam wählen'
    elif (domain == 'com')|(domain == 'co.uk'):
        data_placeholder = 'Filter by national team'

    year_select = soup.find_all('select', {'class': 'chzn-select',
                                           'data-placeholder': data_placeholder})

    if year_select == []:
        return pd.DataFrame()

    else:
        year_select = year_select[0]
        
        selectable_teams = [[opt['value'], opt.text] for opt in year_select.find_all('option')]

        soups = [soup]
        for team_string, team_name in selectable_teams[1:]:
            url_comp = f'https://www.transfermarkt.de/{player_string}/nationalmannschaft/spieler/{player_id}/plus/0/verein_id/{team_string}'
            _, soup_ = get_page_tree_and_soup(url_comp)
            soups.append(soup_)

        dfs = []

        for soup, (team_string, team_name) in zip(soups[-1::-1], selectable_teams[-1::-1]):
            tbodies = soup.find_all('tbody')

            debut_table = tbodies[0]
            game_table = tbodies[-1]

            table = get_table_from_tbody(game_table, rid_empty=False)
            if len(table) > 1:

                table_rows = game_table.find_all('tr')

                game_report_ids = []
                team_ids_and_year = []
                competitions = []
                game_outcomes = []
                team_names = []

                for tr in table_rows:
                    try:
                        game_report_ids.append(tr.find('a', {'class': "ergebnis-link"})['id'])
                        team_info = tr.find_all('a', {'class': 'vereinprofil_tooltip'})[:2]
                        for ti in team_info:
                            team_ids_and_year.append([ti['id'], ti['href'].split('/')[-1]])
                        competitions.append(None)

                        outcome = tr.select('span')[-1]['class']
                        if len(outcome) == 0:
                            outcome = 'D'
                        elif outcome[0] == 'redtext':
                            outcome = 'L'
                        elif outcome[0] == 'greentext':
                            outcome = 'W'           
                        game_outcomes.append(outcome)
                        team_names.append(tr.find_next('img')['alt'])

                    except:

                        try:
                            game_report_ids.append(None)
                            team_ids_and_year.append([None, None])
                            team_ids_and_year.append([None, None])
                            game_outcomes.append(None)
                            team_names.append(None)
                            try:
                                competitions.append(tr.find_all('a')[-1].text.strip())
                            except:
                                competitions.append(None)
                        except:
                            pass

                table = pd.concat([table, pd.DataFrame(np.array(team_ids_and_year).reshape(-1, 4),
                                        columns=['team_id', 'team_year', 'opponent_id', 'opponent_year'])], axis=1)
                table['game_id'] = game_report_ids
                table['competition'] = competitions
                table['competition'] = table['competition'].fillna(method='ffill')
                table['game_outcome'] = game_outcomes
                table['Team'] = team_names

                table.columns = np.concatenate([['', '', 'Date', 'Ground', '', '', 'Opponent', 'Result',
                                                 'Position', 'Goals', 'Assists', 'Yellow', '2ndYellow', 'Red', 'Minutes'],
                                                table.columns[15:]])

                table = table.loc[pd.notna(table['Opponent'])].iloc[:, 2:]

                table['Goals'] = pd.to_numeric(table['Goals'])
                table['Assists'] = pd.to_numeric(table['Assists'])
                table['Yellow'] = pd.to_numeric(table['Yellow'].str.replace("'", ''))
                table['2ndYellow'] = pd.to_numeric(table['2ndYellow'].str.replace("'", ''))
                table['Red'] = pd.to_numeric(table['Red'].str.replace("'", ''))
                table['Minutes'] = pd.to_numeric(table['Minutes'].str.replace("'", ''))

                table = table.drop('', axis=1)

                table = table[['Date', 'Ground', 'Team', 'Opponent', 'Result', 'game_outcome', 'competition', 'game_id',
                               'team_id', 'team_year', 'opponent_id', 'opponent_year',
                                 'Position', 'Goals', 'Assists', 'Yellow', '2ndYellow', 'Red', 'Minutes'
                                 ]]

                dfs.append(table)

        if len(dfs) == 0:
            return pd.DataFrame()
        
        else:
            df = pd.concat(dfs)[-1::-1]
            df.loc[(df['Position'] == 'ohne Einsatz im Kader')|
                   (df['Position'] == 'on the bench'), 'Minutes'] = 0
            df = df.reset_index(drop=True)

            if domain == 'de':
                df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
            elif domain in ['com', 'co.uk']:
                df['Date'] = pd.to_datetime(df['Date'], dayfirst=False)

        return df


def scrape_gameinfo_by_pos(player_id,
                            player_name='player-name',
                            domain='de',
                            year='curr',
                            detailed=False):
    """
    scrape the amount of games played by position for a player_id
    
    Parameters:
    ----------
    player_id: a transfermarkt specific player id
    player_name: optional. the player specific transfermarkt player name string
    domain = 'de'
    year = 'curr', 'curr' for current year or 'all' for all years
    detailed = False; if True get minutes played and additional information by position for all years

    Returns:
    ----------
    a DataFrame
    
    """
    
    def get_games_by_pos(soup):
        tbodies = soup.find_all('tbody')
        table = get_table_from_tbody(tbodies[-4])
        table.columns = ['Position', 'Games Played', 'Goals', 'Assists']

        for col in table.columns[1:]:
            table[col] = table[col].str.replace('-', '0')
        table.iloc[:, 1:] = table.iloc[:, 1:].astype('int')
        table = table.set_index('Position')
        return table

    
    def get_detailed_url(player_id,
                player_name = 'player-name',
                domain = 'de',
                saison = '',
                verein = '',
                liga = '',
                wettbewerb = '',
                pos = '11',
                trainer_id = ''):

        url = f'https://www.transfermarkt.{domain}/{player_name}/leistungsdatendetails/spieler/{player_id}/plus/1?saison={saison}&verein={verein}&liga={liga}&wettbewerb={wettbewerb}&pos={pos}&trainer_id={trainer_id}'
        return url

    def get_detailed_table(soup):    

        tbodies = soup.find_all('tbody')

        table = get_table_from_tbody(tbodies[1], rid_empty=False)
        columns = ['Season', '', 'Competition', '', 'In Squad', 'Games Played', 'PPG', 'Goals', 'Assists', 'Own Goals', 
                   'Subbed In', 'Subbed Out', 'Yellow', '2nd Yellow', 'Red', 'Penalty Goals', 'Minutes per Goal', 'Minutes']
        table.columns = columns
        table = table.drop('', axis=1)

        for col in table.columns[2:]:
            table[col] = pd.to_numeric(table[col]
                            .str.replace('.', '')
                            .str.replace('-', '0')
                            .str.replace("'", '')
                            .str.replace(',', '.'))
        return table
    
    assert year in ['curr', 'all'], 'Choose "curr" or "all" for Parameter year'
    assert isinstance(detailed, bool), 'Parameter detailed must be True of False'
    
    if domain == 'de':
        data_placeholder = 'Position auswählen'
    elif domain in ['com', 'co.uk']:
        data_placeholder = 'Filter by position'
    
    if detailed == False:
        if year == 'curr':
            url_current_season = f'https://www.transfermarkt.{domain}/{player_name}/leistungsdaten/spieler/{player_id}'
            _, soup_curr = get_page_tree_and_soup(url_current_season)
            table = get_games_by_pos(soup_curr)
            
        elif year == 'all':    
            url_all_seasons = f'https://www.transfermarkt.{domain}/{player_name}/leistungsdatendetails/spieler/{player_id}'
            _, soup = get_page_tree_and_soup(url_all_seasons)
            table = get_games_by_pos(soup)
            
    elif detailed == True:
        url_detailed = f'https://www.transfermarkt.{domain}/{player_name}/leistungsdatendetails/spieler/{player_id}/saison//verein/0/liga/0/wettbewerb//pos/0/trainer_id/0/plus/1'
        _, soup_detailed = get_page_tree_and_soup(url_detailed)
        
        pos_select = soup_detailed.find('select', {'data-placeholder': data_placeholder})
        options = [[option['value'], option.text] for option in pos_select.find_all('option')[1:]]
        
        tables = []
        for option in options:
            url = get_detailed_url(player_id=player_id,
                                   domain=domain,
                                   pos=option[0])
            print(url)
            _, soup_detailed_pos = get_page_tree_and_soup(url)
            table = get_detailed_table(soup_detailed_pos)
            table['Position'] = option[1]
            tables.append(table)

        table = pd.concat(tables)
        
    table['player_id'] = player_id
    
    return table

def get_team_schedule(team_id,
                       headers = {'User-Agent': 
               'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
                   ):
    '''
    
    scrapes a teams schedule for the current season from Transfermarkt
    
    Parameters:
    -----------
    team_id: the transfermarkt team specific `team_id`
    headers: for response.get
    
    Returns:
    -----------
    table: DataFrame
    
    '''

    url = f'https://www.transfermarkt.de/teamname/spielplandatum/verein/{team_id}'
    
    pageTree = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(pageTree.content, 'html.parser')

    dfs = pd.read_html(pageTree.content)

    df = dfs[1]
    df['Gegner'] = df['Gegner'].fillna(method='ffill')
    df = df.rename(columns={'Gegner': 'Competition'})
    df = df.rename(columns={'Gegner.1': 'Opponent'})

    # clean out rows where all columns have the same value
    df = df.loc[~(df == df.shift(1, axis=1, fill_value=df.iloc[:, 0])).all(axis=1)]

    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    df['Zuschauer'] = df['Zuschauer'].str.replace('x', '0').str.replace('.', '').astype('float')

    df['Day'] = df['Datum'].apply(lambda x: x.split(' ', maxsplit=1)[0])
    df['Datum'] = df['Datum'].apply(lambda x: x.split(' ', maxsplit=1)[1])

    df['Datum'] = pd.to_datetime(df['Datum'])

    df = df.rename(columns={'Spieltag': 'Gameweek',
                            'Datum':'Date',
                            'Uhrzeit': 'Time',
                            'Ort': 'Ground',
                            'Rang': 'Rank',
                            'Spielsystem': 'System',
                            'Zuschauer': 'Attendance',
                            'Ergebnis': 'Result'})


    df['Gameweek'] = df['Gameweek'].str.replace('Runde', 'Round')

    tds = soup.find_all('td', {'class': "zentriert no-border-rechts tiny_wappen_zelle"})

    club_names = [item for td in tds for i, item in enumerate(td.find_next('a', {'class': "vereinprofil_tooltip"})['href'].split('/')) if i in [1]]
    club_ids = [item for td in tds for i, item in enumerate(td.find_next('a', {'class': "vereinprofil_tooltip"})['href'].split('/')) if i in [4]]

    df['club_string'] = club_names
    df['club_id'] = club_ids
    
    return df