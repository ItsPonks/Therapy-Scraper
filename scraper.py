from aiohttp import ClientSession
from asyncio import gather, Lock, run, sleep
from bs4 import BeautifulSoup
from csv import writer
from random import gauss
from re import compile, IGNORECASE
from urllib.parse import urljoin, urlsplit

async def get(session, url, **kwargs):
    # Removes passed in params if the URL already has a query.
    if urlsplit(url).query:
        kwargs['params'] = None

    while True:
        response = await session.get(url, **kwargs)
        print('Status:', response.status, '|', response.url)
        if response.ok:
            return BeautifulSoup(await response.text(), 'html5lib')
        await sleep(60)

async def scrape(session, function, base, urls, tags, next={'string': compile(r'Next', IGNORECASE)}, **kwargs):
    rows = set()
    soup = await get(session, base)
    for tag in soup(**urls):
        for url in tag(href=True):
            while url:
                await sleep(gauss(gauss(5, 5/3), gauss(5/3, 5/9)))
                base = urljoin(base + '/', url.get('href'))
                soup = await get(session, base, **kwargs)
                url = soup.find(href=True, **next)
                rows.update(filter(None, map(function, soup(**tags))))
    
    async with lock:
        with open('data.csv', 'a', newline='') as f:
            out = writer(f)
            out.writerow([function.__name__])
            out.writerows(rows)

def GoodTherapy(tag):
    profile = tag.a.get('href')
    name = tag.h2.string

    tag = tag.find_next_sibling(class_='col s12 m12 l4 xl4 therapist_contact_list')
    address = next(tag.p.stripped_strings)
    number = tag.a.get('href')
    return address, name, number, profile

def Theravive(tag):
    name = tag.h4.string
    address = next(tag.p.stripped_strings)
    profile = tag.find(class_='stopClick btn btn-default btn-orange').get('href')
    if number := tag.find(class_='tclass stopClick btn btn-default btn-orange visible-xs'):
        number = number.get('href')
    return address, name, number, profile

def NetworkTherapy(tag):
    profile = tag.a.get('href')
    name = tag.a.string
    address = ' '.join(content.string for content in tag.contents[3:8:2])
    return address, name, None, profile

def PsychologyToday(tag):
    name = tag.get('data-prof-name')
    number = tag.get('data-phone')
    profile = tag.get('data-profile-url')
    address = ' '.join(tag.string for tag in tag.find_all('span')[-3:])
    return address, name, number, profile

def AllAboutCounseling(tag):
    if tag.a:
        profile = tag.a.get('href')
        name = tag.string
        address = ' '.join(tag.find_next_sibling('address').stripped_strings)
        return address, name, None, profile

async def main():
    async with ClientSession() as session:
        await gather(
            scrape(session, GoodTherapy, f'https://goodtherapy.org/therapists/{id}', {'class_': 'list_rows_line'}, {'class_': 'col s8 m9 l5 xl6 therapist_middle_section'}, {'rel': 'next'}),
            scrape(session, Theravive, f'https://theravive.com/region/{id}/counseling', {'class_': 'col-md-6'}, {'class_': 'profile-info'}),
            scrape(session, NetworkTherapy, f'https://networktherapy.com/directory/therapist_index.asp?state={id}', {'summary': (f'All {name} cities with therapist listings.', f'{name} counties with therapist listings.')}, {'name': 'tr', 'bgcolor': ('#FFFFFF', '#D2D7EC')}, params='c4=100&sr=c'),
            scrape(session, PsychologyToday, f'https://psychologytoday.com/therapists/{name}', {'class_': 'top-nav hidden-sm-down'}, {'class_': 'result-row normal-result row'}, {'class_': 'btn btn-default btn-next'}),
            scrape(session, AllAboutCounseling, f'https://allaboutcounseling.com/local/{name}', {'name': 'ol'}, {'class_': 'name'})
        )

lock = Lock()
id = 'MD'
name = 'Maryland'

run(main())