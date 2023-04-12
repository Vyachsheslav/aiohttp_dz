import sqlite3
import aiohttp
import asyncio


async def fetch_person(session, url):
    async with session.get(url) as response:
        return await response.json()

async def fetch_all_people():
    async with aiohttp.ClientSession() as session:
        response = await session.get('https://swapi.dev/api/people/')
        data = await response.json()
        tasks = []
        while 'next' in data and data['next'] is not None:
            for character in data['results']:
                tasks.append(fetch_person(session, character['url']))
            response = await session.get(data['next'])
            data = await response.json()
        for character in data['results']:
            tasks.append(fetch_person(session, character['url']))
        characters = await asyncio.gather(*tasks)
        return characters

async def load_data_to_db():
    conn = sqlite3.connect('star_wars.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS characters 
                      (id INTEGER PRIMARY KEY, birth_year TEXT, eye_color TEXT, 
                       films TEXT, gender TEXT, hair_color TEXT, height TEXT, 
                       homeworld TEXT, mass TEXT, name TEXT, skin_color TEXT, 
                       species TEXT, starships TEXT, vehicles TEXT)''')
    characters = await fetch_all_people()
    async with aiohttp.ClientSession() as session:
        for character in characters:
            films = []
            species = []
            starships = []
            vehicles = []
            for film in character['films']:
                async with session.get(film) as film_response:
                    film_data = await film_response.json()
                    films.append(film_data['title'])
            for specie in character['species']:
                async with session.get(specie) as specie_response:
                    specie_data = await specie_response.json()
                    species.append(specie_data['name'])
            for starship in character['starships']:
                async with session.get(starship) as starship_response:
                    starship_data = await starship_response.json()
                    starships.append(starship_data['name'])
            for vehicle in character['vehicles']:
                async with session.get(vehicle) as vehicle_response:
                    vehicle_data = await vehicle_response.json()
                    vehicles.append(vehicle_data['name'])
            homeworld_url = character['homeworld']
            async with session.get(homeworld_url) as homeworld_response:
                homeworld_data = await homeworld_response.json()
                homeworld = homeworld_data['name']
            cursor.execute("INSERT INTO characters (id, birth_year, eye_color, films, gender, hair_color, height, "
                           "homeworld, mass, name, skin_color, species, starships, vehicles) VALUES (?, ?, ?, ?, ?, ?, ?, "
                           "?, ?, ?, ?, ?, ?, ?)",
                           (character['url'].split('/')[-2], character['birth_year'], character['eye_color'],
                            ', '.join(films), character['gender'], character['hair_color'], character['height'],
                            homeworld, character['mass'], character['name'], character['skin_color'],
                            ', '.join(species), ', '.join(starships), ', '.join(vehicles)))
    conn.commit()
    conn.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(load_data_to_db())
