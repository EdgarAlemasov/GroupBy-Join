import psycopg2
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://postgres:ImAlive72ae@localhost:5432/SelectRequests')

connection = engine.connect()

connection.execute("""
    CREATE TABLE IF NOT EXISTS genre (
        id SERIAL PRIMARY KEY,
        genre_name VARCHAR(40) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS artist (
        id SERIAL PRIMARY KEY,
        artist_name VARCHAR(40) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS genre_artist (
        id SERIAL PRIMARY KEY,
        artist_id INTEGER REFERENCES artist(id),
        genre_id INTEGER REFERENCES genre(id)
    );

    CREATE TABLE IF NOT EXISTS album (
        id SERIAL PRIMARY KEY,
        album_name VARCHAR(40) NOT NULL,
        release_date INTEGER
    );

    CREATE TABLE IF NOT EXISTS album_artist (
        id SERIAL PRIMARY KEY,
        album_id INTEGER REFERENCES album(id),
        artist_id INTEGER REFERENCES artist(id)
    );

    CREATE TABLE IF NOT EXISTS song (
        id SERIAL PRIMARY KEY,
        song_name VARCHAR(40) NOT NULL,
        duration INTEGER,
        album_id INTEGER REFERENCES album(id)
    );

    CREATE TABLE IF NOT EXISTS collection (
        id SERIAL PRIMARY KEY,
        collection_name VARCHAR(40) NOT NULL,
        release_year INTEGER
    );

    CREATE TABLE IF NOT EXISTS song_collection (
        id SERIAL PRIMARY KEY,
        song_id INTEGER REFERENCES song(id),
        collection_id INTEGER REFERENCES collection(id)
    );
""")

connection.execute("""
    INSERT INTO genre(id, genre_name)
    VALUES
        (1, 'Rock'),
        (2, 'Pop'),
        (3, 'Rap'),
        (4, 'Jazz'),
        (5, 'Folk'),
        (6, 'Trance');

    INSERT INTO artist
    VALUES
        (1, 'Nickelback'),
        (2, 'JayZ'),
        (3, 'Eminem'),
        (4, 'Ed Sheeran'),
        (5, 'Linkin Park'),
        (6, 'Armin Van Buuren'),
        (7, 'Louis Armstrong'),
        (8, 'Bob Dylan'),
        (9, 'Elton John'),
        (10, 'Markus Schulz');

    INSERT INTO genre_artist
    VALUES
        (1, 1, 1),
        (2, 2, 3),
        (3, 3, 3),
        (4, 4, 2),
        (5, 5, 1),
        (6, 6, 6),
        (7, 7, 4),
        (8, 8, 5),
        (9, 9, 2),
        (10, 10, 6);

    INSERT INTO album
    VALUES
        (1, 'Dark Horse', 2008),
        (2, 'American Gangster', 2007),
        (3, 'Recovery', 2010),
        (4, 'Divide', 2018),
        (5, 'A Thousand Suns', 2010),
        (6, 'Shivers', 2005),
        (7, 'Hello Dolly', 1964),
        (8, 'Desire', 1976),
        (9, 'The Lockdown', 2021),
        (10, 'Escape', 2020);

    INSERT INTO album_artist
    VALUES
        (1, 1, 1),
        (2, 2, 2),
        (3, 3, 3),
        (4, 4, 4),
        (5, 5, 5),
        (6, 6, 6),
        (7, 7, 7),
        (8, 8, 8),
        (9, 9, 9),
        (10, 10, 10);

    INSERT INTO song
    VALUES
        (1, 'Gotta Be Somebody', 253, 1),
        (2, 'Pray', 264, 2),
        (3, 'NotAfraid', 257, 3),
        (4, 'WTP', 240, 3),
        (5, 'Perfect', 264, 4),
        (6, 'TheRequiem', 121, 5),
        (7, 'Burning In The Skies', 253, 5),
        (8, 'Shivers', 194, 6),
        (9, 'Black And Blue', 208, 7),
        (10, 'My Indiana', 264, 7),
        (11, 'My Hurricane', 514, 8),
        (12, 'Cold Heart', 203, 9),
        (13, 'Always Love You', 257, 9),
        (14, 'Learn To Fly', 211, 9),
        (15, 'Feel Alive', 195, 10);

    INSERT INTO collection
    VALUES
        (1, 'Dad News', 2000),
        (2, 'Hello', 2010),
        (3, 'JumpIT', 2012),
        (4, 'Touch Down', 2005),
        (5, 'Relax', 1980),
        (6, 'Classic NY', 1990),
        (7, 'U Choise', 2020),
        (8, 'Forever Young', 2000),
        (9, 'Enemy', 2011);

    INSERT INTO song_collection
    VALUES
        (1, 12, 7),
        (2, 5, 7),
        (3, 8, 2),
        (4, 15, 7),
        (5, 11, 1),
        (6, 9, 1),
        (7, 6, 3),
        (8, 4, 3),
        (9, 1, 9),
        (10, 2, 9),
        (11, 11, 6);
""")

# # название и год выхода альбомов, вышедших в 2018 году
# answer = connection.execute("""SELECT album_name, release_date FROM album
# WHERE release_date = 2018;""").fetchall()
# print(answer)
#
# # название и продолжительность самого длительного трека
# answer = connection.execute("""SELECT song_name, duration FROM song
# WHERE duration = (SELECT MAX(duration) FROM song);""").fetchall()
# print(answer)
#
# # название треков, продолжительность которых не менее 3,5 минуты
# answer = connection.execute("""SELECT song_name, duration FROM song
# WHERE duration >= 3.5*60;""").fetchall()
# print(answer)
#
# # названия сборников, вышедших в период с 2018 по 2020 год включительно
# answer = connection.execute("""SELECT collection_name FROM collection
# WHERE release_year >= 2018 and release_year <= 2020;""").fetchall()
# print(answer)
#
# # исполнители, чье имя состоит из 1 слова
# answer = connection.execute("""SELECT artist_name FROM artist
# WHERE artist_name NOT LIKE '%% %%';""").fetchall()
# print(answer)
#
# # название треков, которые содержат слово "мой"/"my"
# S = 'my'.capitalize()
# answer = connection.execute(f"""SELECT song_name FROM song
# WHERE song_name LIKE '%%{S}%%';""").fetchall()
# print(answer)