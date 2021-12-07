import psycopg2
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://postgres:ImAlive72ae@localhost:5432/SelectRequests')

connection = engine.connect()


# количество исполнителей в каждом жанре
answer = connection.execute("""SELECT genre_name, COUNT(artist_id) FROM genre g
JOIN genre_artist ga ON g.id = ga.genre_id
GROUP BY g.genre_name;""").fetchall()
print(answer)

# количество треков, вошедших в альбомы 2019-2020 годов
answer = connection.execute("""SELECT album_name, COUNT(s.id) FROM album a
JOIN song s ON a.id = s.album_id
WHERE release_date = 2019 or release_date = 2020
GROUP BY a.album_name;""").fetchall()
print(answer)

# средняя продолжительность треков по каждому альбому
answer = connection.execute("""SELECT album_name, AVG(s.duration) FROM album a
JOIN song s ON a.id = s.album_id
GROUP BY album_name;""").fetchall()
print(answer)

# все исполнители, которые не выпустили альбомы в 2020 году
answer = connection.execute("""SELECT artist_name FROM artist a
JOIN album_artist aa ON a.id = aa.artist_id
JOIN album ON aa.album_id = album.id
WHERE release_date != 2020
GROUP BY artist_name;""").fetchall()
print(answer)

# названия сборников, в которых присутствует конкретный исполнитель (выберите сами)
answer = connection.execute("""SELECT collection_name, a.artist_name FROM collection c
JOIN song_collection sc ON c.id = sc.collection_id
JOIN song ON sc.song_id = song.id
JOIN album ON song.album_id = album.id
JOIN album_artist aa ON album.id = aa.album_id
JOIN artist a ON aa.artist_id = a.id
WHERE artist_name = 'Bob Dylan';""").fetchall()
print(answer)

# название альбомов, в которых присутствуют исполнители более 1 жанра
answer = connection.execute("""SELECT album_name, artist.artist_name FROM album a
JOIN album_artist aa ON a.id = aa.album_id
JOIN artist ON aa.artist_id = artist.id
JOIN genre_artist ga ON artist.id = ga.artist_id
JOIN genre g ON ga.genre_id = g.id
GROUP BY a.album_name, artist.artist_name
HAVING COUNT(ga.genre_id) > 1
;""").fetchall()
print(answer)

# наименование треков, которые не входят в сборники
answer = connection.execute("""SELECT song_name, sc.collection_id FROM song s
FULL JOIN song_collection sc ON s.id = sc.song_id
WHERE sc.collection_id IS NULL
GROUP BY s.song_name, sc.collection_id;""").fetchall()
print(answer)

# исполнителей, написавшего самый короткий по продолжительности трек (теоретически таких треков может быть несколько)
answer = connection.execute("""SELECT artist_name, MIN(s.duration) FROM artist a
JOIN album_artist aa ON a.id = aa.artist_id
JOIN album ON aa.album_id = album.id
JOIN song s ON album.id = s.album_id
WHERE duration = (SELECT MIN(duration) FROM song)
GROUP BY artist_name;""").fetchall()
print(answer)

# название альбомов, содержащих наименьшее количество треков
answer = connection.execute("""SELECT album_name FROM album a
LEFT JOIN song s ON a.id = s.album_id
WHERE s.album_id IN (SELECT album_id FROM song
    GROUP BY album_id
    HAVING COUNT(id) = (SELECT COUNT(id) FROM song
        GROUP BY album_id
        ORDER BY COUNT
        LIMIT 1)
        );""").fetchall()
print(answer)
