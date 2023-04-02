import psycopg2

def make_connect(request: str):
    with psycopg2.connect(dbname="testdb", user="postgres", password="8440") as conn:
        with conn.cursor() as cur:
            cur.execute(request)
            conn.commit()
            return cur.fetchall()
print('Первое задание. Количество исполнителей в каждом жанре:')
count_of_musicians = make_connect(" SELECT COUNT(genre_name)AS count_of_musicians_in_one_genre, genre_name from genre_musician\
                                    JOIN genre USING (genre_id)\
                                    JOIN musician USING (musician_id)\
                                    GROUP BY genre_id, genre_name\
                                    ORDER BY count_of_musicians_in_one_genre")
for i, k in count_of_musicians:
    print(f'{i} in {k}')
print()
print('Второе задание. Количество треков, вошедших в альбомы 2019–2020 годов:')
count_of_tracks_2019_2020 = make_connect("  SELECT COUNT(track_name)\
                                            FROM tracks\
                                            JOIN album USING (album_id)\
                                            WHERE album_year BETWEEN 2019 and 2020")
print(count_of_tracks_2019_2020[0][0])
print()
print('Третье задание. Средняя продолжительность треков по каждому альбому:')
avg_duration_of_each_album = make_connect(" SELECT AVG(track_duration) as avg_track_duration, album_name\
                                            FROM tracks\
                                            JOIN album USING (album_id)\
                                            GROUP BY album_name")
for i, j in avg_duration_of_each_album:
    print(i,'-', j)
print()
print('Четвертое задание. Все исполнители, которые не выпустили альбомы в 2020 году:')
count_of_musicians_didnt_realease_in_2020 = make_connect("  SELECT DISTINCT(musician_name) FROM musician_album\
                                                            JOIN album USING (album_id)\
                                                            JOIN musician USING (musician_id)\
                                                    WHERE musician_id NOT IN (	SELECT musician_id FROM musician_album\
							                                                    JOIN album USING (album_id)\
							                                                    JOIN musician USING (musician_id)\
							                                                    WHERE album_year = 2020)")
count_for_2020 = 1
for i in count_of_musicians_didnt_realease_in_2020:
    print(f"{count_for_2020}) {str(i).strip('(),')}")
    count_for_2020 += 1
print()
print('Пятое задание. Названия сборников, в которых присутствует конкретный исполнитель (London Elektricity):')
mix_concrete = make_connect("SELECT DISTINCT(mix_name), musician_name from tracks_mix\
                            JOIN mix USING (mix_id)\
                            JOIN tracks USING (track_id)\
                            JOIN musician_album USING (album_id)\
                            JOIN musician USING (musician_id)\
                            WHERE musician_name = 'London Elektricity'")
for i, j in mix_concrete:
    print(i, '-', j)
print()
print('Шестое задание. Названия альбомов, в которых присутствуют исполнители более чем одного жанра:')
name_of_more_than_one_albums = make_connect("\
                                                SELECT DISTINCT(album_name) from genre_musician\
                                                JOIN musician_album USING (musician_id)\
                                                JOIN album USING (album_id)\
                                                WHERE musician_id IN (	SELECT  musician_id from genre_musician\
                                                                    GROUP BY musician_id\
                                                                    HAVING COUNT(musician_id) > 1)")
for i in name_of_more_than_one_albums:
    print(str(i).strip('(),'))
print()
print('Седьмое задание. Наименования треков, которые не входят в сборники:')
tracks_without_mix = make_connect(" SELECT track_name FROM tracks_mix\
                                    RIGHT JOIN tracks USING (track_id)\
                                    WHERE mix_id IS NULL")
for i in tracks_without_mix:
    print(str(i).strip('(),'))
print()
print('Восьмое задание. Исполнитель или исполнители, написавшие самый короткий по продолжительности трек:')
musician_with_the_shortest_track = make_connect("   SELECT musician_name FROM tracks\
                                                    JOIN musician_album USING (album_id)\
                                                    JOIN musician USING (musician_id)\
                                                    WHERE track_duration = (\
                                                                            SELECT MIN(track_duration)\
                                                                            FROM tracks)")
for i in musician_with_the_shortest_track:
    print(str(i).strip('(),'))
print()
print('Девятое задание. Названия альбомов, содержащих наименьшее количество треков:')
albums_with_the_lowest_tracks = make_connect()