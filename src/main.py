from fastapi import FastAPI, HTTPException
import pandas as pd

url_df_id_belong_id_genre = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20%20Datos%20Movie%20limpia/belong_id_genre.csv"
df_id_belong_id_genre = pd.read_csv(url_df_id_belong_id_genre)

url_df_belong_name_id_movie = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20%20Datos%20Movie%20limpia/belong_name_id.csv"
df_belong_name_id_movie = pd.read_csv(url_df_belong_name_id_movie)

url_df_movies = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20%20Datos%20Movie%20limpia/df_data_central.csv"
df_movies = pd.read_csv(url_df_movies)

url_df_genres_id = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20%20Datos%20Movie%20limpia/genres_id.csv"
df_genres_id = pd.read_csv(url_df_genres_id)

url_df_id_movie_id_genre = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20%20Datos%20Movie%20limpia/id_movie_id_genre.csv"
df_id_movie_id_genre = pd.read_csv(url_df_id_movie_id_genre)

url_df_id_casting_actor = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20Datos%20Staff%20limpia/id_casting_actor_purificada.csv"
df_id_casting_actor = pd.read_csv(url_df_id_casting_actor)

url_df_id_staff = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20Datos%20Staff%20limpia/id_staff_purificada.csv"
df_id_staff = pd.read_csv(url_df_id_staff)

url_df_staff_load = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20Datos%20Staff%20limpia/cargo_staff.csv"
df_staff_load = pd.read_csv(url_df_staff_load)


#MANEJAREMOS LOS DATOS QUE ESTAN DIVIDOS PRIMERO LA TABLA CASTING DONDE ESTAN LOS ACTORES POSTULANTES
url_df_casting_parte_uno = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20Datos%20Staff%20limpia/casting_data_parte1.csv"
df_casting_parte_uno = pd.read_csv(url_df_casting_parte_uno)

url_df_casting_parte_dos = "https://github.com/Nico22724/Proyecto_Henry_MLOps/blob/main/data/Base%20de%20Datos%20Staff%20limpia/casting_data_parte2.csv"
df_casting_parte_dos = pd.read_csv(url_df_casting_parte_dos)

#MANEJAREMOS LA TABLA STAFF DONDE ESTAN EL PERSONAL
url_df_production_staff_parte_uno = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20Datos%20Staff%20limpia/staff_data_parte1.csv"
df_production_staff_parte_uno = pd.read_csv(url_df_production_staff_parte_uno)

url_df_production_staff_parte_dos = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20Datos%20Staff%20limpia/staff_data_parte2.csv"
df_production_staff_parte_dos = pd.read_csv(url_df_production_staff_parte_dos)

df_casting = pd.concat([df_casting_parte_uno, df_casting_parte_dos])
df_production_staff = pd.concat([df_production_staff_parte_uno, df_production_staff_parte_dos])

def mes_a_numero(mes):
    meses = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
        "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
    }
    return meses.get(mes.lower(), None)

# Función para convertir el nombre del día en español a su nombre en inglés
def dia_a_nombre(dia):
    dias = {
        "lunes": "Monday", "martes": "Tuesday", "miércoles": "Wednesday",
        "jueves": "Thursday", "viernes": "Friday", "sábado": "Saturday", "domingo": "Sunday"
    }
    return dias.get(dia.lower(), None)

@app.get("/cantidad_filmaciones_mes/{mes}")
def cantidad_filmaciones_mes(mes: str):
    mes_numero = mes_a_numero(mes)
    if mes_numero is None:
        raise HTTPException(status_code=400, detail="Mes inválido")
    
    df_movies['release_date'] = pd.to_datetime(df_movies['release_date'], errors='coerce')
    peliculas_mes = df_movies[df_movies['release_date'].dt.month == mes_numero]
    cantidad = len(peliculas_mes)
    return {"mes": mes, "cantidad": cantidad, "mensaje": f"{cantidad} películas fueron estrenadas en el mes de {mes}"}

@app.get("/cantidad_filmaciones_dia/{dia}")
def cantidad_filmaciones_dia(dia: str):
    dia_nombre = dia_a_nombre(dia)
    if dia_nombre is None:
        raise HTTPException(status_code=400, detail="Día inválido")
    
    df_movies['release_date'] = pd.to_datetime(df_movies['release_date'], errors='coerce')
    peliculas_dia = df_movies[df_movies['release_date'].dt.day_name() == dia_nombre]
    cantidad = len(peliculas_dia)
    return {"dia": dia, "cantidad": cantidad, "mensaje": f"{cantidad} películas fueron estrenadas en los días {dia}"}

@app.get("/score_titulo/{titulo}")
def score_titulo(titulo: str):
    pelicula = df_belong_name_id_movie[df_belong_name_id_movie['name'].str.contains(titulo, case=False, na=False)]
    if pelicula.empty:
        raise HTTPException(status_code=404, detail="Película no encontrada")
    
    pelicula_info = pelicula.iloc[0]
    id_movie = pelicula_info['id_movie']
    movie_details = df_movies[df_movies['id_movie'] == id_movie].iloc[0]
    return {
        "titulo": pelicula_info['name'],
        "anio_estreno": movie_details['release_year'],
        "score": movie_details['popularity'],
        "mensaje": f"La película {pelicula_info['name']} fue estrenada en el año {movie_details['release_year']} con un score/popularidad de {movie_details['popularity']}"
    }

@app.get("/votos_titulo/{titulo}")
def votos_titulo(titulo: str):
    pelicula = df_belong_name_id_movie[df_belong_name_id_movie['name'].str.contains(titulo, case=False, na=False)]
    if pelicula.empty:
        raise HTTPException(status_code=404, detail="Película no encontrada")
    
    pelicula_info = pelicula.iloc[0]
    id_movie = pelicula_info['id_movie']
    movie_details = df_movies[df_movies['id_movie'] == id_movie].iloc[0]
    if movie_details['vote_count'] < 2000:
        return {"mensaje": "La película no cumple con la condición de tener al menos 2000 valoraciones"}
    
    return {
        "titulo": pelicula_info['name'],
        "cantidad_votos": movie_details['vote_count'],
        "promedio_votos": movie_details['vote_average'],
        "mensaje": f"La película {pelicula_info['name']} fue estrenada en el año {movie_details['release_year']}. La misma cuenta con un total de {movie_details['vote_count']} valoraciones, con un promedio de {movie_details['vote_average']}"
    }

@app.get("/get_actor/{nombre_actor}")
def get_actor(nombre_actor: str):
    actor_movies = df_casting[df_casting['character'].str.contains(nombre_actor, case=False, na=False)]
    if actor_movies.empty:
        raise HTTPException(status_code=404, detail="Actor no encontrado")
    
    cantidad_peliculas = len(actor_movies)
    retorno_total = actor_movies['return'].sum()
    promedio_retorno = retorno_total / cantidad_peliculas if cantidad_peliculas > 0 else 0
    return {
        "actor": nombre_actor,
        "cantidad_peliculas": cantidad_peliculas,
        "retorno_total": retorno_total,
        "promedio_retorno": promedio_retorno,
        "mensaje": f"El actor {nombre_actor} ha participado de {cantidad_peliculas} filmaciones, consiguiendo un retorno de {retorno_total} con un promedio de {promedio_retorno} por filmación"
    }

@app.get("/get_director/{nombre_director}")
def get_director(nombre_director: str):
    director_staff = df_id_staff[df_id_staff['name'].str.contains(nombre_director, case=False, na=False)]
    if director_staff.empty:
        raise HTTPException(status_code=404, detail="Director no encontrado")
    
    director_id = director_staff['id_staff'].iloc[0]
    director_movies = df_production_staff[
        (df_production_staff['id_staff'] == director_id) & 
        (df_production_staff['job'] == 'Director')
    ]
    
    if director_movies.empty:
        raise HTTPException(status_code=404, detail="Director no tiene películas registradas")

    resultado = []
    for _, row in director_movies.iterrows():
        pelicula_info = df_movies[df_movies['id_movie'] == row['movie_id']].iloc[0]
        resultado.append({
            "titulo": pelicula_info['title'],
            "fecha_lanzamiento": pelicula_info['release_date'],
            "retorno_individual": pelicula_info['return'],
            "costo": pelicula_info['budget'],
            "ganancia": pelicula_info['revenue']
        })
    
    return {
        "director": nombre_director,
        "peliculas": resultado,
        "mensaje": f"El director {nombre_director} tiene {len(resultado)} películas registradas"
    }
