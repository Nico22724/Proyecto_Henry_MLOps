from fastapi import FastAPI, HTTPException
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

# URLs de los archivos CSV que se van a cargar
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

# Cargando los datos divididos de la tabla "casting" (actores postulantes)
url_df_casting_parte_uno = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20Datos%20Staff%20limpia/casting_data_parte1.csv"
df_casting_parte_uno = pd.read_csv(url_df_casting_parte_uno)

url_df_casting_parte_dos = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20Datos%20Staff%20limpia/casting_data_parte2.csv"
df_casting_parte_dos = pd.read_csv(url_df_casting_parte_dos)

# Cargando los datos divididos de la tabla "staff" (personal)
url_df_production_staff_parte_uno = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20Datos%20Staff%20limpia/staff_data_parte1.csv"
df_production_staff_parte_uno = pd.read_csv(url_df_production_staff_parte_uno)

url_df_production_staff_parte_dos = "https://raw.githubusercontent.com/Nico22724/Proyecto_Henry_MLOps/main/data/Base%20de%20Datos%20Staff%20limpia/staff_data_parte2.csv"
df_production_staff_parte_dos = pd.read_csv(url_df_production_staff_parte_dos)

# Concatenar las partes de los dataframes
df_casting = pd.concat([df_casting_parte_uno, df_casting_parte_dos])
df_production_staff = pd.concat([df_production_staff_parte_uno, df_production_staff_parte_dos])

# Crear la instancia de la aplicación FastAPI
app = FastAPI(
    title="FastAPI",
    description="Endpoints API",
    version="1.0.0"
)

# Función para convertir el nombre del mes de español a número
def mes_a_numero(mes):
    meses = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
        "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
    }
    return meses.get(mes.lower(), None)

# Función para convertir el nombre del día de español a su nombre en inglés
def dia_a_nombre(dia):
    dias = {
        "lunes": "Monday", "martes": "Tuesday", "miércoles": "Wednesday",
        "jueves": "Thursday", "viernes": "Friday", "sábado": "Saturday", "domingo": "Sunday"
    }
    return dias.get(dia.lower(), None)

# Endpoint para obtener la cantidad de filmaciones en un mes específico
@app.get("/cantidad_filmaciones_mes/{mes}")
def cantidad_filmaciones_mes(mes: str):
    mes_numero = mes_a_numero(mes)
    if mes_numero is None:
        raise HTTPException(status_code=400, detail="Mes inválido")
    
    df_movies['release_date'] = pd.to_datetime(df_movies['release_date'], errors='coerce')
    peliculas_mes = df_movies[df_movies['release_date'].dt.month == mes_numero]
    cantidad = int(len(peliculas_mes))  # Convertir a tipo nativo de Python
    return {"mes": mes, "cantidad": cantidad, "mensaje": f"{cantidad} películas fueron estrenadas en el mes de {mes}"}

# Endpoint para obtener la cantidad de filmaciones en un día específico
@app.get("/cantidad_filmaciones_dia/{dia}")
def cantidad_filmaciones_dia(dia: str):
    dia_nombre = dia_a_nombre(dia)
    if dia_nombre is None:
        raise HTTPException(status_code=400, detail="Día inválido")
    
    df_movies['release_date'] = pd.to_datetime(df_movies['release_date'], errors='coerce')
    peliculas_dia = df_movies[df_movies['release_date'].dt.day_name() == dia_nombre]
    cantidad = int(len(peliculas_dia))  # Convertir a tipo nativo de Python
    return {"dia": dia, "cantidad": cantidad, "mensaje": f"{cantidad} películas fueron estrenadas en los días {dia}"}

# Endpoint para obtener el score de una película por título
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
        "anio_estreno": int(movie_details['release_year']),  # Convertir a tipo nativo de Python
        "score": float(movie_details['popularity']),  # Convertir a tipo nativo de Python
        "mensaje": f"La película {pelicula_info['name']} fue estrenada en el año {movie_details['release_year']} con un score/popularidad de {movie_details['popularity']}"
    }

# Endpoint para obtener la cantidad de votos y promedio de votos de una película por título
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
        "cantidad_votos": int(movie_details['vote_count']),  # Convertir a tipo nativo de Python
        "promedio_votos": float(movie_details['vote_average']),  # Convertir a tipo nativo de Python
        "mensaje": f"La película {pelicula_info['name']} fue estrenada en el año {movie_details['release_year']}. La misma cuenta con un total de {movie_details['vote_count']} valoraciones, con un promedio de {movie_details['vote_average']}"
    }

# Endpoint para obtener información sobre un actor por nombre
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

# Endpoint para obtener información sobre un director por nombre
@app.get("/get_director/{nombre_director}")
def get_director(nombre_director: str):
    director_staff = df_id_staff[df_id_staff['name'].str.contains(nombre_director, case=False, na=False)]
    if director_staff.empty:
        raise HTTPException(status_code=404, detail="Director no encontrado")
    
    director_id = director_staff['id_staff'].iloc[0]
    director_movies = pd.concat([df_production_staff_parte_uno, df_production_staff_parte_dos])
    director_movies = director_movies[
        (director_movies['id_staff'] == director_id) & 
        (director_movies['job'] == 'Director')
    ]
    
    if director_movies.empty:
        raise HTTPException(status_code=404, detail="Director no tiene películas registradas")

    resultado = []
    for _, row in director_movies.iterrows():
        pelicula_info = df_movies[df_movies['id_movie'] == row['movie_id']].iloc[0]
        titulo_info = df_belong_name_id_movie[df_belong_name_id_movie['id_movie'] == row['movie_id']]
        titulo = titulo_info['name'].iloc[0] if not titulo_info.empty else "Título no encontrado"
        
        resultado.append({
            "titulo": titulo,
            "fecha_lanzamiento": str(pelicula_info['release_date']),  # Convertir a tipo nativo de Python
            "retorno_individual": float(pelicula_info['return']),  # Convertir a tipo nativo de Python
            "costo": float(pelicula_info['budget']),  # Convertir a tipo nativo de Python
            "ganancia": float(pelicula_info['revenue'])  # Convertir a tipo nativo de Python
        })
    
    return {
        "director": nombre_director,
        "peliculas": resultado,
        "mensaje": f"El director {nombre_director} tiene {len(resultado)} películas registradas"
    }
    
# Función para combinar información de las películas
def obtener_peliculas_completas():
    df_completa = df_belong_name_id_movie.merge(df_movies, on='id_movie')
    df_completa = df_completa.merge(df_id_movie_id_genre, on='id_movie')
    df_completa = df_completa.merge(df_genres_id, left_on='id_genre', right_on='id_genre')
    df_completa['genres'] = df_completa.groupby('id_movie')['genre'].transform(lambda x: ' '.join(x))
    df_completa = df_completa.drop_duplicates(subset=['id_movie'])
    return df_completa

# Obtener el DataFrame consolidado
df_peliculas_completas = obtener_peliculas_completas()

# Vectorizar los géneros para calcular la similitud
vectorizer = TfidfVectorizer()
tfidf_matrix = vectorizer.fit_transform(df_peliculas_completas['genres'])

# Calcular la similitud del coseno
cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

# Función para obtener el índice de la película dado su título
def obtener_indice_titulo(titulo):
    try:
        return df_peliculas_completas[df_peliculas_completas['name'].str.contains(titulo, case=False)].index[0]
    except IndexError:
        raise HTTPException(status_code=404, detail="Película no encontrada")

# Función de recomendación
@app.get("/recomendacion/{titulo}")
def recomendacion(titulo: str):
    indice_pelicula = obtener_indice_titulo(titulo)
    similitudes = list(enumerate(cosine_sim[indice_pelicula]))
    similitudes = sorted(similitudes, key=lambda x: x[1], reverse=True)
    peliculas_similares = [df_peliculas_completas['name'].iloc[i[0]] for i in similitudes[1:6]]
    return {"titulo": titulo, "recomendaciones": peliculas_similares}

# Punto de entrada para ejecutar la aplicación FastAPI con Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
