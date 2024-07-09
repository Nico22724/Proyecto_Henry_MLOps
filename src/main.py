from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Float, ForeignKey, func
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import extract
import calendar
DATABASE_URL = (
    "oracle+cx_oracle://ADMIN:_KK?G97bC59g.NF@adb.mx-monterrey-1.oraclecloud.com:1522/"
    "gc7ae344f7ff7b6_databasemovies_medium.adb.oraclecloud.com"
    "?ssl_server_cert_dn=CN=adb.mx-monterrey-1.oraclecloud.com, O=Oracle Corporation, "
    "L=Redwood City, ST=California, C=US"
)

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Create a session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create the FastAPI application
app = FastAPI()

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def read_root():
    return {"message": "Welcome to the Movie API"}


movies_table = Table(
    'MOVIES', metadata,
    Column('id_movie', Integer, primary_key=True),
    Column('budget', Integer),
    Column('popularity', Float),
    Column('release_date', Date),
    Column('revenue', Integer),
    Column('runtime', Float),
    Column('status', String),
    Column('vote_average', Float),
    Column('vote_count', Integer),
    Column('return', Float),
    Column('release_year', Integer),
)

actors_table = Table(
    'ID_CASTING_ACTOR', metadata,
    Column('id_actor', Integer, primary_key=True),
    Column('name', String),
)

staff_table = Table(
    'ID_STAFF', metadata,
    Column('id_staff', Integer, primary_key=True),
    Column('name', String),
)

staff_load_table = Table(
    'STAFF_LOAD', metadata,
    Column('id_staff', Integer, primary_key=True),
    Column('cargo', String),
)

id_genres_table = Table(
    'GENRES_ID', metadata,
    Column('id_genre', Integer, primary_key=True),
    Column('genre', String),
)

id_movie_id_genre_table = Table(
    'ID_MOVIE_ID_GENRE', metadata,
    Column('id_movie', Integer, ForeignKey("MOVIES.id_movie")),
    Column('id_genre', Integer, ForeignKey("GENRES_ID.id_genre")),
)

belong_name_id_movie_table = Table(
    'BELONG_NAME_ID_MOVIE', metadata,
    Column('id_belong', Integer, primary_key=True),
    Column('name', String),
    Column('id_movie', Integer, ForeignKey('MOVIE.id_movie')),
)

id_belong_id_genre_table = Table(
    'ID_BELONG_ID_GENRE', metadata,
    Column('id_belong', Integer, ForeignKey('BELONG_NAME_ID_MOVIE.id_belong')),
    Column('id_genres', Integer, ForeignKey('GENRES_ID.id_genre'))
)

casting_table = Table(
    'CASTING', metadata,
    Column('cast_id', Integer, primary_key=True),
    Column('character', String),
    Column('credit_id', String),
    Column('id_actor', Integer, ForeignKey('ID_CASTING_ACTOR.id_actor')),
    Column('order', Integer),
    Column('movie_id', Integer, ForeignKey('MOVIES.id_movie')),
)

production_staff_table = Table(
    'PRODUCTION_STAFF', metadata,
    Column('credit_id', String),
    Column('departament', String),
    Column('id_staff', Integer, ForeignKey('ID_STAFF.id_staff')),
    Column('job', String),
    Column('movie_id', Integer, ForeignKey('MOVIES.id_movie')),
)


@app.get("/items/")
def read_items(db: Session = Depends(get_db)):
    # Example of interacting with the database using the session `db`
    # Replace this with actual database queries as needed
    return {"message": "This endpoint interacts with the database"}

def month_name_to_number(month_name: str):
    month_name = month_name.lower()
    months = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
        'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }
    return months.get(month_name)

@app.get("/cantidad_filmaciones_mes/")
def cantidad_filmaciones_mes(mes: str, db: Session = Depends(get_db)):
    month_number = month_name_to_number(mes)
    if not month_number:
        return {"error": "Mes inválido"}

    query = movies_table.select().where(extract('month', movies_table.c.release_date) == month_number)
    result = db.execute(query).fetchall()
    return {"message": f"{len(result)} películas fueron estrenadas en el mes de {mes}"}

@app.get("/cantidad_filmaciones_dia/")
def cantidad_filmaciones_dia(dia: str, db: Session = Depends(get_db)):
    try:
        day_number = int(dia)
    except ValueError:
        return {"error": "Día inválido"}

    query = movies_table.select().where(extract('day', movies_table.c.release_date) == day_number)
    result = db.execute(query).fetchall()
    return {"message": f"{len(result)} películas fueron estrenadas en el dia {dia}"}

@app.get("/score_titulo/")
def score_titulo(titulo_de_la_filmacion: str, db: Session = Depends(get_db)):
    # Buscar el ID de la película con el título proporcionado en BELONG_NAME_ID_MOVIE
    query_belongs = belong_name_id_movie_table.select().where(
        belong_name_id_movie_table.c.name == titulo_de_la_filmacion
    )
    belong_info = db.execute(query_belongs).fetchone()

    if belong_info:
        # Obtener los detalles de la película desde MOVIES usando el ID obtenido
        query_movie = movies_table.select().where(movies_table.c.id_movie == belong_info.id_movie)
        movie = db.execute(query_movie).fetchone()

        if movie:
            return {
                "title": titulo_de_la_filmacion,
                "release_year": movie.release_date.year,
                "score": movie.vote_average
            }
        else:
            raise HTTPException(status_code=404, detail="Película no encontrada en la tabla MOVIES")
    else:
        raise HTTPException(status_code=404, detail="Película no encontrada en la tabla BELONG_NAME_ID_MOVIE")


@app.get("/votos_titulo/")
def votos_titulo(titulo_de_la_filmacion: str, db: Session = Depends(get_db)):
    # Buscar el ID de la película con el título proporcionado en BELONG_NAME_ID_MOVIE
    query_belongs = belong_name_id_movie_table.select().where(
        belong_name_id_movie_table.c.name == titulo_de_la_filmacion
    )
    belong_info = db.execute(query_belongs).fetchone()

    if belong_info:
        # Obtener los detalles de la película desde MOVIES usando el ID obtenido
        query_movie = movies_table.select().where(movies_table.c.id_movie == belong_info.id_movie)
        movie = db.execute(query_movie).fetchone()

        if movie:
            if movie.vote_count >= 2000:
                return {
                    "title": titulo_de_la_filmacion,
                    "release_year": movie.release_date.year,
                    "votes": movie.vote_count,
                    "average_vote": movie.vote_average
                }
            else:
                raise HTTPException(status_code=400, detail="La película no cumple con la condición de tener al menos 2000 valoraciones")
        else:
            raise HTTPException(status_code=404, detail="Película no encontrada en la tabla MOVIES")
    else:
        raise HTTPException(status_code=404, detail="Película no encontrada en la tabla BELONG_NAME_ID_MOVIE")


@app.get("/get_actor/")
def get_actor(nombre_actor: str, db: Session = Depends(get_db)):
    query_actor = actors_table.select().where(actors_table.c.name == nombre_actor)
    actor = db.execute(query_actor).fetchone()
    if actor:
        query_movies = casting_table.select().where(casting_table.c.id_actor == actor.id_actor)
        movie_ids = [row.movie_id for row in db.execute(query_movies).fetchall()]
        query_movies_details = movies_table.select().where(movies_table.c.id_movie.in_(movie_ids))
        movies = db.execute(query_movies_details).fetchall()
        if movies:
            total_revenue = sum(movie.revenue for movie in movies)
            return {
                "actor": nombre_actor,
                "number_of_movies": len(movies),
                "total_revenue": total_revenue,
                "average_revenue": total_revenue / len(movies) if movies else 0
            }
        else:
            raise HTTPException(status_code=404, detail=f"No se encontraron películas para el actor {nombre_actor}")
    else:
        raise HTTPException(status_code=404, detail="Actor no encontrado")

@app.get("/get_director/")
def get_director(nombre_director: str, db: Session = Depends(get_db)):
    # Verificar si el nombre del director existe en la tabla ID_STAFF
    query_director = staff_table.select().where(staff_table.c.name == nombre_director)
    director = db.execute(query_director).fetchone()
    if director:
        # Si el director existe, verificar si tiene el cargo de "director" en la tabla STAFF_LOAD
        query_cargo = staff_load_table.select().where(
            (staff_load_table.c.id_staff == director.id_staff) &
            (staff_load_table.c.cargo == 'director')
        )
        cargo_director = db.execute(query_cargo).fetchone()
        if cargo_director:
            # Si tiene el cargo de director, proceder a obtener las películas del director
            query_movies = production_staff_table.select().where(production_staff_table.c.id_staff == director.id_staff)
            movies = db.execute(query_movies).fetchall()
            if movies:
                director_info = []
                for movie in movies:
                    # Obtener el nombre de la película desde BELONG_NAME_ID_MOVIE
                    query_belongs = belong_name_id_movie_table.select().where(
                        belong_name_id_movie_table.c.id_movie == movie.id_movie
                    )
                    belong_info = db.execute(query_belongs).fetchone()
                    if belong_info:
                        movie_title = belong_info.name
                    else:
                        movie_title = "Título no encontrado"

                    director_info.append({
                        "title": movie_title,
                        "release_date": movie.release_date,
                        "individual_revenue": movie.revenue,
                        "budget": movie.budget,
                        "profit": movie.revenue - movie.budget
                    })
                return {
                    "director": nombre_director,
                    "movies": director_info
                }
            else:
                raise HTTPException(status_code=404, detail=f"No se encontraron películas para el director {nombre_director}")
        else:
            raise HTTPException(status_code=404, detail=f"{nombre_director} no tiene el cargo de director")
    else:
        raise HTTPException(status_code=404, detail="Director no encontrado")
