from fastapi import FastAPI, Depends
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

production_staff_table = Table(
    'ID_STAFF', metadata,
    Column('id_staff', Integer, primary_key=True),
    Column('name', String),
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
    months = {name.lower(): num for num, name in enumerate(calendar.month_name) if name}
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
    return {"message": f"{len(result)} películas fueron estrenadas en los días {dia}"}

@app.get("/score_titulo/")
def score_titulo(titulo_de_la_filmacion: str, db: Session = Depends(get_db)):
    query = movies_table.select().where(movies_table.c.title == titulo_de_la_filmacion)
    result = db.execute(query).fetchone()
    if result:
        return {"title": result.title, "release_year": result.release_date.year, "score": result.vote_average}
    else:
        return {"error": "Película no encontrada"}

@app.get("/votos_titulo/")
def votos_titulo(titulo_de_la_filmacion: str, db: Session = Depends(get_db)):
    query = movies_table.select().where(movies_table.c.title == titulo_de_la_filmacion)
    result = db.execute(query).fetchone()
    if result and result.vote_count >= 2000:
        return {"title": result.title, "release_year": result.release_date.year, "votes": result.vote_count, "average_vote": result.vote_average}
    elif result:
        return {"message": "La película no cumple con la condición de tener al menos 2000 valoraciones"}
    else:
        return {"error": "Película no encontrada"}

@app.get("/get_actor/")
def get_actor(nombre_actor: str, db: Session = Depends(get_db)):
    query = actors_table.select().where(actors_table.c.name == nombre_actor)
    actor = db.execute(query).fetchone()
    if actor:
        movies_query = casting_table.select().where(casting_table.c.id_actor == actor.id_actor)
        movie_ids = [row.movie_id for row in db.execute(movies_query).fetchall()]
        movies_query = movies_table.select().where(movies_table.c.id_movie.in_(movie_ids))
        movies = db.execute(movies_query).fetchall()
        total_revenue = sum(movie.revenue for movie in movies)
        return {"actor": actor.name, "number_of_movies": len(movies), "total_revenue": total_revenue, "average_revenue": total_revenue / len(movies) if movies else 0}
    else:
        return {"error": "Actor no encontrado"}

@app.get("/get_director/")
def get_director(nombre_director: str, db: Session = Depends(get_db)):
    query = production_staff_table.select().where(production_staff_table.c.name == nombre_director)
    director = db.execute(query).fetchone()
    if director:
        movies_query = production_staff_table.select().where(production_staff_table.c.id_staff == director.id_staff)
        movie_ids = [row.movie_id for row in db.execute(movies_query).fetchall()]
        movies_query = movies_table.select().where(movies_table.c.id_movie.in_(movie_ids))
        movies = db.execute(movies_query).fetchall()
        director_info = []
        for movie in movies:
            director_info.append({
                "title": movie.title,
                "release_date": movie.release_date,
                "individual_revenue": movie.revenue,
                "budget": movie.budget,
                "profit": movie.revenue - movie.budget
            })
        return {"director": director.name, "movies": director_info}
    else:
        return {"error": "Director no encontrado"}