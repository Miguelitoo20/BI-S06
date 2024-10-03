import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Conexión
engine = create_engine('sqlite:///movielens_100k.db')

# Crear base de datos
def create_database():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='Users';"))
        if not result.fetchone():
            print("Base de datos creada.")
        else:
            print("La base de datos ya existe.")

# Leer archivos
def load_and_process_data():
    try:
        # 1. Cargar usuarios
        users_df = pd.read_csv('./ml-100k/u.user', sep='|', names=['user_id', 'age', 'gender', 'occupation', 'zip_code'])
        print(f"Usuarios cargados: {len(users_df)}")

        # 2. Cargar películas
        items_columns = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url'] + [f'genre_{i}' for i in range(19)]
        movies_df = pd.read_csv('./ml-100k/u.item', sep='|', names=items_columns, encoding='ISO-8859-1')
        genres_columns = items_columns[5:]
        movies_df['genres'] = movies_df[genres_columns].apply(lambda row: ','.join([str(i) for i, val in enumerate(row) if val == 1]), axis=1)
        movies_df = movies_df[['movie_id', 'title', 'release_date', 'imdb_url', 'genres']]
        print(f"Películas cargadas: {len(movies_df)}")

        # 3. Cargar calificaciones
        ratings_df = pd.read_csv('./ml-100k/u.data', sep='\t', names=['user_id', 'movie_id', 'rating', 'timestamp'])
        ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'], unit='s')
        print(f"Calificaciones cargadas: {len(ratings_df)}")

        analyze_data(users_df, movies_df, ratings_df)

        insert_data_sql(users_df, movies_df, ratings_df)

    except SQLAlchemyError as e:
        print(f"Error al conectar con la base de datos: {e}")
    except FileNotFoundError as e:
        print(f"Error de archivo: {e}")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

# Análisis de datos
def analyze_data(users_df, movies_df, ratings_df):
    # 1. Distribución de calificaciones
    rating_distribution = ratings_df['rating'].value_counts().sort_index()
    print("\nDistribución de calificaciones:")
    print(rating_distribution)

    # Gráfico: Distribución de calificaciones
    plt.figure(figsize=(8, 6))
    sns.barplot(x=rating_distribution.index, y=rating_distribution.values, hue=rating_distribution.index, palette='plasma', legend=False)
    plt.title('Distribución de Calificaciones')
    plt.xlabel('Calificación')
    plt.ylabel('Cantidad')
    plt.show()

    # 2. Promedio de calificaciones por película
    avg_ratings_per_movie = ratings_df.groupby('movie_id')['rating'].mean()
    print("\nPromedio de calificaciones por película:")
    print(avg_ratings_per_movie.head())

    # Gráfico: Distribución del promedio de calificaciones
    plt.figure(figsize=(8, 6))
    sns.histplot(avg_ratings_per_movie, kde=True, color='green')
    plt.title('Distribución del Promedio de Calificaciones por Película')
    plt.xlabel('Promedio de Calificación')
    plt.ylabel('Frecuencia')
    plt.show()

    # 3. Usuarios por ocupación
    occupation_distribution = users_df['occupation'].value_counts()
    print("\nDistribución de usuarios por ocupación:")
    print(occupation_distribution)

    # Gráfico: Distribución de ocupación
    plt.figure(figsize=(10, 6))
    sns.barplot(x=occupation_distribution.index, y=occupation_distribution.values, hue=occupation_distribution.index, palette='magma', legend=False)
    plt.title('Distribución de Usuarios por Ocupación')
    plt.xlabel('Ocupación')
    plt.ylabel('Cantidad')
    plt.xticks(rotation=90)
    plt.show()

    # 4. Usuarios por género
    gender_distribution = users_df['gender'].value_counts()
    print("\nDistribución de usuarios por género:")
    print(gender_distribution)

    # Gráfico: Distribución de género
    plt.figure(figsize=(6, 4))
    sns.barplot(x=gender_distribution.index, y=gender_distribution.values, hue=gender_distribution.index, palette='coolwarm', legend=False)
    plt.title('Distribución de Usuarios por Género')
    plt.xlabel('Género')
    plt.ylabel('Cantidad')
    plt.show()

    # 5. Películas mejor valoradas
    top_rated_movies = ratings_df.groupby('movie_id')['rating'].mean().sort_values(ascending=False).head(10)
    top_movies = movies_df[movies_df['movie_id'].isin(top_rated_movies.index)]
    print("\nTop 10 películas mejor valoradas:")
    print(top_movies[['title', 'genres']])

    # Gráfico: Top 10 películas mejor valoradas
    plt.figure(figsize=(10, 6))
    sns.barplot(x=top_movies['title'], y=top_rated_movies.values, hue=top_movies['title'], palette='viridis', legend=False)
    plt.title('Top 10 Películas Mejor Valoradas')
    plt.xticks(rotation=45)
    plt.xlabel('Película')
    plt.ylabel('Promedio de Calificación')
    plt.show()

    # 6. Correlación entre edad y calificaciones
    user_ratings = pd.merge(ratings_df, users_df, on='user_id')
    plt.figure(figsize=(8, 6))
    sns.scatterplot(x=user_ratings['age'], y=user_ratings['rating'], alpha=0.5)
    plt.title('Relación entre Edad y Calificaciones')
    plt.xlabel('Edad')
    plt.ylabel('Calificación')
    plt.show()

def insert_data_sql(users_df, movies_df, ratings_df):
    try:
        users_df.to_sql('Users', engine, if_exists='replace', index=False)
        movies_df.to_sql('Movies', engine, if_exists='replace', index=False)
        ratings_df.to_sql('Ratings', engine, if_exists='replace', index=False)
        print("Datos insertados correctamente en la base de datos.")
    except SQLAlchemyError as e:
        print(f"Error al insertar datos: {e}")

create_database()
load_and_process_data()
