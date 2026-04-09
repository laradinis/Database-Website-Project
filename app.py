#____Importações e Configuração Inicial____

import sqlite3
import base64  # RE-ADICIONADO
# 'url_for' adicionado à importação
from flask import Flask, render_template, g, request, abort, url_for 
import re

DATABASE = 'disneyplusDB.db'
app = Flask(__name__)
app.config['SECRET_KEY'] = 'DisneyBD'

# Caminhos para os arquivos de imagem (ASSUMIMOS que estão na mesma pasta do app.py)
IMAGE_ER_PATH = 'Modelo_ER.jpg' 
IMAGE_RELACIONAL_PATH = 'ModeloRelacional.jpg' # NOVO CAMINHO

#____Funções de Conexão Segura à BD____

def get_db():       
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def query_db(query, args=(), one=False):
    try:
        cur = get_db().execute(query, args)
        rv = cur.fetchall()
        cur.close()
        return (rv[0] if rv else None) if one else rv
    except sqlite3.Error as e:
        print(f"Erro de SQL ao executar a query: {query}. Erro: {e}")
        return [] if not one else None


# ____NOVAS FUNÇÕES E ROTAS para Modelos Relacionais e ER____

def encode_image_to_base64(file_path):
    """Lê um arquivo de imagem, codifica-o para Base64 e retorna o Data URI."""
    try:
        # Abertura em modo binário ('rb')
        with open(file_path, 'rb') as image_file:
            # Codificação para Base64
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
            # Retorna o Data URI. Assumimos 'image/jpeg'
            return f"data:image/jpeg;base64,{encoded_string}"
    except FileNotFoundError:
        print(f"Erro: O arquivo de imagem em {file_path} não foi encontrado.")
        return ""


def render_image_template(title, image_filename):
    """Função de suporte para renderizar o template da imagem."""
    return render_template('model_view.html', title=title, image_filename=image_filename)

@app.route('/modelo_relacional/')
def relational_model():
    """Rota para exibir a imagem do Modelo Relacional, usando Base64."""
    base64_data_url = encode_image_to_base64(IMAGE_RELACIONAL_PATH)
    # Título SEM "(Base64)"
    return render_image_template("Modelo Relacional", base64_data_url) 

@app.route('/modelo_er/')
def er_model():
    """Rota para exibir a imagem do Modelo de Entidade-Relacionamento (ER), usando Base64."""
    base64_data_url = encode_image_to_base64(IMAGE_ER_PATH)
    # Título SEM "(Base64)"
    return render_image_template("Modelo ER", base64_data_url)


#____Funções de Suporte à BD_____
def get_table_names():
    tables = query_db("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    return [row['name'] for row in tables]

def get_pk_column(table_name):
    try:
        info = query_db(f"PRAGMA table_info('{table_name}');")
        pk_col = next((row['name'] for row in info if row['pk'] == 1), None)
        if not pk_col:
            if f'{table_name}_id' in [row['name'] for row in info]:
                return f'{table_name}_id'
        return pk_col
    except Exception:
        return None
    
def get_db_stats():
    tables = get_table_names()
    total_rows = 0
    total_columns = 0
    table_stats = []
    for table in tables:
        try:
            row_count = query_db(f"SELECT COUNT(*) AS count FROM {table}", one=True)['count']
            total_rows += row_count
            info = query_db(f"PRAGMA table_info('{table}');")
            col_count = len(info)
            total_columns += col_count
            table_stats.append({'name': table, 'rows': row_count, 'cols': col_count})
        except Exception as e:
            print(f"Erro ao processar a tabela {table}: {e}")
            continue

    return {
        'total_tables': len(tables),
        'total_rows': total_rows,
        'total_columns': total_columns,
        'table_details': table_stats
    }

# ---- Lista de textos das perguntas (para mostrar no query_result.html) ----
QUESTIONS = {
    1: "Ordenar todos os filmes de animação lançados entre 2000 e 2020, por ordem lexicográfica.",
    2: "Contar, com base no tipo de conteúdo, quantas descrições contêm números.",
    3: "Contar o número de filmes e o número de séries dirigidas por cada diretor.",
    4: "Listar todas as obras, não de ação-aventura, cujo título contém um sinal de pontuação.",
    5: "Listar todas as séries não realizadas nos Estados Unidos, com mais de 1 temporada.",
    6: "Obras dirigidas por diretor cujo nome começa por 'J' e com atores cujo nome começa por 'S'.",
    7: "Contar todos os filmes com duração superior a 100 minutos, agrupados por género e ordenados pelo ano.",
    8: "Contar todas as obras adicionadas entre março e novembro de 2021, agrupadas por rating.",
    9: "Listar obras cuja descrição contém o título OU não contém nenhum membro do elenco.",
    10: "Listar filmes com duração entre 30 e 120 minutos agrupados por década.",
    11: "Encontrar diretores e atores que colaboraram mais de uma vez."
}

SQL_QUERIES = {
    1: '''
        SELECT 
            content.title AS Title, 
            content.release_date AS Release_Year
        FROM genre 
        JOIN classification ON genre.genre_id = classification.genre_id
        JOIN content ON classification.content_id = content.content_id
        JOIN type ON content.type_id = type.type_id
        WHERE type.designation = 'Movie' 
          AND genre.designation = 'Animation' 
          AND content.release_date >= 2000 
          AND content.release_date <= 2020
        ORDER BY content.title;
    ''',
    2: '''
        SELECT type.designation as type, COUNT(*) as total
        FROM content
        JOIN type ON content.type_id = type.type_id
        WHERE type.designation IN ('Movie', 'TV Show') 
        AND (description LIKE '%0%' OR description LIKE '%1%' OR description LIKE '%2%'
             OR description LIKE '%3%' OR description LIKE '%4%' OR description LIKE '%5%'
             OR description LIKE '%6%' OR description LIKE '%7%' OR description LIKE '%8%'
             OR description LIKE '%9%')
        GROUP BY type.designation;
    ''',
    3: '''
        SELECT*
        FROM (
            SELECT person.name as director, type.designation as type, COUNT(*) as count
            FROM direction 
            JOIN person ON direction.person_id = person.person_id
            JOIN content ON direction.content_id = content.content_id
            JOIN type ON content.type_id = type.type_id
            WHERE type.designation = 'Movie'
            GROUP BY person.name
            UNION
            SELECT person.name as director, type.designation as type, COUNT(*) as count
            FROM direction 
            JOIN person ON direction.person_id = person.person_id
            JOIN content ON direction.content_id = content.content_id
            JOIN type ON content.type_id = type.type_id
            WHERE type.designation = 'TV Show'
            GROUP BY person.name
        );
    ''',
    4: '''
        SELECT DISTINCT content.title
        FROM content
        JOIN classification ON content.content_id = classification.content_id
        JOIN genre ON classification.genre_id = genre.genre_id
        WHERE genre.designation <> 'Action-Adventure' 
        AND (content.title LIKE '%!%' OR content.title LIKE '%?%' OR content.title LIKE '%,%'
             OR content.title LIKE '%.%' OR content.title LIKE '%"%' OR content.title LIKE "%'%"
             OR content.title LIKE '%:%' OR content.title LIKE '%;%');
    ''',
    5: '''
        SELECT DISTINCT content.title, country.name as country, content.duration
        FROM content 
        JOIN type ON content.type_id = type.type_id
        JOIN made_in ON content.content_id = made_in.content_id
        JOIN country ON made_in.country_id = country.country_id
        WHERE country.name <> 'United States' 
        AND type.designation = 'TV Show' 
        AND content.duration > 1;
    ''',
    6: '''
        SELECT DISTINCT content.content_id, content.title, director.name as director, actor.name as actor
        FROM content
        JOIN direction ON content.content_id = direction.content_id
        JOIN person as director ON direction.person_id = director.person_id
        JOIN c_cast ON content.content_id = c_cast.content_id
        JOIN person as actor ON c_cast.person_id = actor.person_id
        WHERE director.name LIKE 'J%'
        AND (actor.name LIKE '% S%' OR actor.name LIKE 'S%')
        ORDER BY content.content_id;
    ''',
    7: '''
        SELECT genre.designation as genre, content.release_date as year, COUNT(*) as total
        FROM content 
        JOIN type ON content.type_id = type.type_id
        JOIN classification ON content.content_id = classification.content_id
        JOIN genre ON classification.genre_id = genre.genre_id
        WHERE type.designation = 'Movie' 
        AND Content.duration > 100
        GROUP BY genre.designation, content.release_date
        ORDER BY content.release_date;
    ''',
    8: '''
        SELECT c.rating, COUNT(d.title) as total, MIN(d.date_added) as first_added, MAX(d.date_added) as last_added
        FROM (
            SELECT content.content_id as id, content.title, content.date_added
            FROM content
            WHERE content.date_added >= '2021-03-01' AND content.date_added <= '2021-11-30'
        ) d
        JOIN (
            SELECT content.content_id as id, content.rating_id, rating.designation as rating
            FROM content 
            JOIN rating ON content.rating_id = rating.rating_id
        ) c ON d.id = c.id
        GROUP BY c.rating;
    ''',
    9: '''
        SELECT title
        FROM (
            SELECT content.content_id as id, content.title as title
            FROM content 
            JOIN c_cast ON content.content_id = c_cast.content_id
            JOIN person ON c_cast.person_id = person.person_id
            WHERE content.description NOT LIKE '%' || person.name || '%'
            GROUP BY id, title
            UNION
            SELECT content.content_id as id, content.title as title
            FROM content 
            JOIN c_cast ON content.content_id = c_cast.content_id
            JOIN person ON c_cast.person_id = person.person_id
            WHERE content.description LIKE '%' || content.title || '%'
            GROUP BY id, title
        )
        ORDER BY title;
    ''',
    10: '''
        SELECT (content.release_date / 10) * 10 as decade, COUNT(content.content_id) as total
        FROM content 
        JOIN type ON content.type_id = type.type_id
        WHERE type.designation = 'Movie' AND content.duration >= 30 AND content.duration <= 120
        GROUP BY decade
        ORDER BY decade;
    ''',
    11: '''
        SELECT director.name as director, actor.name as actor, COUNT(*) as collaboration_count, GROUP_CONCAT(content.title, ', ') as movies
        FROM direction
        JOIN person as director ON direction.person_id = director.person_id
        JOIN c_cast ON direction.content_id = c_cast.content_id
        JOIN person as actor ON c_cast.person_id = actor.person_id
        JOIN content ON c_cast.content_id = content.content_id
        GROUP BY director.name, actor.name
        HAVING collaboration_count > 1
        ORDER BY collaboration_count DESC;
    '''
}
# ----------------------------

#____Definir Endpoints____

@app.route('/')
def index():
    tables = get_table_names() 
    stats = get_db_stats()
    query_endpoints = [
        ('1. Ordenar todos os filmes de animação lançados entre 2000 e 2020, por ordem lexicográfica.', 'query_1'),
        ('2. Número de descrições que contêm números, com base no tipo de conteúdo', 'query_2'),
        ('3. número de filmes e o número de séries dirigidas por cada diretor.', 'query_3'),
        ('4. Obras, não de ação-aventura, cujo título contém um sinal de pontuação.', 'query_4'),
        ('5. Séries não realizadas nos Estados Unidos, com mais de 1 temporada.', 'query_5'),
        ('6. Obras dirigidas por diretor cujo nome começa por \'J\' e com atores cujo nome começa por \'S\'.', 'query_6'),
        ('7. Número de filmes com duração superior a 100 minutos, agrupados por género e ordenados pelo ano.', 'query_7'),
        ('8. Número de obras adicionadas entre março e novembro de 2021, agrupadas por rating.', 'query_8'),
        ('9. Obras cuja descrição contém o título OU não contém nenhum membro do elenco.', 'query_9'),
        ('10. Filmes com duração entre 30 e 120 minutos agrupados por década.', 'query_10'),
        ('11. Diretores e atores que colaboraram mais de uma vez.', 'query_11')
    ]
    return render_template('index.html', tables=tables, queries=query_endpoints, stats=stats)

def get_table_pk(table_name):
    try:
        cur = get_db().execute(f"PRAGMA table_info({table_name})")
        columns = cur.fetchall()
        cur.close()
        for col in columns:
            if col['pk'] == 1:
                return col['name']
        for col in columns:
            if col['name'].lower() == 'id':
                return 'id'
        possible_pk = f"{table_name}_id"
        for col in columns:
            if col['name'].lower() == possible_pk:
                return possible_pk
        return None
    except sqlite3.Error:
        return None

@app.route('/<table_name>/')
def list_records(table_name):
    try:
        pk = get_table_pk(table_name)
        if not re.match("^[a-zA-Z0-9_]+$", table_name):
            abort(400, description="Nome da tabela inválido.")
        rows = query_db(f"SELECT * FROM {table_name}")
        rows_as_dict = [dict(row) for row in rows]
        return_section = "colunas"
        return render_template('table_list.html', table=table_name, rows=rows_as_dict, pk=pk, return_section=return_section)
    except sqlite3.Error as e:
        print(f"ERRO DE BD: Falha ao listar registos da tabela {table_name}: {e}")
        abort(404, description=f"Tabela '{table_name}' não encontrada ou erro de base de dados.")

@app.route('/<table_name>/<pk_value>/')
def record_detail(table_name, pk_value):
    pk = get_table_pk(table_name)
    if not pk:
        abort(404, description=f"Chave primária não encontrada para a tabela '{table_name}'.")
    try:
        sql_query = f"SELECT * FROM {table_name} WHERE {pk} = ?"
        record = query_db(sql_query, [pk_value], one=True)
        if record is None:
            abort(404, description="Registo não encontrado.")
        record_dict = dict(record)
        return render_template('record_detail.html', table=table_name, record=record_dict)
    except sqlite3.Error as e:
        print(f"Erro ao buscar detalhes do registo na tabela {table_name}: {e}")
        abort(500, description="Erro interno da base de dados.")

# ---- Endpoints Queries 1 a 11 (com query_number e question_text) ----

@app.route('/query_1/')
def query_1():
    sql_query = SQL_QUERIES[1]
    results = query_db(sql_query)
    rows_as_dict = [dict(row) for row in results]
    column_names = rows_as_dict[0].keys() if rows_as_dict else ['type', 'total']
    return render_template('query_result.html', title="Disney+ Database", results=rows_as_dict, column_names=column_names, query_number=1, question_text=QUESTIONS[1], sql_code=SQL_QUERIES[1])

@app.route('/query_2/')
def query_2():
    sql_query = SQL_QUERIES[2]
    results = query_db(sql_query)
    rows_as_dict = [dict(row) for row in results]
    column_names = rows_as_dict[0].keys() if rows_as_dict else ['year', 'total']
    return render_template('query_result.html', title="Disney+ Database", results=rows_as_dict, column_names=column_names, query_number=2, question_text=QUESTIONS[2], sql_code=SQL_QUERIES[2])

@app.route('/query_3/')
def query_3():
    sql_query = SQL_QUERIES[3]
    results = query_db(sql_query)
    rows_as_dict = [dict(row) for row in results]
    column_names = rows_as_dict[0].keys() if rows_as_dict else ['title', 'year', 'duration', 'genre']
    return render_template('query_result.html', title="Disney+ Database", results=rows_as_dict, column_names=column_names, query_number=3, question_text=QUESTIONS[3], sql_code=SQL_QUERIES[3])

@app.route('/query_4/')
def query_4():
    sql_query = SQL_QUERIES[4]
    results = query_db(sql_query)
    rows_as_dict = [dict(row) for row in results]
    column_names = rows_as_dict[0].keys() if rows_as_dict else ['name', 'title', 'role']
    return render_template('query_result.html', title="Disney+ Database", results=rows_as_dict, column_names=column_names, query_number=4, question_text=QUESTIONS[4], sql_code=SQL_QUERIES[4])

@app.route('/query_5/')
def query_5():
    sql_query = SQL_QUERIES[5]
    results = query_db(sql_query)
    rows_as_dict = [dict(row) for row in results]
    column_names = rows_as_dict[0].keys() if rows_as_dict else ['name', 'total_appearances']
    return render_template('query_result.html', title="Disney+ Database", results=rows_as_dict, column_names=column_names, query_number=5, question_text=QUESTIONS[5], sql_code=SQL_QUERIES[5])

@app.route('/query_6/')
def query_6():
    sql_query = SQL_QUERIES[6]
    results = query_db(sql_query)
    rows_as_dict = [dict(row) for row in results]
    column_names = rows_as_dict[0].keys() if rows_as_dict else ['name', 'total_content']
    return render_template('query_result.html', title="Disney+ Database", results=rows_as_dict, column_names=column_names, query_number=6, question_text=QUESTIONS[6], sql_code=SQL_QUERIES[6])

@app.route('/query_7/')
def query_7():
    sql_query = SQL_QUERIES[7]
    results = query_db(sql_query)
    rows_as_dict = [dict(row) for row in results]
    column_names = rows_as_dict[0].keys() if rows_as_dict else ['name', 'total_content']
    return render_template('query_result.html', title="Disney+ Database", results=rows_as_dict, column_names=column_names, query_number=7, question_text=QUESTIONS[7], sql_code=SQL_QUERIES[7])

@app.route('/query_8/')
def query_8():
    sql_query = SQL_QUERIES[8]
    results = query_db(sql_query)
    rows_as_dict = [dict(row) for row in results]
    column_names = rows_as_dict[0].keys() if rows_as_dict else ['title', 'year', 'description']
    return render_template('query_result.html', title="Disney+ Database", results=rows_as_dict, column_names=column_names, query_number=8, question_text=QUESTIONS[8], sql_code=SQL_QUERIES[8])

@app.route('/query_9/')
def query_9():
    sql_query = SQL_QUERIES[9]
    results = query_db(sql_query)
    rows_as_dict = [dict(row) for row in results]
    column_names = rows_as_dict[0].keys() if rows_as_dict else ['type', 'average_duration']
    return render_template('query_result.html', title="Disney+ Database", results=rows_as_dict, column_names=column_names, query_number=9, question_text=QUESTIONS[9], sql_code=SQL_QUERIES[9])

@app.route('/query_10/')
def query_10():
    sql_query = SQL_QUERIES[10]
    results = query_db(sql_query)
    rows_as_dict = [dict(row) for row in results]
    column_names = rows_as_dict[0].keys() if rows_as_dict else ['decade', 'total_added']
    return render_template('query_result.html', title="Disney+ Database", results=rows_as_dict, column_names=column_names, query_number=10, question_text=QUESTIONS[10], sql_code=SQL_QUERIES[10])

@app.route('/query_11/')
def query_11():
    sql_query = SQL_QUERIES[11]
    results = query_db(sql_query)
    rows_as_dict = [dict(row) for row in results]
    column_names = rows_as_dict[0].keys() if rows_as_dict else ['director', 'actor', 'collaboration_count', 'movies']
    return render_template('query_result.html', title="Disney+ Database", results=rows_as_dict, column_names=column_names, query_number=11, question_text=QUESTIONS[11], sql_code=SQL_QUERIES[11])


if __name__ == '__main__':
    app.run(debug=True)