# dashboard/queries.py
def get_categoria_count_query():
    return """
    SELECT categoria, COUNT(*) AS usuario_count
    FROM transformed_data
    GROUP BY categoria;
    """
