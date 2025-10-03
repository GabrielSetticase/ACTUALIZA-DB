import os
import pyodbc
import zipfile
import tempfile
import shutil
import sqlite3

def get_source_data(source_file):
    """Extrae datos de un archivo .odb o .accdb."""
    data = []
    columns = []

    if source_file.endswith('.odb'):
        temp_dir = tempfile.mkdtemp()
        try:
            with zipfile.ZipFile(source_file, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            db_path = os.path.join(temp_dir, "database", "data")
            if not os.path.exists(db_path):
                raise Exception("No se pudo encontrar la base de datos en el archivo ODB")

            sqlite_conn = sqlite3.connect(os.path.join(db_path, "script"))
            sqlite_cursor = sqlite_conn.cursor()

            # Encuentra la primera tabla de usuario
            sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = sqlite_cursor.fetchall()
            if not tables:
                raise Exception("No se encontraron tablas en la base de datos ODB")
            table_name = tables[0][0]
            
            sqlite_cursor.execute(f'SELECT * FROM "{table_name}"')
            columns = [desc[0] for desc in sqlite_cursor.description]
            records = sqlite_cursor.fetchall()
            for record in records:
                data.append(dict(zip(columns, record)))
            
            sqlite_cursor.close()
            sqlite_conn.close()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    elif source_file.endswith('.accdb'):
        conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={source_file};'
        source_conn = pyodbc.connect(conn_str)
        source_cursor = source_conn.cursor()

        table_name = source_cursor.tables(tableType='TABLE').fetchone()[2]
        source_cursor.execute(f'SELECT * FROM [{table_name}]')
        
        columns = [desc[0] for desc in source_cursor.description]
        records = source_cursor.fetchall()
        for record in records:
            data.append(dict(zip(columns, [item for item in record])))

        source_cursor.close()
        source_conn.close()
    
    return data

def create_periodos_table(cursor):
    """Crea la tabla 'periodos' en la base de datos de destino."""
    if not table_exists(cursor, 'periodos'):
        cursor.execute("""
        CREATE TABLE periodos (
            CUIT TEXT(11),
            Mes TEXT(7),
            Afiliados LONG,
            Remuneracion DOUBLE,
            Aporte DOUBLE,
            Contribucion DOUBLE,
            Depo1 DOUBLE,
            FeDepo1 DATETIME,
            Retencion DOUBLE,
            CantMenor LONG,
            RemuMenor DOUBLE,
            CantMayor LONG,
            RemuMayor DOUBLE,
            intepago DOUBLE
        )
        """)
        cursor.commit()

def process_and_insert_periodos(access_cursor, source_data):
    """Transforma y carga los datos en la tabla 'periodos'."""
    # Mapeo de prefijos de campo a campos de destino
    field_prefix_map = {
        'APORTE_381_': 'Aporte',
        'CONTRIB_401_': 'Contribucion',
        'APORTE_Y_CONTR_': 'Depo1',
        'FECHAPAGO_PAG_': 'FeDepo1',
        'RETENCION_471_': 'Retencion',
        'BENEF_CANTPER_': 'Afiliados', # También se usa para CantMayor
        'BENEF_NR_IMPREM_': 'Remuneracion' # También se usa para RemuMayor
    }

    for record in source_data:
        cuit = record.get('CUIT')
        anio = record.get('ANIO')
        if not cuit or not anio:
            continue

        for month in range(1, 13):
            mes_str = f"{anio}-{month:02d}"
            
            # Construir el registro para insertar
            insert_data = {
                'CUIT': cuit,
                'Mes': mes_str,
                'CantMenor': 0,
                'RemuMenor': 0.0,
                'intepago': 0.0
            }

            is_valid_month = False
            for prefix, dest_field in field_prefix_map.items():
                source_field_name = f"{prefix}{month}"
                if source_field_name in record and record[source_field_name] is not None:
                    is_valid_month = True
                    value = record[source_field_name]
                    insert_data[dest_field] = value

                    # Duplicar valores según la lógica
                    if dest_field == 'Afiliados':
                        insert_data['CantMayor'] = value
                    elif dest_field == 'Remuneracion':
                        insert_data['RemuMayor'] = value
            
            # Insertar solo si se encontró al menos un dato para el mes
            if is_valid_month:
                # Asegurarse de que todos los campos tengan un valor por defecto
                for dest_field in ['Aporte', 'Contribucion', 'Depo1', 'FeDepo1', 'Retencion', 'Afiliados', 'CantMayor', 'Remuneracion', 'RemuMayor']:
                    if dest_field not in insert_data:
                        insert_data[dest_field] = 0 if dest_field not in ['FeDepo1'] else None

                # Ordenar las columnas como en la tabla de destino
                ordered_columns = ['CUIT', 'Mes', 'Afiliados', 'Remuneracion', 'Aporte', 'Contribucion', 'Depo1', 'FeDepo1', 'Retencion', 'CantMenor', 'RemuMenor', 'CantMayor', 'RemuMayor', 'intepago']
                values = [insert_data.get(col) for col in ordered_columns]
                placeholders = ', '.join(['?' for _ in ordered_columns])

                sql = f"INSERT INTO periodos ({', '.join(ordered_columns)}) VALUES ({placeholders})"
                access_cursor.execute(sql, values)

def table_exists(cursor, table_name):
    """Verifica si una tabla ya existe en la base de datos."""
    try:
        cursor.tables(table=table_name, tableType='TABLE').fetchone()
        return True
    except pyodbc.Error:
        return False
    return False
