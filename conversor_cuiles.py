import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pyodbc
import zipfile
import tempfile
import shutil
import sqlite3
import time
import msaccessdb
import threading

import platform

class DatabaseEngineError(Exception):
    """Excepción personalizada para cuando falta el motor de base de datos de Access."""
    pass

class ConversorCuiles:
    def __init__(self, root):
        self.root = root
        self.root.title("Conversor de CUILES y Periodos")
        self.root.geometry("600x450")
        
        # Comprobar y mostrar la arquitectura de Python
        py_arch = platform.architecture()[0]
        self.root.title(f"Conversor de CUILES y Periodos (Python {py_arch})")

        # Establecer nombre predeterminado para el archivo de destino en una ruta simple
        temp_db_dir = "C:\\temp_db"
        if not os.path.exists(temp_db_dir):
            os.makedirs(temp_db_dir)
        self.default_output_path = os.path.join(temp_db_dir, "cordobaAux.mdb")
        
        # Configuración de la interfaz
        self.setup_ui()
    
    def setup_ui(self):
        # Frame principal
        main_frame = tk.Frame(self.root, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        title_label = tk.Label(main_frame, text="Conversor de CUILES", font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Selección de archivo origen (CUILES)
        source_cuiles_frame = tk.Frame(main_frame)
        source_cuiles_frame.pack(fill=tk.X, pady=5)
        
        source_cuiles_label = tk.Label(source_cuiles_frame, text="Archivo CUILES (.odb, .accdb):", width=25, anchor="w")
        source_cuiles_label.pack(side=tk.LEFT)
        
        self.source_cuiles_entry = tk.Entry(source_cuiles_frame)
        self.source_cuiles_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        source_cuiles_button = tk.Button(source_cuiles_frame, text="Buscar", command=lambda: self.select_source_file('cuiles'))
        source_cuiles_button.pack(side=tk.RIGHT)

        # Selección de archivo origen (PERIODOS)
        source_periodos_frame = tk.Frame(main_frame)
        source_periodos_frame.pack(fill=tk.X, pady=5)
        
        source_periodos_label = tk.Label(source_periodos_frame, text="Archivo PERIODOS (.odb, .accdb):", width=25, anchor="w")
        source_periodos_label.pack(side=tk.LEFT)
        
        self.source_periodos_entry = tk.Entry(source_periodos_frame)
        self.source_periodos_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        source_periodos_button = tk.Button(source_periodos_frame, text="Buscar", command=lambda: self.select_source_file('periodos'))
        source_periodos_button.pack(side=tk.RIGHT)
        
        # Selección de archivo destino
        dest_frame = tk.Frame(main_frame)
        dest_frame.pack(fill=tk.X, pady=5)
        
        dest_label = tk.Label(dest_frame, text="Archivo destino (.mdb):", width=20, anchor="w")
        dest_label.pack(side=tk.LEFT)
        
        self.dest_entry = tk.Entry(dest_frame)
        self.dest_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        self.dest_entry.insert(0, self.default_output_path)  # Establecer valor predeterminado
        
        dest_button = tk.Button(dest_frame, text="Buscar", command=self.select_dest_file)
        dest_button.pack(side=tk.RIGHT)
        
        # Barra de progreso
        progress_frame = tk.Frame(main_frame)
        progress_frame.pack(fill=tk.X, pady=10)
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill=tk.X)
        
        # Botón de conversión
        self.convert_button = tk.Button(main_frame, text="Convertir", command=self.start_conversion_thread, bg="#4CAF50", fg="white", font=("Arial", 12, "bold"), padx=20, pady=10)
        self.convert_button.pack(pady=20)
        
        # Barra de estado
        self.status_var = tk.StringVar()
        self.status_var.set("Listo para convertir")
        status_label = tk.Label(main_frame, textvariable=self.status_var, bd=1, relief=tk.SUNKEN, anchor=tk.W)
        status_label.pack(side=tk.BOTTOM, fill=tk.X)
    
    def select_source_file(self, file_type):
        file_path = filedialog.askopenfilename(
            title=f"Seleccionar archivo {file_type.upper()}",
            filetypes=[("Bases de datos", "*.odb;*.accdb"), ("LibreOffice Base", "*.odb"), ("Microsoft Access", "*.accdb")]
        )
        if file_path:
            if file_type == 'cuiles':
                self.source_cuiles_entry.delete(0, tk.END)
                self.source_cuiles_entry.insert(0, file_path)
            elif file_type == 'periodos':
                self.source_periodos_entry.delete(0, tk.END)
                self.source_periodos_entry.insert(0, file_path)
    
    def select_dest_file(self):
        initial_dir = os.path.dirname(self.default_output_path)
        initial_file = os.path.basename(self.default_output_path)
        
        file_path = filedialog.asksaveasfilename(
            title="Guardar archivo destino",
            filetypes=[("Microsoft Access 2003", "*.mdb")],
            defaultextension=".mdb",
            initialdir=initial_dir,
            initialfile=initial_file
        )
        if file_path:
            self.dest_entry.delete(0, tk.END)
            self.dest_entry.insert(0, file_path)
    
    def start_conversion_thread(self):
        self.convert_button.config(state=tk.DISABLED)
        conversion_thread = threading.Thread(target=self.convert_database)
        conversion_thread.start()

    def convert_database(self):
        source_cuiles_file = self.source_cuiles_entry.get()
        source_periodos_file = self.source_periodos_entry.get()
        dest_file = self.dest_entry.get()

        if not dest_file:
            messagebox.showerror("Error", "Por favor, especifique un archivo de destino.")
            return

        if not source_cuiles_file and not source_periodos_file:
            messagebox.showerror("Error", "Por favor, seleccione al menos un archivo de origen (CUILES o PERIODOS).")
            return

        try:
            self.progress_var.set(0)
            self.status_var.set("Iniciando conversión...")
            self.root.update()

            self.create_access_database(dest_file)
            self.progress_var.set(10)
            self.root.update()

            try:
                access_conn = pyodbc.connect(f'DRIVER={{Microsoft Access Driver (*.mdb)}};DBQ={dest_file};')
                access_cursor = access_conn.cursor()
            except pyodbc.Error:
                access_conn = pyodbc.connect(f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={dest_file};')
                access_cursor = access_conn.cursor()
            self.progress_var.set(20)
            self.root.update()

            if source_cuiles_file:
                self.status_var.set("Procesando CUILES...")
                self.create_cuiles_table_structure(access_cursor)
                self.progress_var.set(30)
                self.root.update()
                self.extract_and_convert_cuiles_data(source_cuiles_file, access_cursor)

            if source_periodos_file:
                self.status_var.set("Procesando PERIODOS...")
                self.create_periodos_table_structure(access_cursor)
                self.progress_var.set(60)
                self.root.update()
                self.extract_and_convert_periodos_data(source_periodos_file, access_cursor)

            access_conn.commit()
            access_cursor.close()
            access_conn.close()

            self.status_var.set("Conversión completada con éxito")
            self.progress_var.set(100)
            self.root.update()
            messagebox.showinfo("Éxito", "La conversión se ha completado correctamente")
            self.convert_button.config(state=tk.NORMAL)

        except DatabaseEngineError:
            self.status_var.set("Error: Falta el motor de base de datos.")
            self.progress_var.set(0)
            self.root.update()
            messagebox.showerror(
                "Error Crítico: Falta el Motor de Base de Datos de Access",
                "El programa no puede crear la base de datos porque falta el 'Microsoft Access Database Engine'.\n\n"
                "Este es un componente gratuito de Microsoft y es necesario para que este programa funcione.\n\n"
                "Por favor, instale la versión de 64 bits desde el sitio web de Microsoft e intente de nuevo."
            )
            self.root.quit()
        except Exception as e:
            self.status_var.set(f"Error: {str(e)}")
            self.progress_var.set(0)
            self.root.update()
            messagebox.showerror("Error", f"Se produjo un error durante la conversión: {str(e)}")
    
    def create_access_database(self, dest_file):
        # Crear una nueva base de datos Access
        if os.path.exists(dest_file):
            os.remove(dest_file)
        
        try:
            msaccessdb.create(dest_file)
        except Exception as e:
            # Comprobación específica para el error de 'motor de base de datos no encontrado'
            if "No se pudo encontrar el archivo" in str(e) or "-1028" in str(e) or "no reconoce este tipo de base de datos" in str(e):
                raise DatabaseEngineError from e
            
            # Para cualquier otro error, simplemente relanzar.
            raise e
    
    def create_cuiles_table_structure(self, cursor):
        # Crear la tabla con la estructura requerida
        cursor.execute("""
        CREATE TABLE cuiles (
            CUIT TEXT,
            ANIO TEXT,
            CUIL TEXT,
            REMUNERACION1 DOUBLE,
            APORTE1 DOUBLE,
            REMUNERACION2 DOUBLE,
            APORTE2 DOUBLE,
            REMUNERACION3 DOUBLE,
            APORTE3 DOUBLE,
            REMUNERACION4 DOUBLE,
            APORTE4 DOUBLE,
            REMUNERACION5 DOUBLE,
            APORTE5 DOUBLE,
            REMUNERACION6 DOUBLE,
            APORTE6 DOUBLE,
            REMUNERACION7 DOUBLE,
            APORTE7 DOUBLE,
            REMUNERACION8 DOUBLE,
            APORTE8 DOUBLE,
            REMUNERACION9 DOUBLE,
            APORTE9 DOUBLE,
            REMUNERACION10 DOUBLE,
            APORTE10 DOUBLE,
            REMUNERACION11 DOUBLE,
            APORTE11 DOUBLE,
            REMUNERACION12 DOUBLE,
            APORTE12 DOUBLE,
            tipo TEXT,
            PRIMARY KEY (CUIT, ANIO, CUIL)
        )
        """)
    
    def extract_and_convert_cuiles_data(self, source_file, access_cursor):
        # Mapeo de campos
        field_mapping = {
            'REMUNERACION_ENERO': 'REMUNERACION1',
            'APORTE_ENERO': 'APORTE1',
            'REMUNERACION_FEBRERO': 'REMUNERACION2',
            'APORTE_FEBRERO': 'APORTE2',
            'REMUNERACION_MARZO': 'REMUNERACION3',
            'APORTE_MARZO': 'APORTE3',
            'REMUNERACION_ABRIL': 'REMUNERACION4',
            'APORTE_ABRIL': 'APORTE4',
            'REMUNERACION_MAYO': 'REMUNERACION5',
            'APORTE_MAYO': 'APORTE5',
            'REMUNERACION_JUNIO': 'REMUNERACION6',
            'APORTE_JUNIO': 'APORTE6',
            'REMUNERACION_JULIO': 'REMUNERACION7',
            'APORTE_JULIO': 'APORTE7',
            'REMUNERACION_AGOSTO': 'REMUNERACION8',
            'APORTE_AGOSTO': 'APORTE8',
            'REMUNERACION_SEPTIEMBRE': 'REMUNERACION9',
            'APORTE_SEPTIEMBRE': 'APORTE9',
            'REMUNERACION_OCTUBRE': 'REMUNERACION10',
            'APORTE_OCTUBRE': 'APORTE10',
            'REMUNERACION_NOVIEMBRE': 'REMUNERACION11',
            'APORTE_NOVIEMBRE': 'APORTE11',
            'REMUNERACION_DICIEMBRE': 'REMUNERACION12',
            'APORTE_DICIEMBRE': 'APORTE12',
            'TIPO_BENEFICIARIO': 'tipo'
        }

        data = []
        
        # Determinar el tipo de archivo y leer los datos
        if source_file.endswith('.odb'):
            # Lógica para leer archivos .odb
            temp_dir = tempfile.mkdtemp()
            self.status_var.set("Extrayendo archivo ODB...")
            self.progress_var.set(10)
            self.root.update()
            
            try:
                with zipfile.ZipFile(source_file, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                db_path = os.path.join(temp_dir, "database", "data")
                if not os.path.exists(db_path):
                    raise Exception("No se pudo encontrar la base de datos en el archivo ODB")
                
                self.status_var.set("Conectando a la base de datos...")
                self.progress_var.set(30)
                self.root.update()
                
                # Conectar a la base de datos SQLite embebida
                sqlite_conn = sqlite3.connect(os.path.join(db_path, "script"))
                sqlite_cursor = sqlite_conn.cursor()
                
                self.status_var.set("Leyendo datos de la tabla...")
                self.progress_var.set(50)
                self.root.update()
                
                try:
                    sqlite_cursor.execute("SELECT * FROM \"VW_DIBENEF_ANNIO_AP_ADIC_DEL - CUILES 2015\"")
                except sqlite3.OperationalError:
                    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = sqlite_cursor.fetchall()
                    if not tables:
                        raise Exception("No se encontraron tablas en la base de datos")
                    table_name = tables[0][0]
                    sqlite_cursor.execute(f"SELECT * FROM \"{table_name}\"")

                columns = [column[0] for column in sqlite_cursor.description]
                records = sqlite_cursor.fetchall()

                for record in records:
                    data.append(dict(zip(columns, record)))
                
                sqlite_cursor.close()
                sqlite_conn.close()

            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)

        elif source_file.endswith('.accdb'):
            # Lógica para leer archivos .accdb
            self.status_var.set("Conectando a la base de datos Access...")
            self.progress_var.set(20)
            self.root.update()

            try:
                conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={source_file};'
                source_conn = pyodbc.connect(conn_str)
                source_cursor = source_conn.cursor()

                self.status_var.set("Leyendo datos de la tabla...")
                self.progress_var.set(50)
                self.root.update()

                # Asumimos que la tabla de interés es la primera que se encuentra
                table_name = source_cursor.tables(tableType='TABLE').fetchone()[2]
                source_cursor.execute(f"SELECT * FROM [{table_name}]")
                
                columns = [column[0] for column in source_cursor.description]
                records = source_cursor.fetchall()

                for record in records:
                    data.append(dict(zip(columns, [item for item in record])))

                source_cursor.close()
                source_conn.close()

            except pyodbc.Error as e:
                messagebox.showerror("Error de Conexión", f"No se pudo conectar a la base de datos de origen. Asegúrate de que el controlador ODBC de Microsoft Access esté instalado.\n\nError: {e}")
                return # Detener la ejecución si no se puede conectar

        # Procesar los registros
        self.status_var.set("Procesando datos...")
        self.progress_var.set(70)
        self.root.update()
        
        total_records = len(data)
        for i, record in enumerate(data):
            progress = 70 + (i / total_records * 20)
            self.progress_var.set(progress)
            self.status_var.set(f"Procesando registro {i+1} de {total_records}...")
            self.root.update()
            
            insert_data = {}
            
            for field in ['CUIT', 'ANIO', 'CUIL']:
                if field in record:
                    insert_data[field] = record[field]
            
            for source_field, dest_field in field_mapping.items():
                if source_field in record:
                    insert_data[dest_field] = record[source_field]
                else:
                    insert_data[dest_field] = 0
            
            fields = ', '.join(insert_data.keys())
            placeholders = ', '.join(['?' for _ in insert_data])
            values = list(insert_data.values())
            
            sql = f"INSERT INTO cuiles ({fields}) VALUES ({placeholders})"
            access_cursor.execute(sql, values)
        
        self.progress_var.set(95)
        self.root.update()

    def create_periodos_table_structure(self, cursor):
        """Crea la tabla 'periodos' en la base de datos de destino."""
        if not cursor.tables(table='periodos', tableType='TABLE').fetchone():
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
                intepago DOUBLE,
                PRIMARY KEY (CUIT, Mes)
            )
            """)
            cursor.commit()

    def extract_and_convert_periodos_data(self, source_file, access_cursor):
        """Transforma y carga los datos en la tabla 'periodos'."""
        access_cursor.execute("DELETE FROM periodos")
        access_cursor.commit()

        def process_record(record):
            cuit = record.get('CUIT')
            anio_raw = record.get('ANIO')
            if not cuit or not anio_raw:
                return

            anio = int(anio_raw)

            for month in range(1, 13):
                mes_str = f"{anio}-{month:02d}"

                # Construir el registro para insertar
                insert_data = {
                    'CUIT': cuit,
                    'Mes': mes_str,
                    'Aporte': record.get(f'APORTE_381_{month}'),
                    'Contribucion': record.get(f'CONTRIB_401_{month}'),
                    'Depo1': record.get(f'APORTE_Y_CONTR_{month}'),
                    'FeDepo1': record.get(f'FECHAPAGO_PAG_{month}'),
                    'Retencion': record.get(f'RETENCION_471_{month}'),
                    'Afiliados': record.get(f'BENEF_CANTPER_{month}'),
                    'CantMayor': record.get(f'BENEF_CANTPER_{month}'),
                    'Remuneracion': record.get(f'BENEF_NR_IMPREM_{month}'),
                    'RemuMayor': record.get(f'BENEF_NR_IMPREM_{month}'),
                    'CantMenor': 0,
                    'RemuMenor': 0.00,
                    'intepago': 0.00
                }

                # Insertar solo si hay al menos un valor no nulo para el mes (excluyendo CUIT y Mes)
                has_data = any(v is not None for k, v in insert_data.items() if k not in ['CUIT', 'Mes'])

                if has_data:
                    # Ordenar las columnas como en la tabla de destino
                    ordered_columns = ['CUIT', 'Mes', 'Afiliados', 'Remuneracion', 'Aporte', 'Contribucion', 'Depo1', 'FeDepo1', 'Retencion', 'CantMenor', 'RemuMenor', 'CantMayor', 'RemuMayor', 'intepago']
                    values = [insert_data.get(col) for col in ordered_columns]
                    placeholders = ', '.join(['?' for _ in ordered_columns])

                    sql = f"INSERT INTO periodos ({', '.join(ordered_columns)}) VALUES ({placeholders})"
                    access_cursor.execute(sql, values)

        batch_size = 1000
        count = 0

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
                
                for row in sqlite_cursor:
                    record = dict(zip(columns, row))
                    process_record(record)
                    count += 1
                    if count % batch_size == 0:
                        access_cursor.commit()

                access_cursor.commit()

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
            
            for row in source_cursor:
                record = dict(zip(columns, row))
                process_record(record)
                count += 1
                if count % batch_size == 0:
                    access_cursor.commit()
            
            access_cursor.commit()

            source_cursor.close()
            source_conn.close()


def main():
    root = tk.Tk()
    app = ConversorCuiles(root)
    root.mainloop()

if __name__ == "__main__":
    main()