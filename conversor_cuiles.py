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

import platform

class DatabaseEngineError(Exception):
    """Excepción personalizada para cuando falta el motor de base de datos de Access."""
    pass

class ConversorCuiles:
    def __init__(self, root):
        self.root = root
        self.root.title("Conversor de CUILES")
        self.root.geometry("600x400")
        
        # Comprobar y mostrar la arquitectura de Python
        py_arch = platform.architecture()[0]
        self.root.title(f"Conversor de CUILES (Python {py_arch})")

        # Establecer nombre predeterminado para el archivo de destino en una ruta simple
        temp_db_dir = "C:\\temp_db"
        if not os.path.exists(temp_db_dir):
            os.makedirs(temp_db_dir)
        self.default_output_path = os.path.join(temp_db_dir, "cuiles.mdb")
        
        # Configuración de la interfaz
        self.setup_ui()
    
    def setup_ui(self):
        # Frame principal
        main_frame = tk.Frame(self.root, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        title_label = tk.Label(main_frame, text="Conversor de CUILES", font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Selección de archivo origen
        source_frame = tk.Frame(main_frame)
        source_frame.pack(fill=tk.X, pady=5)
        
        source_label = tk.Label(source_frame, text="Archivo origen (.odb, .accdb):", width=20, anchor="w")
        source_label.pack(side=tk.LEFT)
        
        self.source_entry = tk.Entry(source_frame)
        self.source_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        source_button = tk.Button(source_frame, text="Buscar", command=self.select_source_file)
        source_button.pack(side=tk.RIGHT)
        
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
        convert_button = tk.Button(main_frame, text="Convertir", command=self.convert_database, bg="#4CAF50", fg="white", font=("Arial", 12, "bold"), padx=20, pady=10)
        convert_button.pack(pady=20)
        
        # Barra de estado
        self.status_var = tk.StringVar()
        self.status_var.set("Listo para convertir")
        status_label = tk.Label(main_frame, textvariable=self.status_var, bd=1, relief=tk.SUNKEN, anchor=tk.W)
        status_label.pack(side=tk.BOTTOM, fill=tk.X)
    
    def select_source_file(self):
        file_path = filedialog.askopenfilename(
            title="Seleccionar archivo origen",
            filetypes=[("Bases de datos", "*.odb;*.accdb"), ("LibreOffice Base", "*.odb"), ("Microsoft Access", "*.accdb")]
        )
        if file_path:
            self.source_entry.delete(0, tk.END)
            self.source_entry.insert(0, file_path)
    
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
    
    def convert_database(self):
        source_file = self.source_entry.get()
        dest_file = self.dest_entry.get()
        
        if not source_file or not dest_file:
            messagebox.showerror("Error", "Por favor, seleccione los archivos de origen y destino")
            return
        
        try:
            # Inicializar la barra de progreso
            self.progress_var.set(0)
            self.status_var.set("Iniciando conversión...")
            self.root.update()
            
            # Crear una nueva base de datos Access
            self.create_access_database(dest_file)
            self.progress_var.set(10)
            self.root.update()
            
            # Conectar a la base de datos Access
            try:
                # Intentar con el controlador para Access 2003 (.mdb)
                access_conn = pyodbc.connect(f'DRIVER={{Microsoft Access Driver (*.mdb)}};DBQ={dest_file};')
                access_cursor = access_conn.cursor()
            except pyodbc.Error:
                # Intentar con el controlador general
                access_conn = pyodbc.connect(f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={dest_file};')
                access_cursor = access_conn.cursor()
            self.progress_var.set(20)
            self.root.update()
            
            # Crear la tabla en Access con la estructura requerida
            self.create_table_structure(access_cursor)
            self.progress_var.set(30)
            self.root.update()
            
            # Extraer y convertir los datos
            self.extract_and_convert_data(source_file, access_cursor)
            
            # Confirmar los cambios
            access_conn.commit()
            access_cursor.close()
            access_conn.close()
            
            self.status_var.set("Conversión completada con éxito")
            self.progress_var.set(100)
            self.root.update()
            messagebox.showinfo("Éxito", "La conversión se ha completado correctamente")
                
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
    
    def create_table_structure(self, cursor):
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
            tipo TEXT
        )
        """)
    
    def extract_and_convert_data(self, source_file, access_cursor):
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



def main():
    root = tk.Tk()
    app = ConversorCuiles(root)
    root.mainloop()

if __name__ == "__main__":
    main()