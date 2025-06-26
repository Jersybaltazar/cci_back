import pandas as pd
import mysql.connector
from datetime import datetime
import re

# Configuración de la conexión a la base de datos
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "plantas_db"
}

def sanitize_data(df):
    """Limpia y prepara los datos antes de la inserción"""
    # Convertir fechas al formato correcto
    for date_col in ['fecha_censo', 'fecha_actualizacion_sispa']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Convertir tipos numéricos correctamente
    numeric_cols = ['edad', 'area_total_declarada', 
                    'total_ha_sembrada', 'productividad_x_ha', 'jornales_por_ha']
    
    for col in numeric_cols:
        if col in df.columns:
            # Convertir a numérico con coerción (NaN para valores no numéricos)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # Reemplazar NaN con 0 para evitar nulls
            df[col] = df[col].fillna(0)

    # Convertir DNI a string para asegurar formato correcto
    if 'dni' in df.columns:
        df['dni'] = df['dni'].astype(str).str.strip()
        # Asegurar que el DNI tenga 8 dígitos (rellenar con ceros a la izquierda)
        df['dni'] = df['dni'].str.zfill(8)
    
    # Manejar específicamente los campos de cultivos
    cultivos_cols = ['esparrago', 'granada', 'maiz', 'palta', 'papa', 'pecano', 'vid']
    for col in cultivos_cols:
        if col in df.columns:
            # Convierte valores NA/vacíos/null a "NO"
            df[col] = df[col].apply(lambda x: "SÍ" if pd.notna(x) and str(x).strip() not in ["", "NO", "nan", "None"] else "NO")
    
    # Manejar específicamente senasa y sispa como SÍ/NO
    binary_cols = ['senasa', 'sispa']
    for col in binary_cols:
        if col in df.columns:
            # Convierte valores NA/vacíos/null a "NO", cualquier otro valor a "SÍ"
            df[col] = df[col].apply(lambda x: "SÍ" if pd.notna(x) and str(x).strip() not in ["", "NO", "nan", "None"] else "NO")
    
    # Manejar específicamente edad_cultivo (convertir a string "N años" o "N/A")
    # Manejar específicamente edad_cultivo (convertir a string "N años" o "N/A")
    if 'edad_cultivo' in df.columns:
        def formato_edad_cultivo(x):
            if pd.isna(x) or str(x).strip() in ["", "nan", "None", "#N/D", "#N/A"]:
                return "N/A"
            try:
                # Intenta convertir a número y formatear
                return f"{int(float(str(x).replace('O', '0')))} años"  # Reemplaza "O" por "0"
            except (ValueError, TypeError):
                # Si falla, devuelve el valor original o N/A
                return f"{str(x)} años" if str(x).strip() else "N/A"
                
        df['edad_cultivo'] = df['edad_cultivo'].apply(formato_edad_cultivo)
        
    # Dar formato al porcentaje de práctica sostenible
    if 'porcentaje_prac_economica_sost' in df.columns:
        df['porcentaje_prac_economica_sost'] = df['porcentaje_prac_economica_sost'].apply(
            lambda x: "0 - 25%" if pd.isna(x) or str(x).strip() in ["", "nan", "None"] else 
                     f"{int(float(x))}%" if pd.notna(x) and not isinstance(x, str) else x
        )
    
    # Limpiar strings (quitar espacios al inicio/final)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
    
    # Normalizar valores nulos (excepto los cultivos, senasa, sispa que ya procesamos)
    skip_cols = cultivos_cols + binary_cols + ['edad_cultivo', 'productividad_x_ha', 'jornales_por_ha', 'porcentaje_prac_economica_sost']
    non_special_cols = [col for col in df.columns if col not in skip_cols]
    df[non_special_cols] = df[non_special_cols].replace({pd.NA: None, pd.NaT: None, '': None, 'nan': None, 'NaN': None})

    return df
def main():
    # 1. Cargar el archivo Excel
    print("Cargando archivo Excel...")
    excel_path = "Base de datos PLANTAS IT4 v.13.19051223.xlsx"
    df = pd.read_excel(excel_path, sheet_name="Hoja1")
    print(f"Se cargaron {len(df)} filas desde Excel.")
    
    # 2. Mapeo de columnas (Excel → Base de datos)
    column_mapping = {
        "DNI": "dni",
        "FECHA DE CENSO": "fecha_censo",
        "APELLIDOS": "apellidos",
        "NOMBRE": "nombres",
        "NOMBRE COMPLETO": "nombre_completo",
        "SEXO": "sexo",
        "EDAD": "edad",
        "ESPARRAGO": "esparrago",
        "GRANADA": "granada",
        "MAIZ": "maiz",
        "PALTA": "palta",
        "PAPA": "papa",
        "PECANO": "pecano",
        "VID": "vid",
        "DPTO": "dpto",
        "PROVINCIA": "provincia",
        "DISTRITO": "distrito",
        "CENTRO POBLADO": "centro_poblado",
        "SENASA": "senasa",
        "SISPPA": "sispa",
        "CODIGO AUTOGENE SISPPA": "codigo_autogene_sispa",
        "REGIMEN DE TENENCIA SISPPA": "regimen_tenencia_sispa",
        "AREA TOTAL DECLARADA (ha) SISPPA": "area_total_declarada",
        "FECHA ACTUALIZACION SISPPA": "fecha_actualizacion_sispa",
        "TOMA": "toma",
        "EDAD CULTIVO": "edad_cultivo",
        "TOTAL Ha SEMBRADA": "total_ha_sembrada",
        "PRODUCTIVIDAD x Ha": "productividad_x_ha",
        "TIPO DE RIEGO": "tipo_riego",
        "NIVEL ALCANCE DE VENTA": "nivel_alcance_venta",
        "Nº JORNALES POR Ha": "jornales_por_ha",
        "PRACTICA ECONOMICA SOST": "practica_economica_sost",
        "% PRAC ECONOMICA SOST.": "porcentaje_prac_economica_sost"
    }
    
    # Renombrar columnas según el mapeo
    df = df.rename(columns=column_mapping)
    
    # 3. Definir todas las columnas necesarias en la base de datos
    required_columns = [
        'dni', 'fecha_censo', 'apellidos', 'nombres', 'nombre_completo',
        'sexo', 'edad', 'esparrago', 'granada', 'maiz', 'palta', 'papa',
        'pecano', 'vid', 'dpto', 'provincia', 'distrito', 'centro_poblado',
        'senasa', 'sispa', 'codigo_autogene_sispa', 'regimen_tenencia_sispa',
        'area_total_declarada', 'fecha_actualizacion_sispa', 'toma',
        'edad_cultivo', 'total_ha_sembrada', 'productividad_x_ha',
        'tipo_riego', 'nivel_alcance_venta', 'jornales_por_ha',
        'practica_economica_sost', 'porcentaje_prac_economica_sost'
    ]
    
    # 4. Añadir columnas faltantes
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
    
    # Verificar si hay DNIs duplicados en el Excel
    duplicados = df[df.duplicated(subset=['dni'], keep=False)]
    if not duplicados.empty:
        print(f"¡ADVERTENCIA! Se encontraron {len(duplicados)} filas con DNIs duplicados:")
        for dni in duplicados['dni'].unique():
            dup_rows = df[df['dni'] == dni]
            print(f"  DNI {dni} aparece {len(dup_rows)} veces")
        
        handle_dups = input("¿Cómo deseas manejar los duplicados? (s=saltar/k=mantener primero/u=actualizar): ")
        
        if handle_dups.lower() == 's':
            # Saltar duplicados (eliminarlos)
            df = df.drop_duplicates(subset=['dni'], keep=False)
            print(f"Se eliminaron todas las filas con DNIs duplicados. Quedan {len(df)} filas.")
        elif handle_dups.lower() == 'k':
            # Mantener solo la primera aparición
            df = df.drop_duplicates(subset=['dni'], keep='first')
            print(f"Se mantuvo solo la primera ocurrencia de cada DNI duplicado. Quedan {len(df)} filas.")
    
    # 5. Limpiar y preparar datos
    df = sanitize_data(df)
    
    # Verificar los valores de cultivos (para depuración)
    cultivos_cols = ['esparrago', 'granada', 'maiz', 'palta', 'papa', 'pecano', 'vid']
    cultivos_sample = df[cultivos_cols].head(3)
    print("\nEjemplo de valores de cultivos procesados:")
    print(cultivos_sample)
    
    # Validar datos críticos antes de la migración
    critical_fields = ['dni', 'apellidos', 'nombres', 'dpto', 'provincia', 'distrito']
    invalid_rows = []
    
    for i, row in df.iterrows():
        # Verificar campos críticos vacíos
        missing = [field for field in critical_fields if pd.isna(row[field])]
        if missing or (not pd.isna(row['dni']) and len(str(row['dni']).strip()) != 8):
            invalid_rows.append({
                'index': i,
                'dni': row['dni'] if 'dni' in row and pd.notna(row['dni']) else 'N/A',
                'issue': f"Campos vacíos: {missing}" if missing else "DNI inválido"
            })
    
    if invalid_rows:
        print(f"Se encontraron {len(invalid_rows)} filas con datos inválidos:")
        for i, row in enumerate(invalid_rows[:10]):  # Mostrar solo las primeras 10
            print(f"  {i+1}. Fila {row['index']}, DNI: {row['dni']}, Problema: {row['issue']}")
        
        if len(invalid_rows) > 10:
            print(f"  ... y {len(invalid_rows) - 10} más.")
            
        proceed = input("¿Deseas continuar con la migración? (s/n): ")
        if proceed.lower() != 's':
            print("Migración cancelada por el usuario")
            return
    
    # 6. Migrar a MySQL
    conn = None
    try:
        # Conectar a la base de datos
        print("Conectando a la base de datos...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Configurar consulta de inserción
        columns = ', '.join(required_columns)
        placeholders = ', '.join(['%s'] * len(required_columns))
        
        # Opción 2: Insertar actualizando si existe duplicado
        update_stmt = ', '.join([f"{col} = VALUES({col})" for col in required_columns if col != 'dni'])
        insert_query = f"""
        INSERT INTO agricultores ({columns}) 
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_stmt}
        """
        
        # Insertar datos (por lotes para mayor eficiencia)
        print("Migrando datos...")
        batch_size = 50
        total_rows = len(df)
        rows_inserted = 0
        rows_updated = 0
        errors = 0
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:min(i+batch_size, total_rows)]
            
            # Preparar lote de datos
            batch_data = []
            for _, row in batch.iterrows():
                # Convertir cada fila a una tupla en el orden correcto
                row_data = []
                for col in required_columns:
                    val = row[col] if col in row and pd.notna(row[col]) else None
                    row_data.append(val)
                batch_data.append(tuple(row_data))
            
            try:
                # Ejecutar la inserción por lotes
                cursor.executemany(insert_query, batch_data)
                
                # Contar filas insertadas y actualizadas
                if cursor.rowcount > 0:
                    # MySQL cuenta 2 por cada fila actualizada (1 delete + 1 insert)
                    # y 1 por cada inserción nueva
                    rows_affected = cursor.rowcount
                    if rows_affected > len(batch):
                        rows_updated += (rows_affected - len(batch)) // 2
                        rows_inserted += len(batch) - rows_updated
                    else:
                        rows_inserted += rows_affected
                
                # Mostrar progreso
                current_progress = min(i+batch_size, total_rows)
                print(f"Procesados {current_progress}/{total_rows} registros")
                
            except mysql.connector.Error as e:
                errors += 1
                print(f"Error en lote {i//batch_size + 1}: {e}")
                # Intentar insertar uno por uno para identificar el problema
                for j, row_data in enumerate(batch_data):
                    try:
                        cursor.execute(insert_query, row_data)
                    except mysql.connector.Error as e2:
                        print(f"  Error en registro #{i+j}: {e2}")
                        print(f"  Datos: DNI={row_data[0]}")
            
            # Confirmar cada lote
            conn.commit()
        
        # Resumen final
        print("\nMigración finalizada:")
        print(f"- Registros totales procesados: {total_rows}")
        print(f"- Registros insertados nuevos: {rows_inserted}")
        print(f"- Registros actualizados: {rows_updated}")
        print(f"- Lotes con errores: {errors}")
        
    except Exception as e:
        print(f"Error general: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexión cerrada.")

if __name__ == "__main__":
    main()