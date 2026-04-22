from fastapi import FastAPI, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import pandas as pd
import os
import shutil
from typing import List, Optional
import numpy as np
import logging

# Configuración de logs para diagnóstico en Render
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Almacenamiento global
CURRENT_DF = None

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    global CURRENT_DF
    logger.info(f"--- INICIO DE SUBIDA: {file.filename} ---")
    
    temp_path = "temp_render_data.xlsx"
    
    try:
        # GUARDADO EFICIENTE: Por trozos para no saturar la RAM
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        logger.info("Archivo guardado en disco. Iniciando lectura Pandas...")
        
        # LECTURA OPTIMIZADA: Solo leemos lo necesario
        df = pd.read_excel(temp_path, engine='openpyxl')
        
        # Limpieza inmediata de nombres de columnas
        df.columns = [str(c).strip() for c in df.columns]
        
        # Normalización de columnas críticas
        cols_map = {c.upper(): c for c in df.columns}
        if 'HOSPITAL' in cols_map: df.rename(columns={cols_map['HOSPITAL']: 'HOSPITAL'}, inplace=True)
        if 'AÑO' in cols_map: df.rename(columns={cols_map['AÑO']: 'AÑO'}, inplace=True)
        if 'MES' in cols_map: df.rename(columns={cols_map['MES']: 'MES'}, inplace=True)

        if not {'HOSPITAL', 'AÑO', 'MES'}.issubset(df.columns):
            logger.error("Error: Columnas HOSPITAL, AÑO o MES no encontradas.")
            return {"error": "El archivo debe tener las columnas HOSPITAL, AÑO y MES"}
        
        df['HOSPITAL'] = df['HOSPITAL'].astype(str).str.strip()
        CURRENT_DF = df
        
        hospitals = sorted([h for h in df['HOSPITAL'].unique() if str(h).lower() != 'nan'])
        years = sorted([int(y) for y in df['AÑO'].unique() if pd.notnull(y)])
        
        # Detectar indicadores numéricos
        exclude = {'HOSPITAL', 'AÑO', 'MES', 'COD_HOSPITAL', 'ID', 'COD_HOSP'}
        indicators = []
        for col in df.columns:
            if col.upper() not in exclude:
                if pd.api.types.is_numeric_dtype(df[col]):
                    indicators.append(col)
                else:
                    # Intentar convertir si no es numérico puro
                    try:
                        if pd.to_numeric(df[col], errors='coerce').notnull().any():
                            indicators.append(col)
                    except: continue
        
        logger.info(f"Carga completa: {len(hospitals)} hospitales, {len(indicators)} indicadores.")
        
        # Borrar archivo temporal para liberar espacio en Render
        if os.path.exists(temp_path):
            os.remove(temp_path)
            
        return {
            "hospitals": hospitals,
            "years": years,
            "available_indicators": indicators
        }
        
    except Exception as e:
        logger.error(f"FALLO CRÍTICO EN UPLOAD: {str(e)}")
        if os.path.exists(temp_path): os.remove(temp_path)
        return {"error": f"Error técnico: {str(e)}"}

@app.get("/api/analysis")
async def get_analysis(
    hospital: str, 
    year: int, 
    indicators: List[str] = Query(...),
    month: Optional[int] = None
):
    global CURRENT_DF
    if CURRENT_DF is None:
        return {"error": "Sube un archivo primero"}
    
    try:
        # Filtrado optimizado
        mask = (CURRENT_DF['HOSPITAL'].astype(str) == str(hospital)) & (CURRENT_DF['AÑO'].astype(float) == float(year))
        if month:
            mask = mask & (CURRENT_DF['MES'].astype(float) == float(month))
        
        filtered = CURRENT_DF[mask].copy()
        if filtered.empty:
            return {"indicators": {}, "monthly_breakdown": []}

        results = {}
        for col in indicators:
            if col in filtered.columns:
                vals = pd.to_numeric(filtered[col], errors='coerce').dropna()
                if not vals.empty:
                    results[col] = {
                        'mean': round(float(vals.mean()), 2),
                        'max': round(float(vals.max()), 2),
                        'min': round(float(vals.min()), 2),
                        'mode': round(float(vals.mode().iloc[0]), 2) if not vals.mode().empty else 0
                    }

        # Agregación mensual
        monthly_breakdown = []
        for m in sorted(filtered['MES'].unique()):
            m_df = filtered[filtered['MES'] == m]
            m_stats = {}
            m_peaks = {}
            for col in indicators:
                if col in m_df.columns:
                    col_vals = pd.to_numeric(m_df[col], errors='coerce').fillna(0)
                    total = col_vals.sum()
                    m_stats[col] = int(round(total)) if abs(total - round(total)) < 0.0001 else round(float(total), 2)
                    
                    peak = col_vals.max()
                    m_peaks[col] = int(round(peak)) if abs(peak - round(peak)) < 0.0001 else round(float(peak), 2)
            
            monthly_breakdown.append({
                'month': int(m),
                'stats': m_stats,
                'peaks': m_peaks
            })
        
        return {
            "indicators": results,
            "monthly_breakdown": monthly_breakdown
        }
    except Exception as e:
        logger.error(f"FALLO EN ANÁLISIS: {str(e)}")
        return {"error": str(e)}

# Servir frontend (Siempre al final)
app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
