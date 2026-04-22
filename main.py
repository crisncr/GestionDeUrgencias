from fastapi import FastAPI, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import pandas as pd
import io
import os
import gc
from typing import List, Optional
import numpy as np
import logging

# Registro de actividad
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

# Base de datos en memoria
CURRENT_DF = None

@app.get("/health")
def health():
    return {"status": "alive"}

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    global CURRENT_DF
    logger.info(f"--- SUBIDA: {file.filename} ---")
    
    try:
        # 1. Leer contenido
        content = await file.read()
        logger.info(f"Tamaño del archivo: {len(content) / 1024 / 1024:.2f} MB")
        
        # 2. Leer con motor optimizado (Calamine es mucho más ligero que Openpyxl)
        # Usamos engine='calamine' si está disponible, o el estándar con optimización
        try:
            df = pd.read_excel(io.BytesIO(content), engine='calamine')
        except:
            logger.info("Calamine no disponible o falló, usando openpyxl...")
            df = pd.read_excel(io.BytesIO(content), engine='openpyxl')
        
        # 3. Liberar la memoria del contenido original inmediatamente
        del content
        gc.collect()
        
        # 4. Procesar columnas
        df.columns = [str(c).strip() for c in df.columns]
        cols_map = {c.upper(): c for c in df.columns}
        if 'HOSPITAL' in cols_map: df.rename(columns={cols_map['HOSPITAL']: 'HOSPITAL'}, inplace=True)
        if 'AÑO' in cols_map: df.rename(columns={cols_map['AÑO']: 'AÑO'}, inplace=True)
        if 'MES' in cols_map: df.rename(columns={cols_map['MES']: 'MES'}, inplace=True)

        if not {'HOSPITAL', 'AÑO', 'MES'}.issubset(df.columns):
            return {"error": "El Excel debe tener las columnas: HOSPITAL, AÑO y MES"}
        
        # 5. Optimizar tipos de datos para ahorrar RAM
        df['HOSPITAL'] = df['HOSPITAL'].astype('category')
        if 'AÑO' in df.columns: df['AÑO'] = pd.to_numeric(df['AÑO'], downcast='integer')
        if 'MES' in df.columns: df['MES'] = pd.to_numeric(df['MES'], downcast='integer')
        
        CURRENT_DF = df
        
        hospitals = sorted(df['HOSPITAL'].unique().tolist())
        years = sorted([int(y) for y in df['AÑO'].unique() if pd.notnull(y)])
        
        indicators = []
        for col in df.columns:
            if col.upper() not in {'HOSPITAL', 'AÑO', 'MES', 'COD_HOSPITAL', 'ID'}:
                if pd.api.types.is_numeric_dtype(df[col]):
                    indicators.append(col)
        
        logger.info(f"Carga exitosa: {len(hospitals)} hospitales.")
        return {
            "hospitals": hospitals,
            "years": years,
            "available_indicators": indicators
        }
        
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        return {"error": f"Error de memoria: {str(e)}. Intenta con un archivo más pequeño."}

@app.get("/api/analysis")
async def get_analysis(
    hospital: str, 
    year: int, 
    indicators: List[str] = Query(...),
    month: Optional[int] = None
):
    global CURRENT_DF
    if CURRENT_DF is None:
        return {"error": "Sube el archivo primero"}
    
    try:
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
            
            monthly_breakdown.append({'month': int(m), 'stats': m_stats, 'peaks': m_peaks})
        
        return {"indicators": results, "monthly_breakdown": monthly_breakdown}
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        return {"error": str(e)}

app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
