from fastapi import FastAPI, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import pandas as pd
import os
import json
from typing import List, Optional
import sys
import numpy as np
import logging

# Configurar logs para ver qué pasa en Render
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

# Global storage for the current dataframe
CURRENT_DF = None

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    global CURRENT_DF
    logger.info(f"Recibiendo archivo: {file.filename}")
    
    try:
        contents = await file.read()
        
        # Guardar en una ruta segura y temporal
        temp_path = "temp_upload.xlsx"
        with open(temp_path, "wb") as f:
            f.write(contents)
        
        logger.info("Archivo guardado, procediendo a leer con Pandas...")
        
        df = pd.read_excel(temp_path, engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]
        
        cols_upper = {c.upper(): c for c in df.columns}
        if 'HOSPITAL' in cols_upper: df.rename(columns={cols_upper['HOSPITAL']: 'HOSPITAL'}, inplace=True)
        if 'AÑO' in cols_upper: df.rename(columns={cols_upper['AÑO']: 'AÑO'}, inplace=True)
        if 'MES' in cols_upper: df.rename(columns={cols_upper['MES']: 'MES'}, inplace=True)

        if 'HOSPITAL' not in df.columns or 'AÑO' not in df.columns or 'MES' not in df.columns:
            logger.error("Columnas faltantes")
            return {"error": "El archivo debe contener las columnas HOSPITAL, AÑO y MES"}
        
        df['HOSPITAL'] = df['HOSPITAL'].astype(str).str.strip()
        CURRENT_DF = df
        
        hospitals = sorted([h for h in df['HOSPITAL'].unique() if str(h).lower() != 'nan'])
        years = sorted([int(y) for y in df['AÑO'].unique() if pd.notnull(y)])
        
        exclude = ['HOSPITAL', 'AÑO', 'MES', 'COD_HOSPITAL', 'ID', 'COD_HOSP']
        indicators = []
        for col in df.columns:
            if col.upper() not in [e.upper() for e in exclude]:
                try:
                    numeric_col = pd.to_numeric(df[col], errors='coerce')
                    if numeric_col.notnull().any():
                        indicators.append(col)
                except:
                    continue
        
        logger.info(f"Carga exitosa. Hospitales: {len(hospitals)}")
        return {
            "hospitals": hospitals,
            "years": years,
            "available_indicators": indicators
        }
    except Exception as e:
        logger.error(f"Error procesando Excel: {str(e)}")
        return {"error": str(e)}

@app.get("/api/analysis")
async def get_analysis(
    hospital: str, 
    year: int, 
    indicators: List[str] = Query(...),
    month: Optional[int] = None
):
    global CURRENT_DF
    if CURRENT_DF is None:
        return {"error": "No hay datos cargados"}
    
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
        for col in indicators:
            if col in filtered.columns:
                filtered[col] = pd.to_numeric(filtered[col], errors='coerce').fillna(0)
        
        for m in sorted(filtered['MES'].unique()):
            m_df = filtered[filtered['MES'] == m]
            m_stats = {}
            m_peaks = {}
            for col in indicators:
                total = m_df[col].sum()
                if abs(total - round(total)) < 0.0001: total = int(round(total))
                else: total = round(float(total), 2)
                m_stats[col] = total
                
                peak = m_df[col].max()
                if abs(peak - round(peak)) < 0.0001: peak = int(round(peak))
                else: peak = round(float(peak), 2)
                m_peaks[col] = peak
                
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
        logger.error(f"Error en análisis: {str(e)}")
        return {"error": str(e)}

# Montar archivos estáticos para la web
# Esto DEBE ir al final para no interferir con las rutas de la API
app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
