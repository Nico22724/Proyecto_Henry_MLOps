# Proyecto_Henry_MLOps
## La Importancia de la Documentación en Proyectos de Ciencia de Datos:

La documentación desempeña un papel crucial en los proyectos de Ciencia de Datos por varias razones fundamentales:

Facilita el Entendimiento: Una documentación clara y detallada permite a colaboradores y futuros mantenedores comprender rápidamente el propósito, la estructura y el funcionamiento del proyecto.

Promueve la Reutilización y Mantenimiento: Ayuda a reutilizar código, datos y metodologías, optimizando el desarrollo futuro y asegurando la consistencia en diferentes fases del proyecto.

Fomenta la Transparencia y Reproducibilidad: Facilita la validación de resultados y la replicación de análisis, asegurando que los hallazgos sean comprensibles y verificables.

Incentiva la Colaboración y Contribución: Un README bien estructurado facilita que nuevos colaboradores se integren al proyecto, entendiendo rápidamente cómo pueden contribuir y mejorar el trabajo existente.

Para documentar de manera efectiva, organiza el README en secciones como Introducción, Instalación, Uso, Metodología, Resultados y Contribución. Utiliza un lenguaje claro y proporciona instrucciones detalladas para maximizar la utilidad y accesibilidad de la documentación.

## Descripción
Este proyecto tiene como objetivo disponibilizar los datos sobre peliculas pasando por un proceso de ETL en el cual extraemos la informacion relevante para la solucion al problema la transformamos para su correcta manipulacion y la cargamos para comenzar a trabajar extrayendo informacion valiosa mediante un API que tiene como objetivo dar a conocer informacion valiosa que ayude a la toma de decisiones estratégicas y un ML que ayude a recomendar otro tipo de peliculas basado en la primera pelicula vista por el usuario.

## Tabla de contenido 
1. [Introducción](#introducción)
2. [Instalación y Requisitos](#instalación-y-requisitos)
3. [Metodología](#metodología)
4. [Datos y Fuentes](#datos-y-fuentes)
5. [Estructura del Proyecto](#estructura-del-proyecto)
6. [Contribución y Colaboración](#contribución-y-colaboración)
7. [Licencia](#licencia)

## Instalacion y Requisitos 
**Requisitos**
- Python 3.7 o superior
- fastapi
- uvicorn
- scikit-learn

**Pasos de instlación**
1. Clonar el repositorio (https://github.com/Nico22724/Proyecto_Henry_MLOps.git)
2. Crear un entorno virtual: `python -m venv venv`
3. Activar el entorno virtual:
   - Windows: `venv\Scripts\activate`
   - macOS/Linux: `source venv/bin/activate`
4. Instalar las dependencias: `pip install -r requirements.txt`
   
## Metodología
Se utilizaron diferentes tecnicas de Ingeneria de Datos como el ETL para disponibilizar los datos para posteriormente explorar y revisar que no contenga valos nulos o incorrectos que puedan afectar a nuestro modelo de ML o consultas de la APi realizando un manejo adecuado de las herramientas para poder llevar a cabo el proyecto

## Datos y Fuentes
Los datos utilizados en este proyecto provienen del dataset propuestos.

## Estructura del Proyecto
- `data/`: Contiene los archivos de datos utilizados en el proyecto.
- `notebooks/`: Incluye el notebook con el ETL.
- `src/`: Código fuente del proyecto.
- `README.md`: Archivo de documentación del proyecto.

## Contribución y Colaboración
Los contribuidores son bienvenidos a reportar problemas, enviar solicitudes de funciones o enviar pull requests en el repositorio de GitHub.

## Autores:
Este proyecto fue realizado por: Nicolas Hoyos .
