# Usa una imagen base de Python
FROM python:3.9-slim

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Copia el archivo de Python en el contenedor
COPY ProyectoEsios.py /app

# Instala las bibliotecas necesarias
RUN pip install requests pandas Jinja2 


# Ejecuta el archivo Python y mantiene el contenedor activo
CMD ["sh", "-c", "python ProyectoEsios.py && tail -f /dev/null"]