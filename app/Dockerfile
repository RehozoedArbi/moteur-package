FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Utilisateur sans privilèges : l'uid doit rester 10001 (le chart impose runAsUser: 10001)
RUN groupadd --gid 10001 moteur && \
    useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin moteur

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY moteur.py diagnostic.py ./

USER 10001:10001
CMD ["python", "moteur.py"]
