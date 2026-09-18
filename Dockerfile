FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY *.py ./
COPY tests/ ./tests/
COPY README.md BONUS.md ./

# Only the Python standard library is required.
RUN python -m unittest discover -s tests -t . -v
USER 10001:10001

ENTRYPOINT ["python", "app.py"]
