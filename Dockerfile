FROM python:3.9-slim-buster

WORKDIR /usr/local/app

# Create a non-root user to run the application                                                                                                                     │
RUN addgroup --system app && adduser --system --group app 

COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy source code
COPY --chown=app:app src/ .

USER app
# Run the application
CMD ["python", "main.py"]